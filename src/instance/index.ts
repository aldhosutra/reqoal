import stringify from 'json-stable-stringify';
import { DEFAULT_PRUNE_INTERVAL_MS, DEFAULT_TTL_MS, DEFAULT_CONCURRENCY_LIMIT } from './default';
import { hash } from './hash';

type CacheEntry<T> = {
	/** The cached result value. */
	result: T;
	/** The timestamp (ms) when the result was cached. */
	timestamp: number;
};

/**
 * ReqoalInstance - Request Coalescing and Caching Class
 *
 * Provides request coalescing and caching for async functions. Deduplicates concurrent requests for the same function and arguments, caches results for a configurable TTL, and periodically prunes expired cache entries.
 *
 * Usage:
 *
 *   import { ReqoalInstance } from 'reqoal';
 *   const coalescer = new ReqoalInstance();
 *   const result = await coalescer.fetch(myAsyncFn, arg1, arg2);
 *   const isActive = coalescer.isCoalesced(myAsyncFn, arg1, arg2);
 *
 * @class ReqoalInstance
 * @constructor
 * @param {number} [intervalMs=DEFAULT_PRUNE_INTERVAL_MS] - Interval (ms) for pruning expired cache entries.
 * @param {number} [ttlMs=DEFAULT_TTL_MS] - Time-to-live (ms) for cached results.
 * @param {Console} [consoler=console] - Optional custom console for logging.
 */
export class ReqoalInstance {
	private _pruneIntervalMs: number = DEFAULT_PRUNE_INTERVAL_MS;
	private _ttlMs: number = DEFAULT_TTL_MS;
	private _maxConcurrency: number = DEFAULT_CONCURRENCY_LIMIT;
	private _console: Console = console;
	private _interval: NodeJS.Timeout | null = null;
	private _resultCache: Map<string, CacheEntry<unknown>> = new Map();
	private _inFlightRequests: Map<string, Promise<unknown>> = new Map();
	private _currentConcurrency: number = 0;
	private _customKeyGen?: (functionName: string, ...args: unknown[]) => string;

	/**
	 * Create a new ReqoalInstance.
	 * @param intervalMs Interval (ms) for pruning expired cache entries.
	 * @param ttlMs Time-to-live (ms) for cached results.
	 * @param consoler Optional custom console for logging.
	 * @param maxConcurrency Maximum number of concurrent in-flight requests.
	 */
	constructor(
		intervalMs: number = DEFAULT_PRUNE_INTERVAL_MS,
		ttlMs: number = DEFAULT_TTL_MS,
		consoler: Console = console,
		maxConcurrency: number = DEFAULT_CONCURRENCY_LIMIT,
	) {
		this._console = consoler;
		this._pruneIntervalMs = intervalMs;
		this._ttlMs = ttlMs;
		this._maxConcurrency = maxConcurrency;
	}

	/**
	 * Coalesces concurrent or repeated calls for the same function and arguments.
	 * If an in-flight promise exists, returns it.
	 * If a cached result exists within TTL, returns it.
	 * Otherwise, calls the function, caches the result, and returns it.
	 * Supports both async and sync functions.
	 *
	 * @template T
	 * @template Args
	 * @param fn The function to call (can be sync or async).
	 * @param args Arguments to pass to the function.
	 * @returns The result of the function, possibly from cache or in-flight request.
	 */
	public async coalesce<T, Args extends unknown[]>(
		fn: (...args: Args) => T | Promise<T>,
		...args: Args
	): Promise<T> {
		const functionName = fn.name || 'anonymous';
		const key = this._createKey(functionName, ...args);

		if (!this._interval) this._interval = setInterval(this.prune.bind(this), this._pruneIntervalMs);

		const now = Date.now();

		// 1. If in-flight, return existing promise
		if (this._inFlightRequests.has(key)) {
			this._console.trace(`[requestCoalescing] Reusing in-flight request for key: ${key}`);
			return this._inFlightRequests.get(key) as Promise<T>;
		}

		// 2. If cached result within TTL, return it
		const cacheEntry = this._resultCache.get(key) as CacheEntry<T> | undefined;
		if (cacheEntry && now - cacheEntry.timestamp <= this._ttlMs) {
			this._console.trace(`[requestCoalescing] Returning cached result for key within TTL: ${key}`);
			return Promise.resolve(cacheEntry.result);
		}

		// 3. Enforce concurrency limit
		if (this._currentConcurrency >= this._maxConcurrency) {
			this._console.warn(
				`[requestCoalescing] Max concurrency (${this._maxConcurrency}) reached. Rejecting request for key: ${key}`,
			);
			return Promise.reject(new Error('Max concurrency reached'));
		}

		this._currentConcurrency++;

		const promise = (async () => {
			try {
				// Support both sync and async functions
				const result = await Promise.resolve(fn(...args));
				this._resultCache.set(key, { result, timestamp: Date.now() });
				setTimeout(() => {
					const entry = this._resultCache.get(key);
					if (entry && Date.now() - entry.timestamp >= this._ttlMs) {
						this._resultCache.delete(key);
						this._console.trace(`[requestCoalescing] Cleared cached result for key: ${key}`);
					}
				}, this._ttlMs);
				return result;
			} catch (err) {
				this._console.error(`[requestCoalescing] Error processing request for key: ${key}`, err);
				throw err;
			} finally {
				this._inFlightRequests.delete(key);
				this._currentConcurrency--;
				this._console.trace(`[requestCoalescing] Cleared in-flight request for key: ${key}`);
			}
		})();

		this._inFlightRequests.set(key, promise);
		this._console.trace(`[requestCoalescing] Created new in-flight request for key: ${key}`);
		return promise;
	}

	/**
	 * Checks if a request for the given function and arguments is either in-flight or cached (within TTL).
	 *
	 * @template Args
	 * @param fn The async function to check.
	 * @param args Arguments to check.
	 * @returns True if a request is in-flight or cached; otherwise, false.
	 */
	public isCoalesced<Args extends unknown[]>(
		fn: (...args: Args) => Promise<unknown>,
		...args: Args
	): boolean {
		const functionName = fn.name || 'anonymous';
		const key = this._createKey(functionName, ...args);
		const now = Date.now();
		if (this._inFlightRequests.has(key)) {
			return true;
		}
		const entry = this._resultCache.get(key);
		return !!entry && now - entry.timestamp <= this._ttlMs;
	}

	/**
	 * Set a custom key generator for this instance.
	 * @param keyGen A function that generates a cache key from the function name and arguments.
	 */
	public setKeyGenerator(keyGen: (functionName: string, ...args: unknown[]) => string): void {
		this._customKeyGen = keyGen;
	}

	/**
	 * Manually prune expired cache entries. Most users do not need to call this, as pruning is automatic.
	 */
	public prune(): void {
		const now = Date.now();
		// Prune result cache
		for (const [key, { timestamp }] of this._resultCache.entries()) {
			if (now - timestamp > this._ttlMs) {
				this._resultCache.delete(key);
			}
		}
		// Note: in-flight entries are cleaned up on resolution
		this._console.trace('[requestCoalescing] Pruned caches');
	}

	/**
	 * Manually invalidate the cache for a specific function and arguments.
	 * @param fn The async function whose cache should be invalidated.
	 * @param args Arguments to the function.
	 */
	public invalidate<Args extends unknown[]>(
		fn: (...args: Args) => Promise<unknown>,
		...args: Args
	): void {
		const functionName = fn.name || 'anonymous';
		const key = this._createKey(functionName, ...args);
		this._resultCache.delete(key);
		this._inFlightRequests.delete(key);
		this._console.trace(`[requestCoalescing] Invalidated cache and in-flight for key: ${key}`);
	}

	/**
	 * Clear all cached results and in-flight requests in this instance.
	 */
	public clear(): void {
		this._resultCache.clear();
		this._inFlightRequests.clear();
		this._console.trace('[requestCoalescing] Cleared all cache and in-flight requests');
	}

	/**
	 * Generates a unique cache key based on function name and arguments.
	 * If a custom key generator is set via setKeyGenerator, it will be used.
	 * Otherwise, the default is `${functionName}|${stringify(args)}`.
	 *
	 * @param functionName The name of the function.
	 * @param args Arguments to the function.
	 * @returns A string key for caching and coalescing.
	 * @private
	 */
	private _createKey(functionName: string, ...args: unknown[]): string {
		if (this._customKeyGen) {
			return this._customKeyGen(functionName, ...args);
		}
		return `${functionName}|${hash(stringify(args))}`;
	}
}
