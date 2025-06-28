// create hash function using FNV-1a algorithm
export function hash(str: string | undefined): string {
	return fnv1a(str);
}

function fnv1a(str: string | undefined): string {
	if (!str) return '00000000'; // Return a default hash for undefined or empty strings

	let hash = 2166136261;
	for (let i = 0; i < str.length; i++) {
		hash ^= str.charCodeAt(i);
		hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
	}
	// Convert to 8-digit hex string
	return (hash >>> 0).toString(16).padStart(8, '0');
}
