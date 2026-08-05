use std::num::NonZeroUsize;
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

use lru::LruCache;
use parquet::arrow::arrow_reader::ArrowReaderMetadata;

/// Access-based TTL matching Java's `FooterByteCache.EXPIRE_AFTER_ACCESS_SECONDS`.
const TTL: Duration = Duration::from_secs(30);

/// Maximum number of entries. We can't cheaply measure `ArrowReaderMetadata` byte size
/// (it's opaque Arc-wrapped data), so we bound by count rather than bytes. 256 covers
/// the typical case of a node scanning several distinct files per query cycle, with
/// headroom for partitioned datasets.
const MAX_ENTRIES: usize = 256;

struct Entry {
    meta: ArrowReaderMetadata,
    last_access: Instant,
}

/// Cache is backed by `lru::LruCache`, which maintains LRU order via an intrusive
/// doubly-linked list inside its hash map. All operations we use are O(1):
///
///   - `get_mut(k)`     — looks up and promotes to MRU
///   - `put(k, v)`      — inserts and (if at capacity) evicts the LRU entry
///   - `peek_lru()`     — peeks at the LRU end without promoting
///   - `pop_lru()`      — removes the LRU entry
///
/// TTL expiry is layered on top: each `Entry` carries `last_access`, and on every
/// `insert` we sweep expired entries from the LRU tail (cheapest end to inspect).
/// The sweep is amortized O(1) — each entry is created once and popped at most once.
struct Cache {
    entries: LruCache<String, Entry>,
}

impl Cache {
    fn new(capacity: usize) -> Self {
        Self {
            entries: LruCache::new(NonZeroUsize::new(capacity).expect("cache capacity > 0")),
        }
    }

    fn get(&mut self, path: &str) -> Option<ArrowReaderMetadata> {
        // `get_mut` promotes to MRU; we then re-validate the TTL. If expired, pop
        // and treat as a miss. Promoting before the TTL check is fine — an expired
        // entry that we're about to remove doesn't need correct LRU positioning.
        let entry = self.entries.get_mut(path)?;
        if entry.last_access.elapsed() > TTL {
            self.entries.pop(path);
            return None;
        }
        entry.last_access = Instant::now();
        Some(entry.meta.clone())
    }

    fn insert(&mut self, path: String, meta: ArrowReaderMetadata) {
        // Sweep expired entries from the LRU tail. Because we update `last_access`
        // on every `get`/`insert`, the LRU end carries the oldest timestamp(s).
        // Stop at the first non-expired entry (newer entries can't be older than it).
        // Bounded by total entries created — amortized O(1) per insert.
        while let Some((_, e)) = self.entries.peek_lru() {
            if e.last_access.elapsed() > TTL {
                self.entries.pop_lru();
            } else {
                break;
            }
        }
        // `put` evicts the LRU entry when at capacity (returns it; we drop it).
        self.entries.put(path, Entry { meta, last_access: Instant::now() });
    }
}

static CACHE: LazyLock<Mutex<Cache>> =
    LazyLock::new(|| Mutex::new(Cache::new(MAX_ENTRIES)));

/// Returns a cached `ArrowReaderMetadata` if a valid (non-expired) entry exists for `path`,
/// updating the access time. Returns `None` on miss or expiry.
///
/// Keyed by path only (no file size) so cache hits require zero network calls. The 30-second
/// TTL bounds staleness for in-place file replacements — acceptable for immutable S3 objects.
pub(crate) fn get(path: &str) -> Option<ArrowReaderMetadata> {
    CACHE.lock().unwrap_or_else(|e| e.into_inner()).get(path)
}

/// Inserts or replaces the entry for `path`, evicting expired or LRU entries as needed.
pub(crate) fn insert(path: String, meta: ArrowReaderMetadata) {
    CACHE.lock().unwrap_or_else(|e| e.into_inner()).insert(path, meta);
}
