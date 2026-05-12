/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.store;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.common.util.StringLiteralDeduplicator;

import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * A {@link FilterDirectory} that owns per-Directory caches for FieldInfo
 * deduplication. {@link org.elasticsearch.index.codec.DeduplicatingFieldInfosFormat}
 * consults the caches via {@link #unwrap(Directory)} (using {@code SegmentInfo.dir})
 * to share canonical {@link FieldInfo} instances across all segments and DocValues
 * generations of the same shard.
 *
 * <p>The FieldInfo cache uses weak references so that canonical instances become
 * eligible for GC once no live {@code SegmentReader} references them (e.g. after
 * the segment is merged away and the older readers are closed). A reference queue
 * is drained on every intern call to remove stale keys, so the map never grows
 * past the live working set.
 *
 * <p>The attribute-Map cache and string deduplicator use strong references; their
 * keyspace is bounded by the shard's mapping rather than by segment lifecycle, so
 * weak references would gain little. Everything is reclaimed naturally when the
 * Store closes and the directory chain becomes unreachable.
 */
public final class FieldInfoCachingDirectory extends FilterDirectory {

    public static final FeatureFlag FEATURE_FLAG = new FeatureFlag("field_info_caching_directory");

    private final ConcurrentHashMap<Object, FieldInfoRef> fieldInfoCache = new ConcurrentHashMap<>();
    private final ReferenceQueue<FieldInfo> deadFieldInfoRefs = new ReferenceQueue<>();
    private final ConcurrentHashMap<Map<String, String>, Map<String, String>> attributesCache = new ConcurrentHashMap<>();
    private final StringLiteralDeduplicator stringCache = new StringLiteralDeduplicator();

    public FieldInfoCachingDirectory(Directory delegate) {
        super(delegate);
    }

    /**
     * Wraps the given directory with a {@link FieldInfoCachingDirectory} if the feature flag is enabled and the directory is
     * not already wrapped. Otherwise returns the directory unchanged. Idempotent.
     */
    public static Directory wrapIfEnabled(Directory directory) {
        if (FEATURE_FLAG.isEnabled() == false) {
            return directory;
        }
        if (unwrap(directory) != null) {
            return directory;
        }
        return new FieldInfoCachingDirectory(directory);
    }

    /**
     * Walks the {@link FilterDirectory} chain and returns the first {@link FieldInfoCachingDirectory}, or {@code null} if none
     * is present.
     */
    public static FieldInfoCachingDirectory unwrap(Directory directory) {
        Directory d = directory;
        while (d != null) {
            if (d instanceof FieldInfoCachingDirectory cd) {
                return cd;
            }
            if (d instanceof FilterDirectory fd) {
                d = fd.getDelegate();
            } else {
                return null;
            }
        }
        return null;
    }

    /**
     * Returns the canonical {@link FieldInfo} for the given key, allocating one via {@code factory} on the first miss. The cache
     * uses weak references so canonical instances are reclaimed once no live {@code SegmentReader} retains them.
     */
    public FieldInfo internFieldInfo(Object key, Supplier<FieldInfo> factory) {
        drainDeadRefs();
        while (true) {
            FieldInfoRef existing = fieldInfoCache.get(key);
            FieldInfo canonical = existing == null ? null : existing.get();
            if (canonical != null) {
                return canonical;
            }
            FieldInfo created = factory.get();
            FieldInfoRef fresh = new FieldInfoRef(key, created, deadFieldInfoRefs);
            if (existing == null) {
                FieldInfoRef prev = fieldInfoCache.putIfAbsent(key, fresh);
                if (prev == null) {
                    return created;
                }
            } else {
                if (fieldInfoCache.replace(key, existing, fresh)) {
                    return created;
                }
            }
            // Lost the race: another thread either installed a canonical or replaced a stale entry; retry.
        }
    }

    /**
     * Returns a canonical immutable copy of the given attribute map, or installs it as canonical on the first occurrence.
     */
    public Map<String, String> internAttributes(Map<String, String> attributes) {
        if (attributes.isEmpty()) {
            return Map.of();
        }
        Map<String, String> existing = attributesCache.get(attributes);
        if (existing != null) {
            return existing;
        }
        // Intern the keys and values as we copy so the canonical map shares string instances across attribute maps.
        Map<String, String> deduped = new java.util.HashMap<>(attributes.size());
        for (var entry : attributes.entrySet()) {
            deduped.put(stringCache.deduplicate(entry.getKey()), stringCache.deduplicate(entry.getValue()));
        }
        Map<String, String> canonical = Map.copyOf(deduped);
        Map<String, String> prev = attributesCache.putIfAbsent(canonical, canonical);
        return prev != null ? prev : canonical;
    }

    public StringLiteralDeduplicator stringCache() {
        return stringCache;
    }

    /**
     * Exposed for testing/monitoring: number of live FieldInfo entries currently retained in the cache (including ones whose
     * weak reference may have been cleared but not yet drained).
     */
    public int fieldInfoCacheSize() {
        return fieldInfoCache.size();
    }

    private void drainDeadRefs() {
        Reference<? extends FieldInfo> r;
        while ((r = deadFieldInfoRefs.poll()) != null) {
            FieldInfoRef kr = (FieldInfoRef) r;
            fieldInfoCache.remove(kr.key, kr);
        }
    }

    private static final class FieldInfoRef extends WeakReference<FieldInfo> {
        final Object key;

        FieldInfoRef(Object key, FieldInfo value, ReferenceQueue<FieldInfo> queue) {
            super(value, queue);
            this.key = key;
        }
    }
}
