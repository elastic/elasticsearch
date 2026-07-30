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
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.util.FeatureFlag;

import java.io.IOException;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

/**
 * A {@link FilterDirectory} that owns the per-Directory cache for whole {@link FieldInfo} instances.
 * {@link org.elasticsearch.index.codec.CachingFieldInfosFormat} consults the cache via {@link #unwrap(Directory)} (using
 * {@code SegmentInfo.dir}) to share canonical {@link FieldInfo} instances across all segments and DocValues generations of
 * the same shard.
 *
 * <p>The cache uses weak references so canonical instances become eligible for GC once no live {@code SegmentReader} retains
 * them (e.g. after the segment is merged away and the older readers are closed). A reference queue is drained on every
 * intern call to remove stale keys.
 *
 * <p>This wrapper sits inside the {@code Store}'s Directory chain, between the underlying {@code ByteSizeDirectory} and the
 * outer {@code StoreDirectory}, so that {@code store.directory()} continues to return the same {@code StoreDirectory}
 * external callers have always seen. It extends {@link ByteSizeDirectory} for that reason; its size methods delegate to the
 * wrapped directory if it is a {@link ByteSizeDirectory}, falling back to a {@link Directory} file walk otherwise.
 *
 * <p>Cross-shard attribute-map and field-name interning is intentionally <em>not</em> handled here; that lives in
 * {@code CachingFieldInfosFormat} (delegating to global utilities) so that nodes hosting many shards from the same data
 * stream still share canonical attribute maps and name strings across shards.
 */
public final class FieldInfoCachingDirectory extends ByteSizeDirectory implements Accountable {

    public static final FeatureFlag FEATURE_FLAG = new FeatureFlag("field_info_caching_directory");

    private static final long BASE_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FieldInfoCachingDirectory.class);
    private static final long PER_ENTRY_RAM_BYTES_USED = RamUsageEstimator.shallowSizeOfInstance(FieldInfo.class) + RamUsageEstimator
        .shallowSizeOfInstance(FieldInfoRef.class) + RamUsageEstimator.HASHTABLE_RAM_BYTES_PER_ENTRY;

    private final ConcurrentHashMap<Object, FieldInfoRef> fieldInfoCache = new ConcurrentHashMap<>();
    private final ReferenceQueue<FieldInfo> deadFieldInfoRefs = new ReferenceQueue<>();

    public FieldInfoCachingDirectory(Directory delegate) {
        super(delegate);
    }

    /**
     * Wraps the given directory with a {@link FieldInfoCachingDirectory} if the feature flag is enabled and the directory
     * is not already wrapped. Otherwise returns the directory unchanged. Idempotent.
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
     * Walks the {@link FilterDirectory} chain and returns the first {@link FieldInfoCachingDirectory}, or {@code null} if
     * none is present.
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
     * Returns the canonical {@link FieldInfo} for the given key, allocating one via {@code factory} on the first miss. The
     * cache uses weak references so canonical instances are reclaimed once no live {@code SegmentReader} retains them.
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
     * Exposed for testing/monitoring: number of live FieldInfo entries currently retained in the cache (including ones whose
     * weak reference may have been cleared but not yet drained).
     */
    public int fieldInfoCacheSize() {
        return fieldInfoCache.size();
    }

    /**
     * Naive RAM-usage estimate for the cache: a fixed instance overhead plus a per-entry cost covering the {@link FieldInfo}
     * shell, the {@link FieldInfoRef} weak reference, and the {@link ConcurrentHashMap} entry. The cached field name
     * {@code String} and attribute {@code Map} are interned node-wide so they are intentionally not attributed to this
     * per-Directory cache. Counts entries whose weak reference may have been cleared but not yet drained.
     */
    @Override
    public long ramBytesUsed() {
        return BASE_RAM_BYTES_USED + (long) fieldInfoCache.size() * PER_ENTRY_RAM_BYTES_USED;
    }

    @Override
    public long estimateSizeInBytes() throws IOException {
        Directory delegate = getDelegate();
        if (delegate instanceof ByteSizeDirectory bsd) {
            return bsd.estimateSizeInBytes();
        }
        return estimateSizeInBytes(delegate);
    }

    @Override
    public long estimateDataSetSizeInBytes() throws IOException {
        Directory delegate = getDelegate();
        if (delegate instanceof ByteSizeDirectory bsd) {
            return bsd.estimateDataSetSizeInBytes();
        }
        return estimateSizeInBytes(delegate);
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
