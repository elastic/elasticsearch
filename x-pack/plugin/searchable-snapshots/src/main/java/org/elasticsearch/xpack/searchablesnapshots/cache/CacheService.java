/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.searchablesnapshots.cache;

import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.xpack.searchablesnapshots.SearchableSnapshotContext;

import java.io.IOException;
import java.util.Objects;

/**
 * A simple in-memory LRU cache used to cache segments of Lucene files.
 */
public class CacheService extends AbstractLifecycleComponent {

    public static final Setting<Boolean> CACHE_ENABLED_SETTING =
        Setting.boolSetting("xpack.searchablesnapshots.cache.enabled", true, Setting.Property.NodeScope);

    public static final Setting<ByteSizeValue> CACHE_SIZE_SETTING =
        Setting.memorySizeSetting("xpack.searchablesnapshots.cache.size", "10%", Setting.Property.NodeScope);

    public static final Setting<ByteSizeValue> CACHE_SEGMENT_SIZE_SETTING =
        Setting.byteSizeSetting("xpack.searchablesnapshots.cache.segment_size", new ByteSizeValue(8, ByteSizeUnit.KB),
            new ByteSizeValue(1, ByteSizeUnit.KB), new ByteSizeValue(1, ByteSizeUnit.GB), Setting.Property.NodeScope);

    private final Cache<CacheKey, CacheValue> cache;
    private final int segmentSize;

    public CacheService(final Settings settings) {
        assert CACHE_ENABLED_SETTING.get(settings);
        this.cache = CacheBuilder.<CacheKey, CacheValue>builder()
            .setMaximumWeight(CACHE_SIZE_SETTING.get(settings).getBytes())
            .weigher((cacheKey, cacheValue) -> cacheValue.ramBytesUsed())
            .build();
        this.segmentSize = Math.toIntExact(CACHE_SEGMENT_SIZE_SETTING.get(settings).getBytes());
    }

    public int getSegmentSize() {
        return segmentSize;
    }

    @Override
    protected void doStart() {
    }

    @Override
    protected void doStop() {
    }

    @Override
    protected void doClose() throws IOException {
        cache.invalidateAll();
    }

    public CacheValue computeIfAbsent(final CacheKey cacheKey, CheckedFunction<CacheKey, CacheValue, IOException> fn) throws Exception {
        return cache.computeIfAbsent(cacheKey, fn::apply);
    }

    /**
     * A {@link CacheKey} is an object that is used as a key in cache in order to
     * refer to a specific segment of a given file from a snapshot.
     */
    static final class CacheKey {

        /** The snapshot the file is stored into **/
        private final SearchableSnapshotContext context;
        /** The name of the physical Lucene file **/
        private final String name;
        /** The segment (ie, the part) of the file the key refers to **/
        private final long segment;

        CacheKey(final SearchableSnapshotContext context, final String name, final long segment) {
            this.context = Objects.requireNonNull(context);
            this.name = Objects.requireNonNull(name);
            this.segment = segment;
        }

        public long segment() {
            return segment;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CacheKey cacheKey = (CacheKey) o;
            return segment == cacheKey.segment &&
                Objects.equals(context, cacheKey.context) &&
                Objects.equals(name, cacheKey.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(context, name, segment);
        }

        @Override
        public String toString() {
            return "CacheKey{" +
                "context=" + context +
                ", name='" + name + '\'' +
                ", segment=" + segment +
                '}';
        }
    }

    /**
     * Abstract class for all cached objects.
     */
    abstract static class CacheValue {

        abstract long ramBytesUsed();
    }
}
