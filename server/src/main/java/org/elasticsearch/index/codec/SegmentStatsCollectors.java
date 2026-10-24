/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentInfo;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.core.Strings.format;

/**
 * An immutable set of {@link SegmentStatsCollector}s that fans out the flush and merge callbacks to its members. The
 * collectors handed to a {@link CodecService} are narrowed to those that {@link SegmentStatsCollector#appliesTo apply} to the
 * index's current mapping with {@link #applicableTo(MapperService)}.
 * Fanning out from here keeps the hot paths free of iterator allocations: with no collectors a callback is a single length
 * check, with one collector a single call.
 *
 * <p>Collecting segment stats is a side activity that must never fail the flush or merge it observes: an exception from a
 * collector would otherwise abort the flush as a tragic {@link org.apache.lucene.index.IndexWriter} event, or fail the merge
 * and with it the engine. Every collector is therefore wrapped in a {@link SafeCollector} shield that logs a failure and skips the
 * failing call; collection continues with the next field or source and attributes already written remain.
 */
public final class SegmentStatsCollectors {

    private static final Logger logger = LogManager.getLogger(SegmentStatsCollectors.class);

    /** No collectors. */
    public static final SegmentStatsCollectors NONE = new SegmentStatsCollectors(new SafeCollector[0]);

    private static final SegmentStatsCollector.MergeCollector NO_MERGE_COLLECTOR = source -> {};

    private final SafeCollector[] collectors;

    private SegmentStatsCollectors(SafeCollector[] collectors) {
        this.collectors = collectors;
    }

    /**
     * Returns the given collectors as a {@link SegmentStatsCollectors}. The list is copied, later modifications to it have no
     * effect.
     */
    public static SegmentStatsCollectors of(List<SegmentStatsCollector> collectors) {
        if (collectors.isEmpty()) {
            return NONE;
        }
        SafeCollector[] safe = new SafeCollector[collectors.size()];
        for (int i = 0; i < safe.length; i++) {
            safe[i] = new SafeCollector(collectors.get(i));
        }
        return new SegmentStatsCollectors(safe);
    }

    /**
     * Returns the collectors that {@link SegmentStatsCollector#appliesTo apply} to the index with the given mapper service.
     * Returns {@code this} without allocating if all of them do, which is the common case, and {@link #NONE} if none does.
     */
    public SegmentStatsCollectors applicableTo(@Nullable MapperService mapperService) {
        final SafeCollector[] collectors = this.collectors;
        if (collectors.length == 0) {
            return this;
        }
        if (collectors.length == 1) {
            return collectors[0].appliesTo(mapperService) ? this : NONE;
        }

        SafeCollector[] applicable = null; // only allocated once a collector does not apply; null while all of them do
        int count = 0;
        for (int i = 0; i < collectors.length; i++) {
            if (collectors[i].appliesTo(mapperService)) {
                if (applicable != null) {
                    applicable[count++] = collectors[i];
                }
            } else if (applicable == null) {
                // keep the collectors before this one, which all applied; at most one less than all of them fits
                applicable = Arrays.copyOf(collectors, collectors.length - 1);
                count = i;
            }
        }
        if (applicable == null) {
            return this;
        }
        if (count == 0) {
            return NONE;
        }
        return new SegmentStatsCollectors(count == applicable.length ? applicable : Arrays.copyOf(applicable, count));
    }

    public int size() {
        return collectors.length;
    }

    /**
     * Fans out {@link SegmentStatsCollector#onFlush} to all collectors.
     */
    public void onFlush(SegmentInfo segment, FieldInfo field, DocValuesProducer values) {
        for (SafeCollector collector : collectors) {
            collector.onFlush(segment, field, values);
        }
    }

    /**
     * Starts a merge on all collectors and returns a single {@link SegmentStatsCollector.MergeCollector} fanning out
     * {@link SegmentStatsCollector.MergeCollector#onSource} to them. Returns a no-op for no collectors and the collector's own
     * (shielded) merge collector for a single one.
     */
    public SegmentStatsCollector.MergeCollector onMergeStart(SegmentInfo mergedSegment) {
        if (collectors.length == 0) {
            return NO_MERGE_COLLECTOR;
        }
        if (collectors.length == 1) {
            return collectors[0].onMergeStart(mergedSegment);
        }
        final SegmentStatsCollector.MergeCollector[] mergeCollectors = new SegmentStatsCollector.MergeCollector[collectors.length];
        for (int i = 0; i < collectors.length; i++) {
            mergeCollectors[i] = collectors[i].onMergeStart(mergedSegment);
        }
        return source -> {
            for (SegmentStatsCollector.MergeCollector mergeCollector : mergeCollectors) {
                mergeCollector.onSource(source);
            }
        };
    }

    private static final class SafeCollector implements SegmentStatsCollector {
        private final SegmentStatsCollector delegate;

        SafeCollector(SegmentStatsCollector delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean appliesTo(@Nullable MapperService mapperService) {
            try {
                return delegate.appliesTo(mapperService);
            } catch (Exception e) {
                onFailure("applicability check", e);
                return false;
            }
        }

        @Override
        public void onFlush(SegmentInfo segment, FieldInfo field, DocValuesProducer values) {
            try {
                delegate.onFlush(segment, field, values);
            } catch (Exception e) {
                onFailure("flush", e);
            }
        }

        @Override
        public MergeCollector onMergeStart(SegmentInfo mergedSegment) {
            final MergeCollector mergeCollector;
            try {
                mergeCollector = delegate.onMergeStart(mergedSegment);
            } catch (Exception e) {
                onFailure("merge start", e);
                return NO_MERGE_COLLECTOR;
            }
            return source -> {
                try {
                    mergeCollector.onSource(source);
                } catch (Exception e) {
                    onFailure("merge", e);
                }
            };
        }

        private static void onFailure(String phase, Exception e) {
            assert false : e;
            logger.warn(() -> format("segment stats collector failed on %s, skipping", phase), e);
        }

        @Override
        public String toString() {
            return delegate.toString();
        }
    }
}
