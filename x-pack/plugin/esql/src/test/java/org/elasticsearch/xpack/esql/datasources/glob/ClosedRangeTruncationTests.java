/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.glob;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasources.PartitionConfig;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.Operator;
import org.elasticsearch.xpack.esql.datasources.PartitionFilterHintExtractor.PartitionFilterHint;
import org.elasticsearch.xpack.esql.datasources.StorageEntry;
import org.elasticsearch.xpack.esql.datasources.StorageIterator;
import org.elasticsearch.xpack.esql.datasources.spi.FileList;
import org.elasticsearch.xpack.esql.datasources.spi.StorageChildren;
import org.elasticsearch.xpack.esql.datasources.spi.StorageObject;
import org.elasticsearch.xpack.esql.datasources.spi.StoragePath;
import org.elasticsearch.xpack.esql.datasources.spi.StorageProvider;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.elasticsearch.xpack.esql.datasources.glob.GlobExpander.doExpandGlob;

/**
 * {@code expand} accepts a bounded glob with filter hints. These tests call
 * {@link GlobExpander#doExpandGlob} to inspect the one pass a bounded listing gets.
 */
public class ClosedRangeTruncationTests extends ESTestCase {

    /**
     * Closed-range (value-filter) rejects do not consume {@code maxFiles}. Ten out-of-range years with
     * bound 3 drain the whole listing and keep one schema anchor instead of a truncated empty set.
     */
    public void testClosedRangeOnTruncatedListingStaysTruncatedWhenEmpty() throws IOException {
        FileList result = expandClosedRange("s3://bucket/data/year=*/*.parquet");
        assertEquals(1, result.fileCount());
        assertTrue(result.isInferenceAnchor());
        assertFalse(result.isTruncated());
    }

    /**
     * A two-segment wildcard glob does not bind {@code year} in the drain; the closed-range post-filter
     * drops the out-of-range files afterwards. They do not consume {@code maxFiles} either: the drain sets
     * each full batch aside and reads on, so the miss is conclusive and one schema anchor is kept instead
     * of a truncated empty set nothing would list again.
     */
    public void testClosedRangePostFilterRejectsDoNotConsumeTheBound() throws IOException {
        FileList result = expandClosedRange("s3://bucket/data/*/*.parquet");
        assertEquals(1, result.fileCount());
        assertTrue(result.isInferenceAnchor());
        assertFalse(result.isTruncated());
    }

    private static FileList expandClosedRange(String glob) throws IOException {
        List<StorageEntry> entries = new ArrayList<>();
        for (int year = 1990; year < 2000; year++) {
            entries.add(new StorageEntry(StoragePath.of("s3://bucket/data/year=" + year + "/a.parquet"), 100, Instant.EPOCH));
        }
        List<PartitionFilterHint> hints = List.of(
            new PartitionFilterHint("year", Operator.GREATER_THAN_OR_EQUAL, List.of(2024)),
            new PartitionFilterHint("year", Operator.LESS_THAN_OR_EQUAL, List.of(2026))
        );
        return doExpandGlob(
            glob,
            new FixedListing(entries),
            hints,
            PartitionConfig.fromConfig(Map.of()),
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            ExclusionConfig.fromConfig(Map.of()).compile(),
            FileOrderConfig.DEFAULT,
            new ListingExtents(3),
            PlanningMemory.NONE
        );
    }

    private static final class FixedListing implements StorageProvider {
        private final List<StorageEntry> entries;

        private FixedListing(List<StorageEntry> entries) {
            this.entries = entries;
        }

        @Override
        public StorageObject newObject(StoragePath path) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageObject newObject(StoragePath path, long length) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageObject newObject(StoragePath path, long length, Instant lastModified) {
            throw new UnsupportedOperationException();
        }

        @Override
        public StorageIterator listObjects(StoragePath prefix, boolean recursive) {
            Iterator<StorageEntry> it = entries.iterator();
            return new StorageIterator() {
                @Override
                public boolean hasNext() {
                    return it.hasNext();
                }

                @Override
                public StorageEntry next() {
                    if (it.hasNext() == false) {
                        throw new NoSuchElementException();
                    }
                    return it.next();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public StorageChildren listChildren(StoragePath prefix, int limit) {
            return null;
        }

        @Override
        public boolean exists(StoragePath path) {
            return false;
        }

        @Override
        public List<String> supportedSchemes() {
            return List.of("s3");
        }

        @Override
        public void close() {}
    }
}
