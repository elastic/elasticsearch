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
 * {@code expand} declines a listing bound whenever any hint is present, so the truncated-then-filtered empty return
 * is only reachable through {@link GlobExpander#doExpandGlob}.
 */
public class ClosedRangeTruncationTests extends ESTestCase {

    public void testClosedRangeOnTruncatedListingStaysTruncatedWhenEmpty() throws IOException {
        List<StorageEntry> entries = new ArrayList<>();
        for (int year = 1990; year < 2000; year++) {
            entries.add(new StorageEntry(StoragePath.of("s3://bucket/data/year=" + year + "/a.parquet"), 100, Instant.EPOCH));
        }
        List<PartitionFilterHint> hints = List.of(
            new PartitionFilterHint("year", Operator.GREATER_THAN_OR_EQUAL, List.of(2024)),
            new PartitionFilterHint("year", Operator.LESS_THAN_OR_EQUAL, List.of(2026))
        );

        FileList result = doExpandGlob(
            "s3://bucket/data/year=*/*.parquet",
            new FixedListing(entries),
            hints,
            PartitionConfig.fromConfig(Map.of()),
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            ExclusionConfig.fromConfig(Map.of()).compile(),
            FileOrderConfig.DEFAULT,
            new ListingExtents(3, 3)
        );

        assertEquals(0, result.fileCount());
        assertTrue(result.isTruncated());
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
