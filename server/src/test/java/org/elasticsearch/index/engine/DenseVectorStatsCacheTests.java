/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.elasticsearch.index.shard.DenseVectorStats;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.greaterThan;

public class DenseVectorStatsCacheTests extends ESTestCase {

    private static final String FIELD = "vector";

    /** A segment is inspected once, however many times its stats are asked for. */
    public void testSegmentIsInspectedOnce() throws Exception {
        final AtomicInteger prefetches = new AtomicInteger();
        try (Directory directory = new PrefetchCountingDirectory(newDirectory(), prefetches)) {
            indexVectors(directory, 32);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final LeafReader leafReader = reader.leaves().get(0).reader();
                final DenseVectorStatsCache cache = new DenseVectorStatsCache();

                final DenseVectorStats first = cache.get(leafReader, List.of(FIELD), true);
                final int afterFirst = prefetches.get();
                assertThat("opening the vector values should have prefetched", afterFirst, greaterThan(0));
                assertThat(first.getValueCount(), greaterThan(0L));

                for (int i = 0; i < 5; i++) {
                    final DenseVectorStats repeated = cache.get(leafReader, List.of(FIELD), true);
                    assertEquals(first.getValueCount(), repeated.getValueCount());
                    assertEquals(first.offHeapStats(), repeated.offHeapStats());
                }
                assertEquals("repeated stats calls must not touch the directory again", afterFirst, prefetches.get());
                assertEquals(1, cache.cachedSegments());
            }
        }
    }

    /** A refresh reuses unchanged segment cores, so their entries survive and only the new segment is inspected. */
    public void testEntriesSurviveRefresh() throws Exception {
        final AtomicInteger prefetches = new AtomicInteger();
        try (Directory directory = new PrefetchCountingDirectory(newDirectory(), prefetches)) {
            try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE))) {
                addVectorDocs(writer, 16);
                writer.commit();

                final DenseVectorStatsCache cache = new DenseVectorStatsCache();
                DirectoryReader reader = DirectoryReader.open(directory);
                try {
                    final long countBefore = statsFor(cache, reader);
                    assertEquals(1, cache.cachedSegments());
                    final IndexReader.CacheKey originalSegment = reader.leaves().get(0).reader().getCoreCacheHelper().getKey();

                    addVectorDocs(writer, 16);
                    writer.commit();

                    final DirectoryReader refreshed = DirectoryReader.openIfChanged(reader);
                    assertNotNull("expected the commit to be visible", refreshed);
                    reader.close();
                    reader = refreshed;
                    assertEquals(2, reader.leaves().size());

                    // the segment that was already inspected is still cached, so asking for it again is free
                    final LeafReader carriedOver = leafWithCoreKey(reader, originalSegment);
                    final int beforeCarriedOver = prefetches.get();
                    cache.get(carriedOver, List.of(FIELD), true);
                    assertEquals("the surviving segment must not be inspected again", beforeCarriedOver, prefetches.get());

                    // the new segment is inspected, and contributes to the count
                    final long countAfter = statsFor(cache, reader);
                    assertThat(countAfter, greaterThan(countBefore));
                    assertThat(prefetches.get(), greaterThan(beforeCarriedOver));
                    assertEquals(2, cache.cachedSegments());
                } finally {
                    reader.close();
                }
            }
        }
    }

    /** A segment closing drops its entry, so the cache does not grow as segments are merged away. */
    public void testEntryDroppedWhenSegmentCloses() throws Exception {
        final AtomicInteger prefetches = new AtomicInteger();
        try (Directory directory = new PrefetchCountingDirectory(newDirectory(), prefetches)) {
            indexVectors(directory, 16);
            final DenseVectorStatsCache cache = new DenseVectorStatsCache();
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                cache.get(reader.leaves().get(0).reader(), List.of(FIELD), true);
                assertEquals(1, cache.cachedSegments());
            }
            assertEquals("closing the reader must drop the entry", 0, cache.cachedSegments());
        }
    }

    /** A field the segment does not have contributes nothing, and costs nothing. */
    public void testUnknownFieldIsIgnored() throws Exception {
        final AtomicInteger prefetches = new AtomicInteger();
        try (Directory directory = new PrefetchCountingDirectory(newDirectory(), prefetches)) {
            indexVectors(directory, 16);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final DenseVectorStatsCache cache = new DenseVectorStatsCache();
                final int before = prefetches.get();
                final DenseVectorStats stats = cache.get(reader.leaves().get(0).reader(), List.of("no_such_field"), true);
                assertEquals(0L, stats.getValueCount());
                assertEquals(before, prefetches.get());
            }
        }
    }

    /** With counts excluded the values are never opened, but the off-heap sizes are still reported. */
    public void testOffHeapReportedWithoutCounts() throws Exception {
        final AtomicInteger prefetches = new AtomicInteger();
        try (Directory directory = new PrefetchCountingDirectory(newDirectory(), prefetches)) {
            indexVectors(directory, 32);
            try (DirectoryReader reader = DirectoryReader.open(directory)) {
                final LeafReader leafReader = reader.leaves().get(0).reader();
                final DenseVectorStatsCache cache = new DenseVectorStatsCache();

                final int before = prefetches.get();
                final DenseVectorStats stats = cache.get(leafReader, List.of(FIELD), false);
                assertEquals("the vector values must not be opened", before, prefetches.get());
                assertEquals(0L, stats.getValueCount());
                assertEquals(Set.of(FIELD), stats.offHeapStats().keySet());
                assertThat(stats.offHeapStats().get(FIELD).values().stream().mapToLong(Long::longValue).sum(), greaterThan(0L));
                assertEquals("nothing should have been cached", 0, cache.cachedSegments());
            }
        }
    }

    private static LeafReader leafWithCoreKey(DirectoryReader reader, IndexReader.CacheKey key) {
        for (LeafReaderContext context : reader.leaves()) {
            if (context.reader().getCoreCacheHelper().getKey().equals(key)) {
                return context.reader();
            }
        }
        throw new AssertionError("the segment did not survive the refresh");
    }

    private static long statsFor(DenseVectorStatsCache cache, DirectoryReader reader) throws IOException {
        long count = 0;
        for (LeafReaderContext context : reader.leaves()) {
            count += cache.get(context.reader(), List.of(FIELD), true).getValueCount();
        }
        return count;
    }

    private static void indexVectors(Directory directory, int docs) throws IOException {
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig())) {
            addVectorDocs(writer, docs);
            writer.commit();
        }
    }

    /** Only every other document gets a vector, so the field is sparse and read through an {@code IndexedDISI}. */
    private static void addVectorDocs(IndexWriter writer, int docs) throws IOException {
        for (int i = 0; i < docs; i++) {
            final Document document = new Document();
            if (i % 2 == 0) {
                document.add(new KnnFloatVectorField(FIELD, new float[] { i, i + 1, i + 2, i + 3 }, VectorSimilarityFunction.COSINE));
            }
            writer.addDocument(document);
        }
    }

    /** Counts {@link IndexInput#prefetch} calls, including on slices and clones. */
    private static class PrefetchCountingDirectory extends FilterDirectory {

        private final AtomicInteger prefetches;

        PrefetchCountingDirectory(Directory in, AtomicInteger prefetches) {
            super(in);
            this.prefetches = prefetches;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            return new CountingIndexInput(super.openInput(name, context), prefetches);
        }
    }

    private static class CountingIndexInput extends FilterIndexInput {

        private final AtomicInteger prefetches;

        CountingIndexInput(IndexInput in, AtomicInteger prefetches) {
            super("CountingIndexInput(" + in + ")", in);
            this.prefetches = prefetches;
        }

        @Override
        public void prefetch(long offset, long length) throws IOException {
            prefetches.incrementAndGet();
            in.prefetch(offset, length);
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new CountingIndexInput(in.slice(sliceDescription, offset, length), prefetches);
        }

        @Override
        public IndexInput clone() {
            return new CountingIndexInput(in.clone(), prefetches);
        }
    }
}
