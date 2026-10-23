/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.aggregations;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.sort.BucketedSort;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.sort.SortValue;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * How a sorted {@code top_hits} under a high cardinality {@code terms} collects the top documents of its buckets, with the two
 * strategies side by side on the same data: one Lucene {@code TopFieldCollector} per bucket and, for every segment, a leaf
 * collector per bucket, which is what {@code top_hits} did for every sort, or one {@code BucketedSort} for all the buckets, which
 * is what it does for a single numeric sort. Each run collects every document into a fresh set of collectors, as a request
 * does, and then reads the top documents of every bucket, as building the aggregation does.
 * <p>
 * It is the collection alone: no {@code terms}, no fetch phase, no search context. The documents of a bucket are spread over the
 * whole index, the way the documents of a term are, and the index has several segments so that the leaf collectors are opened
 * more than once. Run with {@code -prof gc} to see how much each strategy allocates, which is the memory it needs. The time and the
 * allocation are per run, which collects {@code buckets * DOCS_PER_BUCKET} documents.
 * </p>
 * <pre>
 * ../gradlew run --args "TopHitsCollectionBenchmark -prof gc"
 * </pre>
 * The {@code BucketedSort} side repeats what {@code BucketedTopHits} does in the aggregations package, which is not visible from
 * here: the doc id stored next to each entry, and the tie-break on it that makes the lower doc id win.
 */
@Fork(1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Thread)
public class TopHitsCollectionBenchmark {
    private static final String FIELD = "number";
    private static final int DOCS_PER_BUCKET = 20;
    private static final int DOCS_PER_SEGMENT = 50_000;
    private static final String LUCENE = "lucene";
    private static final String BUCKETED = "bucketed";

    private static final Scorable NO_SCORE = new Scorable() {
        @Override
        public float score() {
            return 0f;
        }
    };

    @Param({ "1000", "10000" })
    public int buckets;

    /**
     * The {@code size} of the {@code top_hits}, which is the number of documents kept for every bucket. A {@code BucketedSort}
     * reserves {@code size} slots for every bucket it sees, so what it needs grows with it, which the Lucene collectors do not do
     * as much. 100 is the default maximum of {@code index.max_inner_result_window}.
     */
    @Param({ "1", "10", "100" })
    public int size;

    @Param({ LUCENE, BUCKETED })
    public String strategy;

    private Directory directory;
    private DirectoryReader reader;
    private int[] bucketOfDoc;
    private BigArrays bigArrays;

    @Setup
    public void setup() throws IOException {
        if (false == "true".equals(System.getProperty("skipSelfTest"))) {
            selfTest();
        }
        bigArrays = new BigArrays(new PageCacheRecycler(Settings.EMPTY), null, "REQUEST");
        directory = new ByteBuffersDirectory();
        bucketOfDoc = new int[buckets * DOCS_PER_BUCKET];
        reader = index(directory, bucketOfDoc, buckets, 10_000);
    }

    @TearDown
    public void tearDown() throws IOException {
        reader.close();
        directory.close();
    }

    @Benchmark
    public void collect(Blackhole bh) throws IOException {
        try (Strategy collection = strategy(strategy, bigArrays, buckets, size, bucketOfDoc)) {
            for (LeafReaderContext leaf : reader.leaves()) {
                collection.collect(leaf);
            }
            collection.consumeTopDocs(bh);
        }
    }

    /**
     * Writes {@code buckets * DOCS_PER_BUCKET} documents, spread over several segments that are never merged so that doc ids
     * follow the order the documents were added, with a numeric value drawn from {@code distinctValues} values, so that a small
     * range makes many ties. Document {@code i} belongs to bucket {@code i % buckets}, so every bucket has documents in every
     * segment.
     */
    private static DirectoryReader index(Directory directory, int[] bucketOfDoc, int buckets, int distinctValues) throws IOException {
        Random random = new Random(42);
        IndexWriterConfig config = new IndexWriterConfig().setMergePolicy(NoMergePolicy.INSTANCE).setMaxBufferedDocs(DOCS_PER_SEGMENT);
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int doc = 0; doc < bucketOfDoc.length; doc++) {
                bucketOfDoc[doc] = doc % buckets;
                Document document = new Document();
                document.add(new NumericDocValuesField(FIELD, random.nextInt(distinctValues)));
                writer.addDocument(document);
            }
        }
        return DirectoryReader.open(directory);
    }

    private static Strategy strategy(String name, BigArrays bigArrays, int buckets, int size, int[] bucketOfDoc) {
        return switch (name) {
            case LUCENE -> new LuceneStrategy(buckets, size, bucketOfDoc);
            case BUCKETED -> new BucketedStrategy(bigArrays, buckets, size, bucketOfDoc);
            default -> throw new IllegalArgumentException("unknown strategy [" + name + "]");
        };
    }

    /**
     * Both strategies must keep the same documents in the same order for every bucket, ties included: the lower doc id wins among
     * equal values.
     */
    static void selfTest() {
        try (Directory directory = new ByteBuffersDirectory()) {
            int buckets = 17;
            int[] bucketOfDoc = new int[buckets * 40];
            // Few distinct values, so that most buckets have ties at the edge of what they keep.
            try (DirectoryReader reader = index(directory, bucketOfDoc, buckets, 4)) {
                BigArrays bigArrays = new BigArrays(new PageCacheRecycler(Settings.EMPTY), null, "REQUEST");
                for (int size : new int[] { 1, 3, 10, 100 }) {
                    List<List<Integer>> lucene = topDocs(LUCENE, reader, bigArrays, buckets, size, bucketOfDoc);
                    List<List<Integer>> bucketed = topDocs(BUCKETED, reader, bigArrays, buckets, size, bucketOfDoc);
                    if (lucene.equals(bucketed) == false) {
                        throw new AssertionError("size [" + size + "] lucene " + lucene + " but bucketed " + bucketed);
                    }
                }
            }
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

    private static List<List<Integer>> topDocs(
        String name,
        DirectoryReader reader,
        BigArrays bigArrays,
        int buckets,
        int size,
        int[] bucketOfDoc
    ) throws IOException {
        try (Strategy collection = strategy(name, bigArrays, buckets, size, bucketOfDoc)) {
            for (LeafReaderContext leaf : reader.leaves()) {
                collection.collect(leaf);
            }
            List<List<Integer>> topDocs = new ArrayList<>();
            for (int bucket = 0; bucket < buckets; bucket++) {
                topDocs.add(collection.topDocs(bucket));
            }
            return topDocs;
        }
    }

    private interface Strategy extends Releasable {
        /**
         * Collect every document of a segment into the bucket it belongs to, in doc id order.
         */
        void collect(LeafReaderContext leaf) throws IOException;

        /**
         * The doc ids kept for a bucket, best first.
         */
        List<Integer> topDocs(int bucket) throws IOException;

        default void consumeTopDocs(Blackhole bh) throws IOException {
            int buckets = bucketCount();
            for (int bucket = 0; bucket < buckets; bucket++) {
                bh.consume(topDocs(bucket));
            }
        }

        int bucketCount();
    }

    /**
     * A collector for every bucket, and for every segment a leaf collector for every bucket that has a document in it.
     */
    private static final class LuceneStrategy implements Strategy {
        private final Sort sort = new Sort(new SortField(FIELD, SortField.Type.LONG));
        private final int size;
        private final int[] bucketOfDoc;
        private final TopFieldCollector[] collectors;
        private final LeafCollector[] leafCollectors;

        LuceneStrategy(int buckets, int size, int[] bucketOfDoc) {
            this.size = size;
            this.bucketOfDoc = bucketOfDoc;
            this.collectors = new TopFieldCollector[buckets];
            this.leafCollectors = new LeafCollector[buckets];
        }

        @Override
        public void collect(LeafReaderContext leaf) throws IOException {
            Arrays.fill(leafCollectors, null);
            for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                int bucket = bucketOfDoc[leaf.docBase + doc];
                if (collectors[bucket] == null) {
                    collectors[bucket] = new TopFieldCollectorManager(sort, size, null, Integer.MAX_VALUE).newCollector();
                }
                LeafCollector leafCollector = leafCollectors[bucket];
                if (leafCollector == null) {
                    leafCollector = collectors[bucket].getLeafCollector(leaf);
                    leafCollector.setScorer(NO_SCORE);
                    leafCollectors[bucket] = leafCollector;
                }
                leafCollector.collect(doc);
            }
        }

        @Override
        public List<Integer> topDocs(int bucket) {
            List<Integer> docs = new ArrayList<>();
            for (ScoreDoc scoreDoc : collectors[bucket].topDocs().scoreDocs) {
                docs.add(scoreDoc.doc);
            }
            return docs;
        }

        @Override
        public int bucketCount() {
            return collectors.length;
        }

        @Override
        public void close() {}
    }

    /**
     * One {@link BucketedSort} for every bucket, the number of documents collected for each bucket, and the doc ids, like
     * {@code BucketedTopHits}.
     */
    private static final class BucketedStrategy implements Strategy {
        private final BigArrays bigArrays;
        private final int buckets;
        private final int[] bucketOfDoc;
        private final DocIds docIds;
        private final BucketedSort sort;
        private LongArray hitCounts;

        BucketedStrategy(BigArrays bigArrays, int buckets, int size, int[] bucketOfDoc) {
            this.bigArrays = bigArrays;
            this.buckets = buckets;
            this.bucketOfDoc = bucketOfDoc;
            this.docIds = new DocIds(bigArrays);
            this.hitCounts = bigArrays.newLongArray(1);
            this.sort = new BucketedSort.ForLongs(bigArrays, SortOrder.ASC, DocValueFormat.RAW, size, docIds) {
                @Override
                public Leaf forLeaf(LeafReaderContext ctx) throws IOException {
                    NumericDocValues values = DocValues.getNumeric(ctx.reader(), FIELD);
                    return new Leaf(ctx) {
                        private long value;

                        @Override
                        protected boolean advanceExact(int doc) throws IOException {
                            if (values.advanceExact(doc)) {
                                value = values.longValue();
                                return true;
                            }
                            return false;
                        }

                        @Override
                        protected long docValue() {
                            return value;
                        }
                    };
                }
            };
        }

        @Override
        public void collect(LeafReaderContext leaf) throws IOException {
            BucketedSort.Leaf sortLeaf = sort.forLeaf(leaf);
            for (int doc = 0; doc < leaf.reader().maxDoc(); doc++) {
                int bucket = bucketOfDoc[leaf.docBase + doc];
                hitCounts = bigArrays.grow(hitCounts, bucket + 1);
                hitCounts.increment(bucket, 1);
                sortLeaf.collect(doc, bucket);
            }
        }

        @Override
        public List<Integer> topDocs(int bucket) throws IOException {
            List<Integer> docs = new ArrayList<>();
            for (Hit hit : sort.getValues(bucket, (index, value) -> new Hit(value, docIds.get(index), SortOrder.ASC))) {
                docs.add(hit.doc());
            }
            return docs;
        }

        @Override
        public int bucketCount() {
            return buckets;
        }

        @Override
        public void close() {
            Releasables.close(sort, docIds, hitCounts);
        }
    }

    /**
     * The doc id next to each entry of the {@link BucketedSort}, and the tie-break on it: the lower doc id wins among equal values.
     */
    private static final class DocIds implements BucketedSort.TieBreakingExtraData, Releasable {
        private final BigArrays bigArrays;
        private IntArray docIds;

        DocIds(BigArrays bigArrays) {
            this.bigArrays = bigArrays;
            this.docIds = bigArrays.newIntArray(1, false);
        }

        int get(long index) {
            return docIds.get(index);
        }

        @Override
        public void swap(long lhs, long rhs) {
            int tmp = docIds.get(lhs);
            docIds.set(lhs, docIds.get(rhs));
            docIds.set(rhs, tmp);
        }

        @Override
        public Loader loader(LeafReaderContext ctx) {
            int docBase = ctx.docBase;
            return (index, doc) -> {
                docIds = bigArrays.grow(docIds, index + 1);
                docIds.set(index, docBase + doc);
            };
        }

        @Override
        public boolean betterOnTie(long lhs, long rhs) {
            return docIds.get(lhs) < docIds.get(rhs);
        }

        @Override
        public void close() {
            docIds.close();
        }
    }

    /**
     * An entry of a bucket, ordered by value and then by doc id, lowest first. {@link BucketedSort#getValues} reverses the whole
     * comparison for a descending sort, so the doc id comparison is reversed in that case.
     */
    private record Hit(SortValue value, int doc, SortOrder order) implements Comparable<Hit> {
        @Override
        public int compareTo(Hit other) {
            int cmp = value.compareTo(other.value);
            if (cmp != 0) {
                return cmp;
            }
            return order == SortOrder.DESC ? Integer.compare(other.doc, doc) : Integer.compare(doc, other.doc);
        }
    }
}
