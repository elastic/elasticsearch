/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFSegmentScan;
import org.elasticsearch.index.codec.vectors.diskbbq.es95.ES950DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;

/** Threads ("parts") sharing an {@link IVFSegmentScan} must visit the posting lists, and return the hits, of a single search. */
public class IVFSegmentScanConcurrencyTests extends ESTestCase {

    private static final String FIELD = "v";
    private static final int K = 20;

    private static ExecutorService threads;

    @BeforeClass
    public static void startThreads() {
        threads = Executors.newFixedThreadPool(8);
    }

    @AfterClass
    public static void stopThreads() {
        terminate(threads);
        threads = null;
    }

    public void testPartsMatchASinglePart() throws Exception {
        int dims = randomIntBetween(8, 64);
        float[] repeated = randomVector(dims);
        KnnVectorsFormat format = randomBoolean()
            ? new ES950DiskBBQVectorsFormat(64, randomIntBetween(2, 4))
            : new ESNextDiskBBQVectorsFormat(64, randomIntBetween(2, 4), null);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, config(format))) {
            int numDocs = randomIntBetween(3_000, 8_000);
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                // copies tie, and ties must break the same way however the work is split
                doc.add(new KnnFloatVectorField(FIELD, randomInt(4) == 0 ? repeated : randomVector(dims), DOT_PRODUCT));
                doc.add(new StringField("id", Integer.toString(i), Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            if (randomBoolean()) { // after the merge, so the segment keeps them as deletes
                for (int i = 0; i < numDocs / 20; i++) {
                    writer.deleteDocuments(new Term("id", Integer.toString(randomInt(numDocs - 1))));
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                warmUp(leaf, dims);
                for (int q = 0; q < 20; q++) {
                    float[] query = randomBoolean() ? repeated : randomVector(dims);
                    float visitRatio = randomBoolean() ? 0f : randomFloatBetween(0.01f, 0.3f, true);
                    Search search = new Search(leaf, query, randomAcceptBits(leaf), visitRatio, null);
                    Result alone = search.alone();
                    assertEquals(alone, search.inParts(randomIntBetween(1, 8)));
                    IVFSegmentScan prepared = search.preparedThenInParts(randomIntBetween(1, 8));
                    assertEquals(alone, result(prepared));
                    assertFalse("the parts share the query quantized per centroid", prepared.quantizedQueries().isEmpty());
                }
            }
        }
    }

    public void testNestedPartsMatchASinglePart() throws Exception {
        int dims = randomIntBetween(8, 64);
        float[] repeated = randomVector(dims);
        KnnVectorsFormat format = new ES950DiskBBQVectorsFormat(64, randomIntBetween(2, 4));
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, config(format))) {
            int numParents = randomIntBetween(800, 2_000);
            for (int p = 0; p < numParents; p++) {
                List<Document> block = new ArrayList<>();
                // alike children tie; copies of one vector make parents tie too
                float[] alike = randomInt(3) == 0 ? null : randomInt(4) == 0 ? repeated : randomVector(dims);
                for (int c = randomIntBetween(1, 8); c > 0; c--) {
                    Document child = new Document();
                    child.add(new KnnFloatVectorField(FIELD, alike != null ? alike : randomVector(dims), DOT_PRODUCT));
                    block.add(child);
                }
                Document parent = new Document();
                parent.add(new StringField("docType", "_parent", Field.Store.NO));
                block.add(parent);
                writer.addDocuments(block);
            }
            writer.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                BitSet parents = new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent"))).getBitSet(leaf.getContext());
                warmUp(leaf, dims);
                for (int q = 0; q < 20; q++) {
                    float[] query = randomBoolean() ? repeated : randomVector(dims);
                    // a small budget makes a search go on past it
                    float visitRatio = randomFrom(0f, 0.01f, 0.05f, 0.3f);
                    Search search = new Search(leaf, query, randomAcceptBits(leaf), visitRatio, parents);
                    Result alone = search.alone();
                    assertEquals(alone, search.inParts(randomIntBetween(2, 8)));
                    IVFSegmentScan prepared = search.preparedThenInParts(randomIntBetween(1, 8));
                    assertEquals(alone, result(prepared));
                    assertFalse("the parts share the query quantized per centroid", prepared.quantizedQueries().isEmpty());
                }
            }
        }
    }

    public void testAFailingPartFailsTheSearch() throws Exception {
        int dims = randomIntBetween(8, 32);
        try (Directory dir = newDirectory()) {
            indexSegment(dir, dims, 4_000);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                Search search = new Search(getOnlyLeafReader(reader), randomVector(dims), null, 1f, null);
                List<AbstractMaxScoreKnnCollector> collectors = search.collectors(new IVFSegmentScan(), 4);
                // submitted first, with dozens of posting lists to take: it does collect
                ((DelayingCollector) collectors.get(0)).failsOnFirstCollect = true;
                Exception e = expectThrows(ExecutionException.class, () -> search.run(collectors));
                assertTrue(e.getMessage(), e.getMessage().contains("injected"));
            }
        }
    }

    /** A part must be done with its collector before it records a posting list: once all are, another part may take its hits. */
    public void testHitsAreGatheredOnlyFromFinishedParts() throws Exception {
        int dims = randomIntBetween(8, 32);
        try (Directory dir = newDirectory()) {
            indexSegment(dir, dims, 4_000);
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                for (int q = 0; q < 10; q++) {
                    IVFSegmentScan scan = new IVFSegmentScan();
                    SharedMinCompetitiveScore cutoff = new SharedMinCompetitiveScore();
                    AtomicBoolean usedAfterItsHitsWereTaken = new AtomicBoolean();
                    List<StallingCollector> collectors = new ArrayList<>();
                    for (int p = 0; p < 4; p++) {
                        IVFKnnSearchStrategy strategy = new IVFKnnSearchStrategy(0.1f, K, K, cutoff, QueryTransformCache.NONE, scan);
                        collectors.add(new StallingCollector(K, strategy, usedAfterItsHitsWereTaken));
                        strategy.setCollector(collectors.get(p));
                    }
                    new Search(leaf, randomVector(dims), null, 0.1f, null).run(collectors);
                    assertNotNull("the parts searched the segment", scan.finalCollector());
                    assertFalse("a part used its collector after another part took its hits", usedAfterItsHitsWereTaken.get());
                }
            }
        }
    }

    /** Reading further ahead per part would reach past the budget (on serverless, each posting list a possible download). */
    public void testPartsPrefetchWhatASearchOnItsOwnPrefetches() throws Exception {
        int dims = randomIntBetween(8, 64);
        Set<String> prefetched = ConcurrentHashMap.newKeySet();
        try (Directory dir = newDirectory()) {
            indexSegment(dir, dims, randomIntBetween(4_000, 8_000));
            try (DirectoryReader reader = DirectoryReader.open(new PrefetchRecordingDirectory(dir, prefetched))) {
                LeafReader leaf = getOnlyLeafReader(reader);
                warmUp(leaf, dims); // near-equal centroids are then ordered the same in every search
                for (int q = 0; q < 10; q++) {
                    // a small budget, so the segment has posting lists past it
                    float visitRatio = randomFloatBetween(0.01f, 0.1f, true);
                    Search search = new Search(leaf, randomVector(dims), randomAcceptBits(leaf), visitRatio, null);
                    int parts = randomIntBetween(1, 8);
                    Set<String> alone = prefetches(prefetched, search::alone);
                    assertFalse("the search prefetches posting lists", alone.isEmpty());
                    assertEquals(parts + " parts", alone, prefetches(prefetched, () -> search.inParts(parts)));
                }
            }
        }
    }

    private record Result(long visited, List<Integer> docs, List<Float> scores) {}

    /** One query of one segment, and the ways to run it. With {@code parents}, the segment holds nested docs. */
    private record Search(LeafReader leaf, float[] query, Bits accept, float visitRatio, BitSet parents) {

        Result alone() throws Exception {
            AbstractMaxScoreKnnCollector collector = collectors(null, 1).get(0);
            run(List.of(collector));
            return result(collector, collector.visitedCount());
        }

        Result inParts(int parts) throws Exception {
            IVFSegmentScan scan = new IVFSegmentScan();
            run(collectors(scan, parts));
            return result(scan);
        }

        /** As IVFParallelScan does: a first search prepares the scan, visiting nothing, then the parts run. */
        IVFSegmentScan preparedThenInParts(int parts) throws Exception {
            IVFSegmentScan scan = new IVFSegmentScan();
            scan.prepare(() -> run(collectors(scan, 1)));
            run(collectors(scan, parts));
            return scan;
        }

        /** One collector per part; without a scan, of a single search. */
        List<AbstractMaxScoreKnnCollector> collectors(IVFSegmentScan scan, int parts) {
            List<AbstractMaxScoreKnnCollector> collectors = new ArrayList<>();
            for (int p = 0; p < parts; p++) {
                IVFKnnSearchStrategy strategy = new IVFKnnSearchStrategy(visitRatio, K, K, null, QueryTransformCache.NONE, scan);
                Random random = new Random(random().nextLong());
                collectors.add(
                    parents == null ? new DelayingCollector(K, strategy, random) : new DelayingNestedCollector(K, strategy, parents, random)
                );
                strategy.setCollector(collectors.get(p));
            }
            return collectors;
        }

        void run(List<? extends KnnCollector> collectors) throws Exception {
            List<Future<?>> searches = new ArrayList<>();
            for (KnnCollector collector : collectors) {
                searches.add(threads.submit(() -> {
                    leaf.searchNearestVectors(FIELD, query, collector, acceptDocs(leaf, accept));
                    return null;
                }));
            }
            Exception failure = null;
            for (Future<?> search : searches) {
                try {
                    search.get(30, TimeUnit.SECONDS); // a search that never returns fails the test instead of hanging it
                } catch (ExecutionException e) {
                    failure = e;
                }
            }
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static Result result(IVFSegmentScan scan) {
        return result(scan.finalCollector(), scan.visitedCount());
    }

    private static Result result(KnnCollector collector, long visited) {
        List<Integer> docs = new ArrayList<>();
        List<Float> scores = new ArrayList<>();
        for (ScoreDoc hit : collector == null ? new ScoreDoc[0] : collector.topDocs().scoreDocs) {
            docs.add(hit.doc);
            scores.add(hit.score);
        }
        return new Result(visited, docs, scores);
    }

    /** Compiled and interpreted code can round a float sum differently, so search before comparing results bit for bit. */
    private static void warmUp(LeafReader leaf, int dims) throws Exception {
        for (int q = 0; q < 30; q++) {
            float visitRatio = randomBoolean() ? 0f : randomFloatBetween(0.01f, 0.3f, true);
            new Search(leaf, randomVector(dims), randomAcceptBits(leaf), visitRatio, null).inParts(randomIntBetween(1, 8));
        }
    }

    private static Set<String> prefetches(Set<String> prefetched, CheckedRunnable<Exception> search) throws Exception {
        prefetched.clear();
        search.run();
        return Set.copyOf(prefetched);
    }

    private static IndexWriterConfig config(KnnVectorsFormat format) {
        return newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(format));
    }

    private static void indexSegment(Directory dir, int dims, int docs) throws IOException {
        try (IndexWriter writer = new IndexWriter(dir, config(new ES950DiskBBQVectorsFormat(64, randomIntBetween(2, 4))))) {
            for (int i = 0; i < docs; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField(FIELD, randomVector(dims), DOT_PRODUCT));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
    }

    private static float[] randomVector(int dims) {
        return VectorTestUtils.randomNormalizedFloatVector(dims);
    }

    private static AcceptDocs acceptDocs(LeafReader leaf, Bits accept) {
        if (accept != null) {
            return new ESAcceptDocs.BitsAcceptDocs(accept, leaf.maxDoc());
        }
        return leaf.getLiveDocs() == null
            ? new ESAcceptDocs.ESAcceptDocsAll()
            : new ESAcceptDocs.BitsAcceptDocs(leaf.getLiveDocs(), leaf.maxDoc());
    }

    /** null (no filter), or a random 0.5% to 100% of the live docs. */
    private static Bits randomAcceptBits(LeafReader leaf) {
        if (randomBoolean()) {
            return null;
        }
        double selectivity = randomFrom(0.005, 0.05, 0.5, 1.0);
        FixedBitSet bits = new FixedBitSet(leaf.maxDoc());
        Bits live = leaf.getLiveDocs();
        for (int doc = 0; doc < leaf.maxDoc(); doc++) {
            if ((live == null || live.get(doc)) && random().nextDouble() < selectivity) {
                bits.set(doc);
            }
        }
        return bits;
    }

    private static final class PrefetchRecordingDirectory extends FilterDirectory {
        private final Set<String> prefetched;

        PrefetchRecordingDirectory(Directory in, Set<String> prefetched) {
            super(in);
            this.prefetched = prefetched;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            return new PrefetchRecordingInput(name, 0, in.openInput(name, context), prefetched);
        }
    }

    /** Records its prefetches by their position in the file. */
    private static final class PrefetchRecordingInput extends FilterIndexInput {
        private final String file;
        private final long start; // where this input starts in the file
        private final Set<String> prefetched;

        PrefetchRecordingInput(String file, long start, IndexInput in, Set<String> prefetched) {
            super("PrefetchRecordingInput(" + in + ")", in);
            this.file = file;
            this.start = start;
            this.prefetched = prefetched;
        }

        @Override
        public void prefetch(long offset, long length) throws IOException {
            prefetched.add(file + "@" + (start + offset) + "+" + length);
            in.prefetch(offset, length);
        }

        @Override
        public IndexInput slice(String sliceDescription, long offset, long length) throws IOException {
            return new PrefetchRecordingInput(file, start + offset, in.slice(sliceDescription, offset, length), prefetched);
        }

        @Override
        public PrefetchRecordingInput clone() {
            return new PrefetchRecordingInput(file, start, in.clone(), prefetched);
        }
    }

    /** Stalls while its score cutoff is read, and notes any use after its hits were taken out of it. */
    private static final class StallingCollector extends MaxScoreTopKnnCollector {
        private final AtomicBoolean usedAfterItsHitsWereTaken;
        private volatile boolean hitsTaken;

        StallingCollector(int k, IVFKnnSearchStrategy strategy, AtomicBoolean usedAfterItsHitsWereTaken) {
            super(k, Long.MAX_VALUE, strategy);
            this.usedAfterItsHitsWereTaken = usedAfterItsHitsWereTaken;
        }

        @Override
        public long getMinCompetitiveDocScore() {
            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
            noteUse();
            return super.getMinCompetitiveDocScore();
        }

        @Override
        public int bulkCollect(int[] docs, float[] scores, int count, float bestScore) {
            noteUse();
            return super.bulkCollect(docs, scores, count, bestScore);
        }

        @Override
        public TopDocs unsortedTopK() {
            hitsTaken = true;
            return super.unsortedTopK();
        }

        private void noteUse() {
            if (hitsTaken) {
                usedAfterItsHitsWereTaken.set(true);
            }
        }
    }

    /** The nested collector, yielding at random like {@link DelayingCollector}. */
    private static final class DelayingNestedCollector extends DiversifyingNearestChildrenKnnCollector {
        private final Random random;

        DelayingNestedCollector(int k, IVFKnnSearchStrategy strategy, BitSet parents, Random random) {
            super(k, Integer.MAX_VALUE, strategy, parents);
            this.random = random;
        }

        @Override
        public boolean collect(int docId, float nodeScore) {
            if (random.nextInt(64) == 0) {
                Thread.yield();
            }
            return super.collect(docId, nodeScore);
        }
    }

    /** Yields at random, to vary which part takes which posting list. */
    private static final class DelayingCollector extends MaxScoreTopKnnCollector {
        private final Random random;
        volatile boolean failsOnFirstCollect;

        DelayingCollector(int k, IVFKnnSearchStrategy strategy, Random random) {
            super(k, Long.MAX_VALUE, strategy);
            this.random = random;
        }

        @Override
        public int bulkCollect(int[] docs, float[] scores, int count, float bestScore) {
            if (failsOnFirstCollect) {
                throw new IllegalStateException("injected failure");
            }
            if (random.nextInt(4) == 0) {
                Thread.yield();
            }
            return super.bulkCollect(docs, scores, count, bestScore);
        }
    }
}
