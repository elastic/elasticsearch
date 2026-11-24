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
import org.apache.lucene.index.VectorSimilarityFunction;
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
import org.elasticsearch.index.codec.vectors.diskbbq.IVFSegmentScan;
import org.elasticsearch.index.codec.vectors.diskbbq.es95.ES950DiskBBQVectorsFormat;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.test.ESTestCase;

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

/**
 * Several threads ("parts") searching one segment through a shared {@link IVFSegmentScan} must visit exactly the
 * postings, and return exactly the hits, of a single part. Random delays in the parts make the claim order vary.
 */
public class IVFSegmentScanConcurrencyTests extends ESTestCase {

    private static final String FIELD = "v";
    private static final int K = 20;

    public void testPartsMatchASinglePart() throws Exception {
        int dims = randomIntBetween(8, 64);
        float[] repeated = randomVector(dims);
        KnnVectorsFormat format = randomBoolean()
            ? new ES950DiskBBQVectorsFormat(64, randomIntBetween(2, 4))
            : new ESNextDiskBBQVectorsFormat(64, randomIntBetween(2, 4), null);
        IndexWriterConfig config = newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(format));
        ExecutorService threads = Executors.newFixedThreadPool(8);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, config)) {
            int numDocs = randomIntBetween(3_000, 8_000);
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                // exact duplicates produce score ties, which must be broken the same way however the work is split
                doc.add(
                    new KnnFloatVectorField(FIELD, randomInt(4) == 0 ? repeated : randomVector(dims), VectorSimilarityFunction.DOT_PRODUCT)
                );
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
                warmUp(threads, leaf, dims);
                for (int q = 0; q < 20; q++) {
                    float[] query = randomBoolean() ? repeated : randomVector(dims);
                    Bits accept = randomAcceptBits(leaf);
                    float visitRatio = randomBoolean() ? 0f : randomFloatBetween(0.01f, 0.3f, true);
                    Result alone = searchAlone(threads, leaf, query, accept, visitRatio, null);
                    Result one = searchInParts(threads, leaf, query, accept, visitRatio, 1, -1, null);
                    Result many = searchInParts(threads, leaf, query, accept, visitRatio, randomIntBetween(2, 8), -1, null);
                    assertEquals("visited", alone.visited, one.visited);
                    assertArrayEquals("docs", alone.docs, one.docs);
                    assertArrayEquals("scores", alone.scores, one.scores, 0f);
                    assertEquals("visited", one.visited, many.visited);
                    assertArrayEquals("docs", one.docs, many.docs);
                    assertArrayEquals("scores", one.scores, many.scores, 0f);
                    assertPreparingFirstChangesNothing(threads, leaf, query, accept, visitRatio, null, one);
                }
            }
        } finally {
            terminate(threads);
        }
    }

    /**
     * The same for a nested field. Its collector keeps the best child of each parent and is full once it holds enough
     * parents, however many children were scored: any number of parts must find the parents and children a single part finds.
     */
    public void testNestedPartsMatchASinglePart() throws Exception {
        int dims = randomIntBetween(8, 64);
        float[] repeated = randomVector(dims);
        IndexWriterConfig config = newIndexWriterConfig().setCodec(
            TestUtil.alwaysKnnVectorsFormat(new ES950DiskBBQVectorsFormat(64, randomIntBetween(2, 4)))
        );
        ExecutorService threads = Executors.newFixedThreadPool(8);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, config)) {
            int numParents = randomIntBetween(800, 2_000);
            for (int p = 0; p < numParents; p++) {
                List<Document> block = new ArrayList<>();
                int children = randomIntBetween(1, 8);
                // The chunks of one doc are often alike: they then land in the same posting lists, so many children are scored
                // for few parents, and their scores tie. Copies of one vector make parents tie with each other too.
                float[] alike = randomInt(3) == 0 ? null : randomInt(4) == 0 ? repeated : randomVector(dims);
                for (int c = 0; c < children; c++) {
                    Document child = new Document();
                    float[] vector = alike != null ? alike : randomVector(dims);
                    child.add(new KnnFloatVectorField(FIELD, vector, VectorSimilarityFunction.DOT_PRODUCT));
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
                warmUp(threads, leaf, dims);
                for (int q = 0; q < 20; q++) {
                    float[] query = randomBoolean() ? repeated : randomVector(dims);
                    Bits accept = randomAcceptBits(leaf);
                    // a small budget often ends before enough parents were found, so the search has to go on past it
                    float visitRatio = randomFrom(0f, 0.01f, 0.05f, 0.3f);
                    Result alone = searchAlone(threads, leaf, query, accept, visitRatio, parents);
                    Result many = searchInParts(threads, leaf, query, accept, visitRatio, randomIntBetween(2, 8), -1, parents);
                    assertEquals("visited", alone.visited, many.visited);
                    assertArrayEquals("docs", alone.docs, many.docs);
                    assertArrayEquals("scores", alone.scores, many.scores, 0f);
                    assertPreparingFirstChangesNothing(threads, leaf, query, accept, visitRatio, parents, alone);
                }
            }
        } finally {
            terminate(threads);
        }
    }

    public void testAFailingPartStopsTheOthers() throws Exception {
        int dims = randomIntBetween(8, 32);
        // the first part fails on its first collect; it is submitted first and the scan has dozens of postings, so it does
        // claim one
        IndexWriterConfig config = newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(new ES950DiskBBQVectorsFormat(64, 2)));
        ExecutorService threads = Executors.newFixedThreadPool(8);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, config)) {
            for (int i = 0; i < 4_000; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField(FIELD, randomVector(dims), VectorSimilarityFunction.DOT_PRODUCT));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                Exception e = expectThrows(Exception.class, () -> searchInParts(threads, leaf, randomVector(dims), null, 1f, 4, 0, null));
                assertTrue(e.getMessage(), e.getMessage().contains("injected"));
            }
        } finally {
            terminate(threads);
        }
    }

    /**
     * Once every posting list within the budget is recorded as visited, another part may gather a part's hits out of its
     * collector. So a part must be done with its collector before it records a posting list, also with publishing its score
     * cutoff. Here a collector stalls while its cutoff is read, which lets the other parts get ahead, and notes any use
     * after its hits were taken.
     */
    public void testAPartIsDoneWithItsCollectorBeforeItsHitsAreGathered() throws Exception {
        int dims = randomIntBetween(8, 32);
        IndexWriterConfig config = newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(new ES950DiskBBQVectorsFormat(64, 2)));
        ExecutorService threads = Executors.newFixedThreadPool(4);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, config)) {
            for (int i = 0; i < 4_000; i++) {
                Document doc = new Document();
                doc.add(new KnnFloatVectorField(FIELD, randomVector(dims), VectorSimilarityFunction.DOT_PRODUCT));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            try (DirectoryReader reader = DirectoryReader.open(writer)) {
                LeafReader leaf = getOnlyLeafReader(reader);
                for (int q = 0; q < 10; q++) {
                    float[] query = randomVector(dims);
                    IVFSegmentScan scan = new IVFSegmentScan();
                    SharedMinCompetitiveScore cutoff = new SharedMinCompetitiveScore();
                    AtomicBoolean usedAfterItsHitsWereTaken = new AtomicBoolean();
                    List<Future<?>> parts = new ArrayList<>();
                    for (int p = 0; p < 4; p++) {
                        IVFKnnSearchStrategy strategy = new IVFKnnSearchStrategy(0.1f, K, K, cutoff, scan);
                        StallingCollector collector = new StallingCollector(K, strategy, usedAfterItsHitsWereTaken);
                        strategy.setCollector(collector);
                        parts.add(threads.submit(() -> {
                            leaf.searchNearestVectors(FIELD, query, collector, acceptDocs(leaf, null));
                            return null;
                        }));
                    }
                    for (Future<?> part : parts) {
                        part.get(30, TimeUnit.SECONDS);
                    }
                    assertNotNull("the parts searched the segment", scan.finalCollector());
                    assertFalse("a part used its collector after another part took its hits", usedAfterItsHitsWereTaken.get());
                }
            }
        } finally {
            terminate(threads);
        }
    }

    /**
     * A segment's search prefetches the same posting lists however many parts share its scan. The listing walks the budget
     * once, prefetching each posting list as it goes, so reading further ahead per part would only reach past the budget, into
     * posting lists the search may never visit (on serverless, each one a possible download).
     */
    public void testPartsPrefetchWhatASearchOnItsOwnPrefetches() throws Exception {
        int dims = randomIntBetween(8, 64);
        IndexWriterConfig config = newIndexWriterConfig().setCodec(
            TestUtil.alwaysKnnVectorsFormat(new ES950DiskBBQVectorsFormat(64, randomIntBetween(2, 4)))
        );
        ExecutorService threads = Executors.newFixedThreadPool(8);
        Set<String> prefetched = ConcurrentHashMap.newKeySet();
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, config)) {
                int numDocs = randomIntBetween(4_000, 8_000);
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    doc.add(new KnnFloatVectorField(FIELD, randomVector(dims), VectorSimilarityFunction.DOT_PRODUCT));
                    writer.addDocument(doc);
                }
                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(new PrefetchRecordingDirectory(dir, prefetched))) {
                LeafReader leaf = getOnlyLeafReader(reader);
                warmUp(threads, leaf, dims); // compiled code only, so near-equal centroids are ordered the same in every search
                for (int q = 0; q < 10; q++) {
                    float[] query = randomVector(dims);
                    Bits accept = randomAcceptBits(leaf);
                    // a small budget, so the segment has posting lists past it
                    float visitRatio = randomFloatBetween(0.01f, 0.1f, true);
                    int parts = randomIntBetween(2, 8);
                    Set<String> alone = prefetches(prefetched, () -> runParts(threads, leaf, query, accept, visitRatio, null, 1, -1, null));
                    Set<String> one = prefetches(
                        prefetched,
                        () -> runParts(threads, leaf, query, accept, visitRatio, new IVFSegmentScan(), 1, -1, null)
                    );
                    Set<String> many = prefetches(
                        prefetched,
                        () -> runParts(threads, leaf, query, accept, visitRatio, new IVFSegmentScan(), parts, -1, null)
                    );
                    assertFalse("the search prefetches posting lists", alone.isEmpty());
                    assertEquals("one part", alone, one);
                    assertEquals(parts + " parts", alone, many);
                }
            }
        } finally {
            terminate(threads);
        }
    }

    /** The ranges prefetched while {@code search} runs. */
    private static Set<String> prefetches(Set<String> prefetched, CheckedRunnable<Exception> search) throws Exception {
        prefetched.clear();
        search.run();
        return Set.copyOf(prefetched);
    }

    private record Result(long visited, int[] docs, float[] scores) {}

    /**
     * Panama's float reductions don't fix their order of additions, so interpreted and compiled code can round the last bit
     * differently. Searching first lets the bit-exact comparisons compare compiled code with compiled code.
     */
    private static void warmUp(ExecutorService threads, LeafReader leaf, int dims) throws Exception {
        for (int q = 0; q < 30; q++) {
            float visitRatio = randomBoolean() ? 0f : randomFloatBetween(0.01f, 0.3f, true);
            searchInParts(threads, leaf, randomVector(dims), randomAcceptBits(leaf), visitRatio, randomIntBetween(1, 8), -1, null);
        }
    }

    /**
     * A split leaf is prepared before its parts run: one part plans the search and lists the budget without claiming any of
     * it. Preparing first must change nothing, and the parts must share the query quantized per centroid.
     */
    private static void assertPreparingFirstChangesNothing(
        ExecutorService threads,
        LeafReader leaf,
        float[] query,
        Bits accept,
        float visitRatio,
        BitSet parents,
        Result notPrepared
    ) throws Exception {
        int parts = randomIntBetween(1, 8);
        IVFSegmentScan scan = new IVFSegmentScan();
        scan.holdClaims();
        runParts(threads, leaf, query, accept, visitRatio, scan, 1, -1, parents);
        scan.releaseClaims();
        runParts(threads, leaf, query, accept, visitRatio, scan, parts, -1, parents);
        assertFalse("the parts share the query quantized per centroid", scan.quantizedQueries().isEmpty());
        Result prepared = result(scan.finalCollector(), scan.visitedCount());
        assertEquals("visited", notPrepared.visited, prepared.visited);
        assertArrayEquals("docs", notPrepared.docs, prepared.docs);
        assertArrayEquals("scores", notPrepared.scores, prepared.scores, 0f);
    }

    /** Searches one leaf with a single search on its own, without a shared scan. */
    private static Result searchAlone(
        ExecutorService threads,
        LeafReader leaf,
        float[] query,
        Bits accept,
        float visitRatio,
        BitSet parents
    ) throws Exception {
        KnnCollector collector = runParts(threads, leaf, query, accept, visitRatio, null, 1, -1, parents).get(0);
        return result(collector, collector.visitedCount());
    }

    /**
     * Searches one leaf with {@code parts} threads sharing a scan; {@code failingPart} >= 0 makes that part throw. The hits
     * are those of the collector the scan gathered every part's hits into.
     */
    private static Result searchInParts(
        ExecutorService threads,
        LeafReader leaf,
        float[] query,
        Bits accept,
        float visitRatio,
        int parts,
        int failingPart,
        BitSet parents
    ) throws Exception {
        IVFSegmentScan scan = new IVFSegmentScan();
        runParts(threads, leaf, query, accept, visitRatio, scan, parts, failingPart, parents);
        return result(scan.finalCollector(), scan.visitedCount());
    }

    /**
     * Runs {@code parts} searches of one leaf through {@code scan} (null: one search on its own), each on its own thread,
     * and returns their collectors. With {@code parents}, the leaf holds nested docs and the collectors keep the best child
     * of each parent.
     */
    private static List<AbstractMaxScoreKnnCollector> runParts(
        ExecutorService threads,
        LeafReader leaf,
        float[] query,
        Bits accept,
        float visitRatio,
        IVFSegmentScan scan,
        int parts,
        int failingPart,
        BitSet parents
    ) throws Exception {
        List<AbstractMaxScoreKnnCollector> collectors = new ArrayList<>();
        List<Future<?>> futures = new ArrayList<>();
        for (int p = 0; p < parts; p++) {
            IVFKnnSearchStrategy strategy = new IVFKnnSearchStrategy(visitRatio, K, K, null, scan);
            Random random = new Random(random().nextLong());
            AbstractMaxScoreKnnCollector collector = parents == null
                ? new DelayingCollector(K, strategy, p == failingPart, random)
                : new DelayingNestedCollector(K, strategy, parents, random);
            strategy.setCollector(collector);
            collectors.add(collector);
            futures.add(threads.submit(() -> {
                leaf.searchNearestVectors(FIELD, query, collector, acceptDocs(leaf, accept));
                return null;
            }));
        }
        Exception failure = null;
        for (Future<?> future : futures) {
            try {
                future.get(30, TimeUnit.SECONDS); // a part that never returns fails the test instead of hanging it
            } catch (ExecutionException e) {
                failure = e;
            }
        }
        if (failure != null) {
            throw failure;
        }
        return collectors;
    }

    /** A collector's hits, best first, with the given visited count; no hits if no part visited anything. */
    private static Result result(KnnCollector collector, long visited) {
        ScoreDoc[] hits = collector == null ? new ScoreDoc[0] : collector.topDocs().scoreDocs;
        int[] docs = new int[hits.length];
        float[] scores = new float[hits.length];
        for (int i = 0; i < hits.length; i++) {
            docs[i] = hits[i].doc;
            scores[i] = hits[i].score;
        }
        return new Result(visited, docs, scores);
    }

    private static AcceptDocs acceptDocs(LeafReader leaf, Bits accept) {
        if (accept != null) {
            return new ESAcceptDocs.BitsAcceptDocs(accept, leaf.maxDoc());
        }
        return leaf.getLiveDocs() == null
            ? new ESAcceptDocs.ESAcceptDocsAll()
            : new ESAcceptDocs.BitsAcceptDocs(leaf.getLiveDocs(), leaf.maxDoc());
    }

    /** null (no filter), or live docs restricted to a random selectivity between 0.5% and 100%. */
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

    private static float[] randomVector(int dims) {
        float[] v = new float[dims];
        double norm = 0;
        for (int i = 0; i < dims; i++) {
            v[i] = random().nextFloat() * 2 - 1;
            norm += v[i] * v[i];
        }
        for (int i = 0; i < dims; i++) {
            v[i] /= (float) Math.sqrt(norm);
        }
        return v;
    }

    /**
     * Records every range prefetched from its files, as file, start and length, so a test can compare what different searches
     * read ahead. A real directory is wrapped rather than mocked: the reader opens, slices and clones its inputs as usual.
     */
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

    /** An input that records its prefetches by their position in the file, and wraps its slices and clones the same way. */
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

    /** The collector of nested fields, yielding at random to shuffle which part claims which posting list. */
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

    /** Yields at random to shuffle which part claims which posting; optionally fails on its first collect. */
    private static final class DelayingCollector extends MaxScoreTopKnnCollector {
        private final boolean fail;
        private final Random random;

        DelayingCollector(int k, IVFKnnSearchStrategy strategy, boolean fail, Random random) {
            super(k, Long.MAX_VALUE, strategy);
            this.fail = fail;
            this.random = random;
        }

        @Override
        public int bulkCollect(int[] docs, float[] scores, int count, float bestScore) {
            if (fail) {
                throw new IllegalStateException("injected failure");
            }
            if (random.nextInt(4) == 0) {
                Thread.yield();
            }
            return super.bulkCollect(docs, scores, count, bestScore);
        }
    }
}
