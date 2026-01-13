/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector;

import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.search.DocAndFloatFeatureBuffer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.Elasticsearch92Lucene103Codec;
import org.elasticsearch.index.codec.vectors.BulkScorableFloatVectorValues;
import org.elasticsearch.index.codec.vectors.es93.ES93FlatVectorFormat;
import org.elasticsearch.index.store.FsDirectoryFactory;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
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

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.List;

@Fork(value = 1, jvmArgsPrepend = { "--enable-native-access=ALL-UNNAMED" })
@Warmup(iterations = 2)
@Measurement(iterations = 2)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class VectorIOBenchmark {
    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    private static final String TEST_DIR = "/home/esbench/.rally/benchmarks/races/test";

    private final Random random = new Random();
    private Directory dir;
    private IndexReader reader;
    private ExecutorService executor;

    private float[] queryVector;

    @Param({ "200000000" })
    private int numVectors;

    @Param({ "1024" })
    private int dims;

    @Param({ "100" })
    private int numVectorPerThread;

    @Param({ "1", "32" })
    private int readThreads;

    @Param({ "true", "false" })
    private boolean prefetch;

    @Param({ "MMAP" })
    private String readMethod;

    @Param({ "true", "false" })
    private boolean batch;

    @Setup
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public void setUp() throws IOException {
        Logger log = LogManager.getLogger(VectorIOBenchmark.class);
        switch (readMethod) {
            case "DIRECT_IO":
                var delegate = new MMapDirectory(Path.of(TEST_DIR));
                dir = new FsDirectoryFactory.AlwaysDirectIODirectory(delegate, 8192, DirectIODirectory.DEFAULT_MIN_BYTES_DIRECT, 0);
                break;
            case "MMAP":
                var mmapDir = new MMapDirectory(Path.of(TEST_DIR));
                mmapDir.setReadAdvice(FsDirectoryFactory.getReadAdviceFunc());
                dir = mmapDir;
                break;
            default:
                throw new IllegalArgumentException("Unknown read method [" + readMethod + "]");
        }

        int currentVectors;
        try {
            var reader = DirectoryReader.open(dir);
            currentVectors = reader.numDocs();
        } catch (IOException e) {
            currentVectors = 0;
        }

        if (currentVectors < numVectors) {
            IndexWriterConfig iwc = new IndexWriterConfig();
            iwc.setUseCompoundFile(false);
            iwc.setCodec(new Elasticsearch92Lucene103Codec() {
                @Override
                public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                    return new ES93FlatVectorFormat();
                }
            });
            iwc.setMaxBufferedDocs(1000000);
            iwc.setRAMBufferSizeMB(4096);
            try (IndexWriter writer = new IndexWriter(dir, iwc)) {
                writer.deleteAll();
                Document doc = new Document();
                for (int docID = 0; docID < numVectors; docID++) {
                    if (docID % 1_000_000 == 0) {
                        System.out.println("Indexing " + docID + "/" + numVectors);
                    }
                    doc.clear();
                    doc.add(new KnnFloatVectorField("vector", randomVector(random, dims), VectorSimilarityFunction.COSINE));
                    writer.addDocument(doc);
                }
            }
        }

        this.reader = DirectoryReader.open(dir);

        this.executor = Executors.newFixedThreadPool(readThreads);
        this.queryVector = randomVector(random, dims);
    }

    @TearDown
    public void tearDown() throws IOException {
        reader.close();
        dir.close();
        executor.shutdown();
    }

    private static float[] randomVector(Random random, int dims) {
        float[] vec = new float[dims];
        for (int i = 0; i < vec.length; i++) {
            vec[i] = random.nextFloat();
        }
        return vec;
    }

    private int randomDoc() {
        return random.nextInt(0, numVectors-1);
    }

    @Benchmark
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public double run() throws IOException, ExecutionException, InterruptedException {
        List<Future<Double>> futures = new ArrayList<>();
        for (int i = 0; i < readThreads; i++) {
            futures.add(executor.submit(new ReadThread(reader, numVectorPerThread, dims)));
        }
        double sim = 0;
        for (Future<Double> future : futures) {
            sim += future.get();
        }
        return sim;
    }

    class ReadThread implements Callable<Double> {
        private final IndexReader reader;
        private final int numRead;
        private final int dims;

        ReadThread(IndexReader reader, int numRead, int dims) {
            this.reader = reader;
            this.numRead = numRead;
            this.dims = dims;
        }

        @Override
        public Double call() {
            int[] docs = new int[numRead];
            try {
                for (int i = 0; i < numRead; i++) {
                    docs[i] = randomDoc();
                }
                Arrays.sort(docs);

                if (batch) {
                    return scoreBatch(docs);
                } else {
                    if (prefetch) {
                        prefetch(docs);
                    }
                    return scoreIndividually(docs);
                }
            } catch(IOException e){
                throw new RuntimeException(e);
            }
        }

        public void prefetch(int[] docs) throws IOException {
            int seg = ReaderUtil.subIndex(docs[0], reader.leaves());
            var leaf = reader.leaves().get(seg);
            var vectorValues = leaf.reader().getFloatVectorValues("vector");
            var slice = vectorValues instanceof HasIndexSlice s ? s.getSlice() : null;
            for (int i = 0; i < docs.length; i++) {
                if (docs[i] >= leaf.docBase + leaf.reader().numDocs()) {
                    seg = ReaderUtil.subIndex(docs[i], reader.leaves());
                    leaf = reader.leaves().get(seg);
                    vectorValues = leaf.reader().getFloatVectorValues("vector");
                    slice = vectorValues instanceof HasIndexSlice s ? s.getSlice() : null;
                }
                long offset = (docs[i] - leaf.docBase) * dims * 4L;
                slice.prefetch(offset, dims * 4L);
            }
        }

        private int[] findBoundary(int[] docs, int docBase, int docEnd) {
            int start = Arrays.binarySearch(docs, docBase);
            if (start < 0) {
                start = -start - 1;
            }
            if (start >= docs.length) {
                return new int[0];
            }
            int end = Arrays.binarySearch(docs, docEnd);
            if (end < 0) {
                end = -end - 1;
            }
            return Arrays.copyOfRange(docs, start, end);
        }

        public double scoreBatch(int[] docs) throws IOException {
            double res = 0;
            int total = 0;
            for (var leaf : reader.leaves()) {
                var segDocs = findBoundary(docs, leaf.docBase, leaf.docBase + leaf.reader().numDocs());
                if (segDocs.length == 0) {
                    continue;
                }
                var vectorValues = leaf.reader().getFloatVectorValues("vector");
                var bulk = vectorValues instanceof BulkScorableFloatVectorValues b ? b : null;
                var bulkRescorer = bulk.bulkRescorer(queryVector, prefetch).bulkScore(new DocIdSetIterator() {
                    int pos = -1;

                    @Override
                    public int docID() {
                        if (pos == -1) {
                            return -1;
                        }
                        if (pos >= segDocs.length) {
                            return NO_MORE_DOCS;
                        }
                        return segDocs[pos] - leaf.docBase;
                    }

                    @Override
                    public int nextDoc() throws IOException {
                        if (++pos >= segDocs.length) {
                            return NO_MORE_DOCS;
                        }
                        return segDocs[pos] - leaf.docBase;
                    }

                    @Override
                    public int advance(int target) throws IOException {
                        if (pos >= segDocs.length) {
                            return NO_MORE_DOCS;
                        }
                        pos = Arrays.binarySearch(segDocs, Math.max(pos, 0), segDocs.length, target+leaf.docBase);
                        if (pos < 0) {
                            pos = -pos - 1;
                        }
                        if (pos >= segDocs.length) {
                            return NO_MORE_DOCS;
                        }
                        return segDocs[pos] - leaf.docBase;
                    }

                    @Override
                    public long cost() {
                        return segDocs.length;
                    }
                });
                DocAndFloatFeatureBuffer buffer = new DocAndFloatFeatureBuffer();
                bulkRescorer.nextDocsAndScores(docs.length, leaf.reader().getLiveDocs(), buffer);
                for (int i = 0; i < buffer.size; i++) {
                    res += buffer.features[i];
                }
                total += buffer.size;
            }
            if (total != numRead) {
                throw new IllegalStateException("Total scored is less than " + numRead + " " + total);
            }
            return res;
        }

        public double scoreIndividually(int[] docs) throws IOException {
            int seg = ReaderUtil.subIndex(docs[0], reader.leaves());
            var leaf = reader.leaves().get(seg);
            var vectorValues = leaf.reader().getFloatVectorValues("vector");
            var scorer = vectorValues.scorer(queryVector);
            var it = scorer.iterator();
            double res = 0;
            for (int i = 0; i < docs.length; i++) {
                if (docs[i] >= leaf.docBase + leaf.reader().numDocs()) {
                    seg = ReaderUtil.subIndex(docs[i], reader.leaves());
                    leaf = reader.leaves().get(seg);
                    vectorValues = leaf.reader().getFloatVectorValues("vector");
                    scorer = vectorValues.scorer(queryVector);
                    it = scorer.iterator();
                }
                int leafDoc = docs[i] - leaf.docBase;
                it.advance(leafDoc);
                if (it.docID() != leafDoc) {
                    throw new IllegalStateException("Wrong doc " + docs[i]);
                }
                res += scorer.score();
            }
            return res;
        }
    }
}
