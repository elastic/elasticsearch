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
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.misc.store.DirectIODirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MMapDirectory;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.Elasticsearch92Lucene103Codec;
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
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Fork(value = 1)
@Warmup(iterations = 5)
@Measurement(iterations = 5)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class VectorIOBenchmark {
    static {
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    private static final String TEST_DIR = "/home/esbench/.rally/benchmarks/races/index";

    private final Random random = new Random();
    private Directory dir;
    private IndexReader reader;
    private ExecutorService executor;

    private float[] queryVector;

    @Param({ "20000" })
    private int numVectors;

    @Param({ "1024" })
    private int dims;

    @Param({ "100" })
    private int numVectorPerThread;

    @Param({ "1" })
    private int readThreads;

    @Param({ "true", "false" })
    private boolean prefetch;

    @Param({ "MMAP" })
    private String readMethod;

    @Param({ "false" })
    private boolean inMemory;

    @Setup
    @Fork(jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
    public void setUp() throws IOException {
        Logger log = LogManager.getLogger(VectorIOBenchmark.class);
        switch (readMethod) {
            case "DIRECT_IO":
                var delegate = new MMapDirectory(Path.of(TEST_DIR));
                dir = new FsDirectoryFactory.AlwaysDirectIODirectory(delegate, 8192, DirectIODirectory.DEFAULT_MIN_BYTES_DIRECT, 64);
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
                        System.out.println(new Date() + " Indexing " + docID + "/" + numVectors);
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
        return random.nextInt(0, inMemory ? numVectors / 500 : numVectors - 1);
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

                if (prefetch) {
                    prefetch(docs);
                }
                return scoreIndividually(docs);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private IndexInput getSlice(FloatVectorValues values) {
            if (values instanceof HasIndexSlice slice) {
                return slice.getSlice();
            }
            throw new IllegalStateException("Instance is not compatible with index slice");
        }

        public void prefetch(int[] docs) throws IOException {
            int seg = ReaderUtil.subIndex(docs[0], reader.leaves());
            var leaf = reader.leaves().get(seg);
            var vectorValues = leaf.reader().getFloatVectorValues("vector");
            var slice = getSlice(vectorValues);
            for (int i = 0; i < docs.length; i++) {
                if (docs[i] >= leaf.docBase + leaf.reader().numDocs()) {
                    seg = ReaderUtil.subIndex(docs[i], reader.leaves());
                    leaf = reader.leaves().get(seg);
                    vectorValues = leaf.reader().getFloatVectorValues("vector");
                    slice = getSlice(vectorValues);
                }
                long offset = (docs[i] - leaf.docBase) * dims * 4L;
                slice.prefetch(offset, dims * 4L);
            }
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
