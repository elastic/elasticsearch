/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.mapper;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.index.codec.vectors.diskbbq.ES920DiskBBQVectorsFormat;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Compares the cost of updating one field of a document two ways: reindexing the whole document versus an in-place doc-values update
 * (the {@code doc_values.updatable} feature). Each document is deliberately expensive to index — a large dense vector stored with disk
 * BBQ plus a few analyzed text fields — so it models the case the feature targets: the field being changed is cheap, but the document
 * around it is not.
 *
 * <p>Each measured invocation starts from a freshly built, committed base index of {@code numDocs} documents (built in {@code @Setup},
 * not measured), applies {@code updateBatch} updates, and flushes. The reindex path re-adds the vector — forcing BBQ quantization and
 * clustering for the new segment — while the doc-values path only rewrites the small keyword column.
 *
 * <p>Representative result (numDocs=2000, updateBatch=100, AverageTime, ms/op):
 * <pre>
 *   operation             512 dims   1024 dims
 *   doc-values update       ~2.4        ~3.0
 *   full reindex            ~8.9       ~10.6
 * </pre>
 * The doc-values update is ~3.6x cheaper, and its cost barely moves with the vector dimensionality while reindex scales with the
 * document's indexing cost. Two things this flush-level measurement <em>under</em>-states: (1) the vector graph/IVF is fully rebuilt on
 * <em>merge</em>, a cost reindex pays and a doc-values update does not, so the real-world gap is larger; (2) a doc-values update rewrites
 * the whole updated column of every touched segment, so its cost is dominated by segment size rather than by {@code updateBatch} — the
 * write-amplification trade-off of the feature.
 */
@Fork(1)
@Warmup(iterations = 2)
@Measurement(iterations = 4)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
public class DocValuesUpdateVsReindexBenchmark {

    static {
        // The Elasticsearch disk-BBQ vector format touches Elasticsearch logging during class init, which NPEs unless the logging SPI
        // has been configured. Set it up before anything can trigger that lookup.
        Utils.configureBenchmarkLogging();
    }

    private static final String VECTOR_FIELD = "vector";
    private static final String STATUS_FIELD = "status";
    private static final String ID_FIELD = "_id";

    @Param({ "512", "1024" })
    private int dims;

    @Param({ "5000" })
    private int numDocs;

    @Param({ "200" })
    private int updateBatch;

    private Path tempDir;
    private Directory directory;
    private IndexWriter writer;
    private Codec codec;
    private final Random random = new Random(42);

    @Setup(Level.Trial)
    public void setupCodec() {
        KnnVectorsFormat bbq = new ES920DiskBBQVectorsFormat();
        Codec defaultCodec = Codec.getDefault();
        codec = new FilterCodec(defaultCodec.getName(), defaultCodec) {
            @Override
            public KnnVectorsFormat knnVectorsFormat() {
                return new PerFieldKnnVectorsFormat() {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                        return bbq;
                    }
                };
            }
        };
    }

    /** Build a fresh committed base index before every measured invocation, so each update path starts from the same clean state. */
    @Setup(Level.Invocation)
    public void buildBaseIndex() throws IOException {
        tempDir = Files.createTempDirectory("dv-update-bench");
        directory = FSDirectory.open(tempDir);
        IndexWriterConfig config = new IndexWriterConfig(new StandardAnalyzer()).setCodec(codec)
            .setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
            .setUseCompoundFile(false);
        writer = new IndexWriter(directory, config);
        for (int i = 0; i < numDocs; i++) {
            writer.addDocument(newDocument(Integer.toString(i)));
        }
        writer.commit();
    }

    @TearDown(Level.Invocation)
    public void closeBaseIndex() throws IOException {
        IOUtils.close(writer, directory);
        IOUtils.rm(tempDir);
    }

    private Document newDocument(String id) {
        Document doc = new Document();
        doc.add(new StringField(ID_FIELD, id, Field.Store.NO));
        doc.add(new KnnFloatVectorField(VECTOR_FIELD, randomVector(), VectorSimilarityFunction.DOT_PRODUCT));
        // a few analyzed text fields, re-analyzed on every reindex
        doc.add(new TextField("title", "the quick brown fox jumps over the lazy dog " + id, Field.Store.NO));
        doc.add(new TextField("body", "lorem ipsum dolor sit amet consectetur adipiscing elit " + id, Field.Store.NO));
        doc.add(new BinaryDocValuesField(STATUS_FIELD, new BytesRef("active")));
        return doc;
    }

    private float[] randomVector() {
        float[] v = new float[dims];
        double norm = 0;
        for (int i = 0; i < dims; i++) {
            v[i] = random.nextFloat() - 0.5f;
            norm += v[i] * v[i];
        }
        norm = Math.sqrt(norm);
        for (int i = 0; i < dims; i++) {
            v[i] /= (float) norm;
        }
        return v;
    }

    @Benchmark
    public void reindexFullDocument() throws IOException {
        for (int i = 0; i < updateBatch; i++) {
            String id = Integer.toString(random.nextInt(numDocs));
            writer.softUpdateDocument(new Term(ID_FIELD, id), newDocument(id), new NumericDocValuesField(Lucene.SOFT_DELETES_FIELD, 1));
        }
        writer.flush();
    }

    @Benchmark
    public void docValuesUpdate() throws IOException {
        for (int i = 0; i < updateBatch; i++) {
            String id = Integer.toString(random.nextInt(numDocs));
            writer.updateBinaryDocValue(new Term(ID_FIELD, id), STATUS_FIELD, new BytesRef("updated-" + i));
        }
        writer.flush();
    }
}
