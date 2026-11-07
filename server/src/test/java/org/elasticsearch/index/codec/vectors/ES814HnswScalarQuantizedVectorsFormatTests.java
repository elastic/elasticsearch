/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.search.vectors.DenseVectorQuery;

import java.io.IOException;
import java.nio.file.Path;

import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;

public class ES814HnswScalarQuantizedVectorsFormatTests extends BaseQuantizedKnnVectorsFormatTestCase {

    @Override
    protected boolean supportsFloatVectorFallback() {
        return false;
    }

    static final Codec codec = TestUtil.alwaysKnnVectorsFormat(new ES814HnswScalarQuantizedRWVectorsFormat());

    @Override
    protected Codec getCodec() {
        return codec;
    }

    // The following test scenarios are similar to their superclass namesakes,
    // but here we ensure that the Directory implementation is a FSDirectory
    // which helps test the native code vector distance implementation

    public void testAddIndexesDirectory0FS() throws Exception {
        Path root = createTempDir();
        String fieldName = "field";
        Document doc = new Document();
        doc.add(new KnnFloatVectorField(fieldName, new float[4], VectorSimilarityFunction.DOT_PRODUCT));
        try (Directory dir = new MMapDirectory(root.resolve("dir1")); Directory dir2 = new MMapDirectory(root.resolve("dir2"))) {
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
                w.addDocument(doc);
            }
            try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
                w2.addIndexes(dir);
                w2.forceMerge(1);
                try (IndexReader reader = DirectoryReader.open(w2)) {
                    LeafReader r = getOnlyLeafReader(reader);
                    FloatVectorValues vectorValues = r.getFloatVectorValues(fieldName);
                    KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
                    assertEquals(0, iterator.nextDoc());
                    assertEquals(0, vectorValues.vectorValue(iterator.index())[0], 0);
                    assertEquals(NO_MORE_DOCS, iterator.nextDoc());
                }
            }
        }
    }

    public void testAddIndexesDirectory01FSCosine() throws Exception {
        testAddIndexesDirectory01FS(VectorSimilarityFunction.COSINE);
    }

    public void testAddIndexesDirectory01FSDot() throws Exception {
        testAddIndexesDirectory01FS(VectorSimilarityFunction.DOT_PRODUCT);
    }

    public void testAddIndexesDirectory01FSEuclidean() throws Exception {
        testAddIndexesDirectory01FS(VectorSimilarityFunction.EUCLIDEAN);
    }

    public void testAddIndexesDirectory01FSMaxIP() throws Exception {
        testAddIndexesDirectory01FS(VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);
    }

    private void testAddIndexesDirectory01FS(VectorSimilarityFunction similarityFunction) throws Exception {
        Path root = createTempDir();
        String fieldName = "field";
        float[] vector = new float[] { 1f };
        Document doc = new Document();
        doc.add(new KnnFloatVectorField(fieldName, vector, similarityFunction));
        try (Directory dir = new MMapDirectory(root.resolve("dir1")); Directory dir2 = new MMapDirectory(root.resolve("dir2"))) {
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
                w.addDocument(doc);
            }
            try (IndexWriter w2 = new IndexWriter(dir2, newIndexWriterConfig())) {
                vector[0] = 2f;
                w2.addDocument(doc);
                w2.addIndexes(dir);
                w2.forceMerge(1);
                try (IndexReader reader = DirectoryReader.open(w2)) {
                    LeafReader r = getOnlyLeafReader(reader);
                    FloatVectorValues vectorValues = r.getFloatVectorValues(fieldName);
                    KnnVectorValues.DocIndexIterator iterator = vectorValues.iterator();
                    assertEquals(0, iterator.nextDoc());
                    // The merge order is randomized, we might get 1 first, or 2
                    float value = vectorValues.vectorValue(iterator.index())[0];
                    assertTrue(value == 1 || value == 2);
                    assertEquals(1, iterator.nextDoc());
                    value += vectorValues.vectorValue(iterator.index())[0];
                    assertEquals(3f, value, 0);
                }
            }
        }
    }

    public void testSingleVectorPerSegmentCosine() throws Exception {
        testSingleVectorPerSegment(VectorSimilarityFunction.COSINE);
    }

    public void testSingleVectorPerSegmentDot() throws Exception {
        testSingleVectorPerSegment(VectorSimilarityFunction.DOT_PRODUCT);
    }

    public void testSingleVectorPerSegmentEuclidean() throws Exception {
        testSingleVectorPerSegment(VectorSimilarityFunction.EUCLIDEAN);
    }

    public void testSingleVectorPerSegmentMIP() throws Exception {
        testSingleVectorPerSegment(VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT);
    }

    private void testSingleVectorPerSegment(VectorSimilarityFunction sim) throws Exception {
        var codec = getCodec();
        try (Directory dir = new MMapDirectory(createTempDir().resolve("dir1"))) {
            try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig().setCodec(codec))) {
                Document doc2 = new Document();
                doc2.add(new KnnFloatVectorField("field", new float[] { 0.8f, 0.6f }, sim));
                doc2.add(newTextField("id", "A", Field.Store.YES));
                writer.addDocument(doc2);
                writer.commit();

                Document doc1 = new Document();
                doc1.add(new KnnFloatVectorField("field", new float[] { 0.6f, 0.8f }, sim));
                doc1.add(newTextField("id", "B", Field.Store.YES));
                writer.addDocument(doc1);
                writer.commit();

                Document doc3 = new Document();
                doc3.add(new KnnFloatVectorField("field", new float[] { -0.6f, -0.8f }, sim));
                doc3.add(newTextField("id", "C", Field.Store.YES));
                writer.addDocument(doc3);
                writer.commit();

                writer.forceMerge(1);
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                LeafReader leafReader = getOnlyLeafReader(reader);
                StoredFields storedFields = reader.storedFields();
                float[] queryVector = new float[] { 0.6f, 0.8f };
                var hits = leafReader.searchNearestVectors(
                    "field",
                    queryVector,
                    3,
                    AcceptDocs.fromLiveDocs(leafReader.getLiveDocs(), leafReader.maxDoc()),
                    100
                );
                assertThat(hits.scoreDocs, arrayWithSize(3));
                assertEquals("B", storedFields.document(hits.scoreDocs[0].doc).get("id"));
                assertEquals("A", storedFields.document(hits.scoreDocs[1].doc).get("id"));
                assertEquals("C", storedFields.document(hits.scoreDocs[2].doc).get("id"));
            }
        }
    }

    public void testSimpleOffHeapSize() throws IOException {
        float[] vector = randomVector(random().nextInt(12, 500));
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
            Document doc = new Document();
            doc.add(new KnnFloatVectorField("f", vector, DOT_PRODUCT));
            w.addDocument(doc);
            w.commit();
            try (IndexReader reader = DirectoryReader.open(w)) {
                LeafReader r = getOnlyLeafReader(reader);
                if (r instanceof CodecReader codecReader) {
                    KnnVectorsReader knnVectorsReader = codecReader.getVectorReader();
                    if (knnVectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
                        knnVectorsReader = fieldsReader.getFieldReader("f");
                    }
                    var fieldInfo = r.getFieldInfos().fieldInfo("f");
                    var offHeap = knnVectorsReader.getOffHeapByteSize(fieldInfo);

                    assertThat(offHeap, aMapWithSize(3));
                    assertThat(offHeap, hasEntry("vex", 1L));
                    assertThat(offHeap, hasEntry(equalTo("veq"), greaterThan(0L)));
                    assertThat(offHeap, hasEntry("vec", (long) vector.length * Float.BYTES));
                }
            }
        }
    }

    /**
     * Regression test for <a href="https://github.com/elastic/elasticsearch/issues/163972">#163972</a>: exact-knn
     * ({@link DenseVectorQuery}) scorers over segments written with this (pre-9.2 {@code int8_hnsw}) format must be
     * unpositioned when handed out, so they can be combined with other clauses in a conjunction.
     *
     * <p>The legacy Lucene99 scalar-quantized reader relies on the default {@code VectorScorer#bulk}, which advances the
     * scorer's iterator as a side effect. Building the bulk scorer eagerly therefore left the {@link DenseVectorQuery}
     * iterator on its first document, and conjunctions such as the one built for knn {@code inner_hits} (the exact-knn
     * query as a {@code MUST} clause plus the knn filter as a {@code FILTER} clause) failed with
     * {@code Sub-iterators of ConjunctionDISI are not on the same document!}.
     */
    public void testExactKnnConjunctionOnLegacySegments() throws IOException {
        int numDocs = TestUtil.nextInt(random(), 10, 100);
        int expectedMatches = 0;
        try (Directory dir = newDirectory()) {
            try (IndexWriter writer = new IndexWriter(dir, newIndexWriterConfig().setCodec(getCodec()))) {
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    doc.add(new KnnFloatVectorField("field", new float[] { 1f, i + 1f, 2f, 3f }, VectorSimilarityFunction.COSINE));
                    // The first document does not match the filter, so the vector and filter iterators diverge from the start
                    boolean match = i % 2 == 1;
                    doc.add(new StringField("tag", match ? "match" : "other", Field.Store.NO));
                    if (match) {
                        expectedMatches++;
                    }
                    writer.addDocument(doc);
                }
            }
            try (DirectoryReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setQueryCache(null);
                float[] queryVector = new float[] { 1f, 2f, 3f, 4f };
                Query filter = new TermQuery(new Term("tag", "match"));
                Query knnQuery = random().nextBoolean()
                    ? DenseVectorQuery.Floats.codecScored(queryVector, "field")
                    : DenseVectorQuery.Floats.codecScored(queryVector, "field").filteredBy(filter);
                Query conjunction = new BooleanQuery.Builder().add(knnQuery, BooleanClause.Occur.MUST)
                    .add(filter, BooleanClause.Occur.FILTER)
                    .build();

                Weight knnWeight = searcher.createWeight(searcher.rewrite(knnQuery), ScoreMode.COMPLETE, 1f);
                Weight conjunctionWeight = searcher.createWeight(searcher.rewrite(conjunction), ScoreMode.COMPLETE, 1f);
                int matches = 0;
                for (LeafReaderContext ctx : reader.leaves()) {
                    // Mirrors InnerHitsContext#intersect, which requests the scorer with a lead cost of 0
                    ScorerSupplier conjunctionScorerSupplier = conjunctionWeight.scorerSupplier(ctx);
                    if (conjunctionScorerSupplier != null) {
                        Scorer scorer = conjunctionScorerSupplier.get(0);
                        for (int doc = scorer.iterator().nextDoc(); doc != NO_MORE_DOCS; doc = scorer.iterator().nextDoc()) {
                            assertThat(scorer.score(), greaterThan(0f));
                            matches++;
                        }
                    }
                    ScorerSupplier knnScorerSupplier = knnWeight.scorerSupplier(ctx);
                    if (knnScorerSupplier != null) {
                        assertEquals(-1, knnScorerSupplier.get(0).iterator().docID());
                    }
                }
                assertEquals(expectedMatches, matches);
                assertEquals(expectedMatches, searcher.count(conjunction));
            }
        }
    }
}
