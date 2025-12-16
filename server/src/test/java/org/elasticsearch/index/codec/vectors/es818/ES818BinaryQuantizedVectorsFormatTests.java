/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2024 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.vectors.es818;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.CheckJoinIndex;
import org.apache.lucene.search.join.DiversifyingChildrenFloatKnnVectorQuery;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.tests.index.BaseKnnVectorsFormatTestCase;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static java.lang.String.format;
import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class ES818BinaryQuantizedVectorsFormatTests extends BaseKnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    static final Codec codec = TestUtil.alwaysKnnVectorsFormat(new ES818BinaryQuantizedVectorsFormat());

    @Override
    protected Codec getCodec() {
        return codec;
    }

    @Override
    protected VectorSimilarityFunction randomSimilarity() {
        return RandomPicks.randomFrom(
            random(),
            List.of(
                VectorSimilarityFunction.DOT_PRODUCT,
                VectorSimilarityFunction.EUCLIDEAN,
                VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT
            )
        );
    }

    static String encodeInts(int[] i) {
        return Arrays.toString(i);
    }

    static BitSetProducer parentFilter(IndexReader r) throws IOException {
        // Create a filter that defines "parent" documents in the index
        BitSetProducer parentsFilter = new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));
        CheckJoinIndex.check(r, parentsFilter);
        return parentsFilter;
    }

    Document makeParent(int[] children) {
        Document parent = new Document();
        parent.add(newStringField("docType", "_parent", Field.Store.NO));
        parent.add(newStringField("id", encodeInts(children), Field.Store.YES));
        return parent;
    }

    public void testEmptyDiversifiedChildSearch() throws Exception {
        String fieldName = "field";
        int dims = random().nextInt(4, 65);
        float[] vector = randomVector(dims);
        VectorSimilarityFunction similarityFunction = VectorSimilarityFunction.EUCLIDEAN;
        try (Directory d = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig().setCodec(codec);
            iwc.setMergePolicy(new SoftDeletesRetentionMergePolicy("soft_delete", () -> Queries.ALL_DOCS_INSTANCE, iwc.getMergePolicy()));
            try (IndexWriter w = new IndexWriter(d, iwc)) {
                List<Document> toAdd = new ArrayList<>();
                for (int j = 1; j <= 5; j++) {
                    Document doc = new Document();
                    doc.add(new KnnFloatVectorField(fieldName, vector, similarityFunction));
                    doc.add(newStringField("id", Integer.toString(j), Field.Store.YES));
                    toAdd.add(doc);
                }
                toAdd.add(makeParent(new int[] { 1, 2, 3, 4, 5 }));
                w.addDocuments(toAdd);
                w.addDocuments(List.of(makeParent(new int[] { 6, 7, 8, 9, 10 })));
                w.deleteDocuments(new FieldExistsQuery(fieldName), new TermQuery(new Term("id", encodeInts(new int[] { 1, 2, 3, 4, 5 }))));
                w.flush();
                w.commit();
                w.forceMerge(1);
                try (IndexReader reader = DirectoryReader.open(w)) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    BitSetProducer parentFilter = parentFilter(searcher.getIndexReader());
                    Query query = new DiversifyingChildrenFloatKnnVectorQuery(fieldName, vector, null, 1, parentFilter);
                    assertTrue(searcher.search(query, 1).scoreDocs.length == 0);
                }
            }

        }
    }

    public void testSearch() throws Exception {
        String fieldName = "field";
        int numVectors = random().nextInt(99, 500);
        int dims = random().nextInt(4, 65);
        float[] vector = randomVector(dims);
        VectorSimilarityFunction similarityFunction = randomSimilarity();
        KnnFloatVectorField knnField = new KnnFloatVectorField(fieldName, vector, similarityFunction);
        IndexWriterConfig iwc = newIndexWriterConfig();
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < numVectors; i++) {
                    Document doc = new Document();
                    knnField.setVectorValue(randomVector(dims));
                    doc.add(knnField);
                    w.addDocument(doc);
                }
                w.commit();

                try (IndexReader reader = DirectoryReader.open(w)) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    final int k = random().nextInt(5, 50);
                    float[] queryVector = randomVector(dims);
                    Query q = new KnnFloatVectorQuery(fieldName, queryVector, k);
                    TopDocs collectedDocs = searcher.search(q, k);
                    assertEquals(k, collectedDocs.totalHits.value());
                    assertEquals(TotalHits.Relation.EQUAL_TO, collectedDocs.totalHits.relation());
                }
            }
        }
    }

    public void testToString() {
        FilterCodec customCodec = new FilterCodec("foo", Codec.getDefault()) {
            @Override
            public KnnVectorsFormat knnVectorsFormat() {
                return new ES818BinaryQuantizedVectorsFormat();
            }
        };
        String expectedPattern = "ES818BinaryQuantizedVectorsFormat("
            + "name=ES818BinaryQuantizedVectorsFormat, "
            + "flatVectorScorer=ES818BinaryFlatVectorsScorer(nonQuantizedDelegate=%s()))";
        var defaultScorer = format(Locale.ROOT, expectedPattern, "DefaultFlatVectorScorer");
        var memSegScorer = format(Locale.ROOT, expectedPattern, "Lucene99MemorySegmentFlatVectorsScorer");
        assertThat(customCodec.knnVectorsFormat().toString(), is(oneOf(defaultScorer, memSegScorer)));
    }

    @Override
    public void testRandomWithUpdatesAndGraph() {
        // graph not supported
    }

    @Override
    public void testSearchWithVisitedLimit() {
        // visited limit is not respected, as it is brute force search
    }

    public void testQuantizedVectorsWriteAndRead() throws IOException {
        String fieldName = "field";
        int numVectors = random().nextInt(99, 500);
        int dims = random().nextInt(4, 65);

        float[] vector = randomVector(dims);
        VectorSimilarityFunction similarityFunction = randomSimilarity();
        KnnFloatVectorField knnField = new KnnFloatVectorField(fieldName, vector, similarityFunction);
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, newIndexWriterConfig())) {
                for (int i = 0; i < numVectors; i++) {
                    Document doc = new Document();
                    knnField.setVectorValue(randomVector(dims));
                    doc.add(knnField);
                    w.addDocument(doc);
                    if (i % 101 == 0) {
                        w.commit();
                    }
                }
                w.commit();
                w.forceMerge(1);

                try (IndexReader reader = DirectoryReader.open(w)) {
                    LeafReader r = getOnlyLeafReader(reader);
                    FloatVectorValues vectorValues = r.getFloatVectorValues(fieldName);
                    assertEquals(vectorValues.size(), numVectors);
                    BinarizedByteVectorValues qvectorValues = ((ES818BinaryQuantizedVectorsReader.BinarizedVectorValues) vectorValues)
                        .getQuantizedVectorValues();
                    float[] centroid = qvectorValues.getCentroid();
                    assertEquals(centroid.length, dims);

                    OptimizedScalarQuantizer quantizer = new OptimizedScalarQuantizer(similarityFunction);
                    int[] quantizedVector = new int[dims];
                    byte[] expectedVector = new byte[BQVectorUtils.discretize(dims, 64) / 8];
                    if (similarityFunction == VectorSimilarityFunction.COSINE) {
                        vectorValues = new ES818BinaryQuantizedVectorsWriter.NormalizedFloatVectorValues(vectorValues);
                    }
                    KnnVectorValues.DocIndexIterator docIndexIterator = vectorValues.iterator();

                    float[] scratch = new float[dims];
                    while (docIndexIterator.nextDoc() != NO_MORE_DOCS) {
                        OptimizedScalarQuantizer.QuantizationResult corrections = quantizer.scalarQuantize(
                            vectorValues.vectorValue(docIndexIterator.index()),
                            scratch,
                            quantizedVector,
                            (byte) 1,
                            centroid
                        );
                        ESVectorUtil.packAsBinary(quantizedVector, expectedVector);
                        assertArrayEquals(expectedVector, qvectorValues.vectorValue(docIndexIterator.index()));
                        assertEquals(corrections, qvectorValues.getCorrectiveTerms(docIndexIterator.index()));
                    }
                }
            }
        }
    }

    public void testSimpleOffHeapSize() throws IOException {
        try (Directory dir = newDirectory()) {
            testSimpleOffHeapSizeImpl(dir, newIndexWriterConfig(), true);
        }
    }

    public void testSimpleOffHeapSizeMMapDir() throws IOException {
        try (Directory dir = newMMapDirectory()) {
            testSimpleOffHeapSizeImpl(dir, newIndexWriterConfig(), true);
        }
    }

    public void testSimpleOffHeapSizeImpl(Directory dir, IndexWriterConfig config, boolean expectVecOffHeap) throws IOException {
        float[] vector = randomVector(random().nextInt(12, 500));
        try (IndexWriter w = new IndexWriter(dir, config)) {
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
                    assertEquals(expectVecOffHeap ? 2 : 1, offHeap.size());
                    assertTrue(offHeap.get("veb") > 0L);
                    if (expectVecOffHeap) {
                        assertEquals(vector.length * Float.BYTES, (long) offHeap.get("vec"));
                    }
                }
            }
        }
    }

    static Directory newMMapDirectory() throws IOException {
        Directory dir = new MMapDirectory(createTempDir("ES818BinaryQuantizedVectorsFormatTests"));
        if (random().nextBoolean()) {
            dir = new MockDirectoryWrapper(random(), dir);
        }
        return dir;
    }
}
