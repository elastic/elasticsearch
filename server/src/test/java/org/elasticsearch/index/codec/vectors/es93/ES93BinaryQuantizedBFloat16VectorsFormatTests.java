/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.es93;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SoftDeletesRetentionMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
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
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.common.logging.LogConfigurator;
import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.index.codec.vectors.BaseBFloat16KnnVectorsFormatTestCase;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.junit.AssumptionViolatedException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

import static java.lang.String.format;
import static org.apache.lucene.index.VectorSimilarityFunction.DOT_PRODUCT;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.oneOf;

public class ES93BinaryQuantizedBFloat16VectorsFormatTests extends BaseBFloat16KnnVectorsFormatTestCase {

    static {
        LogConfigurator.loadLog4jPlugins();
        LogConfigurator.configureESLogging(); // native access requires logging to be initialized
    }

    private KnnVectorsFormat format;

    @Override
    public void setUp() throws Exception {
        format = new ES93BinaryQuantizedVectorsFormat(DenseVectorFieldMapper.ElementType.BFLOAT16, random().nextBoolean());
        super.setUp();
    }

    @Override
    protected Codec getCodec() {
        return TestUtil.alwaysKnnVectorsFormat(format);
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
            IndexWriterConfig iwc = newIndexWriterConfig().setCodec(getCodec());
            iwc.setMergePolicy(new SoftDeletesRetentionMergePolicy("soft_delete", MatchAllDocsQuery::new, iwc.getMergePolicy()));
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
                    assertThat(searcher.search(query, 1).scoreDocs, emptyArray());
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
        String expected = "ES93BinaryQuantizedVectorsFormat(name=ES93BinaryQuantizedVectorsFormat, rawVectorFormat=%s, scorer=%s)";
        expected = format(
            Locale.ROOT,
            expected,
            "ES93GenericFlatVectorsFormat(name=ES93GenericFlatVectorsFormat, format=%s)",
            "ES818BinaryFlatVectorsScorer(nonQuantizedDelegate={}())"
        );
        expected = format(
            Locale.ROOT,
            expected,
            "ES93BFloat16FlatVectorsFormat(name=ES93BFloat16FlatVectorsFormat, flatVectorScorer={}())"
        );

        var defaultScorer = expected.replaceAll("\\{}", "DefaultFlatVectorScorer");
        var memSegScorer = expected.replaceAll("\\{}", "Lucene99MemorySegmentFlatVectorsScorer");

        KnnVectorsFormat format = new ES93BinaryQuantizedVectorsFormat(DenseVectorFieldMapper.ElementType.BFLOAT16, false);
        assertThat(format, hasToString(oneOf(defaultScorer, memSegScorer)));
    }

    @Override
    public void testRandomWithUpdatesAndGraph() {
        throw new AssumptionViolatedException("Graph not supported");
    }

    @Override
    public void testSearchWithVisitedLimit() {
        throw new AssumptionViolatedException("visited limit not respected");
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
                    if (expectVecOffHeap) {
                        assertThat(offHeap, aMapWithSize(2));
                        assertThat(offHeap, hasEntry(equalTo("veb"), greaterThan(0L)));
                        assertThat(offHeap, hasEntry("vec", (long) vector.length * BFloat16.BYTES));
                    } else {
                        assertThat(offHeap, aMapWithSize(1));
                        assertThat(offHeap, hasEntry(equalTo("veb"), greaterThan(0L)));
                    }
                }
            }
        }
    }

    static Directory newMMapDirectory() throws IOException {
        Directory dir = new MMapDirectory(createTempDir("ES93BinaryQuantizedBFloat16VectorsFormatTests"));
        if (random().nextBoolean()) {
            dir = new MockDirectoryWrapper(random(), dir);
        }
        return dir;
    }
}
