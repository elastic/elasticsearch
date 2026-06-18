/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.cache.query.TrivialQueryCachingPolicy;
import org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskBBQVectorsFormat;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static com.carrotsearch.randomizedtesting.RandomizedTest.randomFloat;
import static com.carrotsearch.randomizedtesting.RandomizedTest.rarely;
import static org.hamcrest.Matchers.equalTo;

/** Tests for {@link DiversifyingChildrenIVFKnnFloatSlicedVectorQuery}. */
public class DiversifyingChildrenIVFKnnFloatSlicedVectorQueryTests extends AbstractDiversifyingChildrenIVFKnnVectorQueryTestCase {

    private static final BytesRef SLICE_ZERO = new BytesRef("0");

    private static void addRoutingSlice(Document doc, BytesRef sliceId) {
        doc.add(SortedDocValuesField.indexedField(RoutingFieldMapper.NAME, sliceId));
    }

    private IndexWriterConfig slicedIndexWriterConfig() {
        return newIndexWriterConfig().setCodec(TestUtil.alwaysKnnVectorsFormat(format))
            .setMergePolicy(newMergePolicy(random(), false))
            .setIndexSort(new Sort(new SortField(RoutingFieldMapper.NAME, SortField.Type.STRING)))
            .setParentField(Engine.ROOT_DOC_FIELD_NAME);
    }

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        format = new ESNextDiskBBQVectorsFormat(128, 4, RoutingFieldMapper.NAME);
    }

    @Override
    Query getDiversifyingChildrenKnnQuery(String fieldName, float[] queryVector, Query childFilter, int k, BitSetProducer parentBitSet) {
        return new DiversifyingChildrenIVFKnnFloatSlicedVectorQuery(
            fieldName,
            queryVector,
            k,
            k,
            childFilter,
            parentBitSet,
            0,
            testResolver(),
            RoutingFieldMapper.NAME,
            SLICE_ZERO
        );
    }

    @Override
    Field getKnnVectorField(String name, float[] vector) {
        return new KnnFloatVectorField(name, vector);
    }

    @Override
    Directory getIndexStore(String field, float[]... contents) throws IOException {
        Directory indexStore = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(random(), indexStore, slicedIndexWriterConfig());
        for (int i = 0; i < contents.length; ++i) {
            List<Document> toAdd = new ArrayList<>();
            Document doc = new Document();
            doc.add(getKnnVectorField(field, contents[i]));
            doc.add(newStringField("id", Integer.toString(i), Field.Store.YES));
            addRoutingSlice(doc, SLICE_ZERO);
            toAdd.add(doc);
            Document parent = makeParent(new int[] { i });
            addRoutingSlice(parent, SLICE_ZERO);
            toAdd.add(parent);
            writer.addDocuments(toAdd);
        }
        for (int i = 0; i < 5; i++) {
            List<Document> toAdd = new ArrayList<>();
            Document doc = new Document();
            doc.add(new StringField("other", "value", Field.Store.NO));
            addRoutingSlice(doc, SLICE_ZERO);
            toAdd.add(doc);
            Document parent = makeParent(new int[0]);
            addRoutingSlice(parent, SLICE_ZERO);
            toAdd.add(parent);
            writer.addDocuments(toAdd);
        }
        writer.close();
        return indexStore;
    }

    @Override
    public void testIndexWithNoVectorsNorParents() throws IOException {
        try (Directory d = newDirectory()) {
            try (IndexWriter w = new IndexWriter(d, slicedIndexWriterConfig())) {
                for (int i = 0; i < 5; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("other", "value", Field.Store.NO));
                    addRoutingSlice(doc, SLICE_ZERO);
                    w.addDocument(doc);
                }
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                BitSetProducer parentFilter = new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));
                Query query = getDiversifyingChildrenKnnQuery("field", new float[] { 2, 2 }, null, 3, parentFilter);
                TopDocs topDocs = searcher.search(query, 3);
                assertEquals(0, topDocs.totalHits.value());
                assertEquals(0, topDocs.scoreDocs.length);

                query = getDiversifyingChildrenKnnQuery("field", new float[] { 2, 2 }, Queries.ALL_DOCS_INSTANCE, 10, parentFilter);
                topDocs = searcher.search(query, 3);
                assertEquals(0, topDocs.totalHits.value());
                assertEquals(0, topDocs.scoreDocs.length);
            }
        }
    }

    @Override
    public void testIndexWithNoParents() throws IOException {
        try (Directory d = newDirectory()) {
            try (IndexWriter w = new IndexWriter(d, slicedIndexWriterConfig())) {
                for (int i = 0; i < 3; ++i) {
                    Document doc = new Document();
                    doc.add(getKnnVectorField("field", new float[] { 2, 2 }));
                    doc.add(newStringField("id", Integer.toString(i), Field.Store.YES));
                    addRoutingSlice(doc, SLICE_ZERO);
                    w.addDocument(doc);
                }
                for (int i = 0; i < 5; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("other", "value", Field.Store.NO));
                    addRoutingSlice(doc, SLICE_ZERO);
                    w.addDocument(doc);
                }
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                BitSetProducer parentFilter = new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));
                Query query = getDiversifyingChildrenKnnQuery("field", new float[] { 2, 2 }, null, 3, parentFilter);
                TopDocs topDocs = searcher.search(query, 3);
                assertEquals(0, topDocs.totalHits.value());
                assertEquals(0, topDocs.scoreDocs.length);

                query = getDiversifyingChildrenKnnQuery("field", new float[] { 2, 2 }, Queries.ALL_DOCS_INSTANCE, 10, parentFilter);
                topDocs = searcher.search(query, 3);
                assertEquals(0, topDocs.totalHits.value());
                assertEquals(0, topDocs.scoreDocs.length);
            }
        }
    }

    @Override
    public void testScoringWithMultipleChildren() throws IOException {
        try (Directory d = newDirectory()) {
            try (IndexWriter w = new IndexWriter(d, slicedIndexWriterConfig())) {
                List<Document> toAdd = new ArrayList<>();
                for (int j = 1; j <= 5; j++) {
                    Document doc = new Document();
                    doc.add(getKnnVectorField("field", new float[] { j, j }));
                    doc.add(newStringField("id", Integer.toString(j), Field.Store.YES));
                    addRoutingSlice(doc, SLICE_ZERO);
                    toAdd.add(doc);
                }
                Document parent1 = makeParent(new int[] { 1, 2, 3, 4, 5 });
                addRoutingSlice(parent1, SLICE_ZERO);
                toAdd.add(parent1);
                w.addDocuments(toAdd);

                toAdd = new ArrayList<>();
                for (int j = 7; j <= 11; j++) {
                    Document doc = new Document();
                    doc.add(getKnnVectorField("field", new float[] { j, j }));
                    doc.add(newStringField("id", Integer.toString(j), Field.Store.YES));
                    addRoutingSlice(doc, SLICE_ZERO);
                    toAdd.add(doc);
                }
                Document parent2 = makeParent(new int[] { 6, 7, 8, 9, 10 });
                addRoutingSlice(parent2, SLICE_ZERO);
                toAdd.add(parent2);
                w.addDocuments(toAdd);
                w.forceMerge(1);
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                assertEquals(1, reader.leaves().size());
                IndexSearcher searcher = new IndexSearcher(reader);
                BitSetProducer parentFilter = parentFilter(searcher.getIndexReader());
                Query query = getDiversifyingChildrenKnnQuery("field", new float[] { 2, 2 }, null, 3, parentFilter);
                assertScorerResults(searcher, query, new float[] { 1f, 1f / 51f }, new String[] { "2", "7" }, 2, 0.001f);

                query = getDiversifyingChildrenKnnQuery("field", new float[] { 6, 6 }, null, 3, parentFilter);
                assertScorerResults(searcher, query, new float[] { 1f / 3f, 1f / 3f }, new String[] { "5", "7" }, 2, 0.001f);
                query = getDiversifyingChildrenKnnQuery("field", new float[] { 6, 6 }, Queries.ALL_DOCS_INSTANCE, 20, parentFilter);
                assertScorerResults(searcher, query, new float[] { 1f / 3f, 1f / 3f }, new String[] { "5", "7" }, 2, 0.001f);

                query = getDiversifyingChildrenKnnQuery("field", new float[] { 6, 6 }, Queries.ALL_DOCS_INSTANCE, 1, parentFilter);
                assertScorerResults(searcher, query, new float[] { 1f / 3f, 1f / 3f }, new String[] { "5", "7" }, 1, 0.001f);
            }
        }
    }

    @Override
    public void testSkewedIndex() throws IOException {
        try (Directory d = newDirectory()) {
            try (IndexWriter w = new IndexWriter(d, slicedIndexWriterConfig())) {
                int r = 0;
                for (int i = 0; i < 5; i++) {
                    for (int j = 0; j < 5; j++) {
                        List<Document> toAdd = new ArrayList<>();
                        Document doc = new Document();
                        doc.add(getKnnVectorField("field", new float[] { r, r }));
                        doc.add(newStringField("id", Integer.toString(r), Field.Store.YES));
                        addRoutingSlice(doc, SLICE_ZERO);
                        toAdd.add(doc);
                        Document parent = makeParent(new int[] { r });
                        addRoutingSlice(parent, SLICE_ZERO);
                        toAdd.add(parent);
                        w.addDocuments(toAdd);
                        ++r;
                    }
                    w.flush();
                }
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                IndexSearcher searcher = newSearcher(reader);
                TopDocs results = searcher.search(
                    getDiversifyingChildrenKnnQuery("field", new float[] { 0, 0 }, null, 8, parentFilter(searcher.getIndexReader())),
                    10
                );
                assertEquals(8, results.scoreDocs.length);
                assertIdMatches(reader, "0", results.scoreDocs[0].doc);
                assertIdMatches(reader, "7", results.scoreDocs[7].doc);

                results = searcher.search(
                    getDiversifyingChildrenKnnQuery("field", new float[] { 10, 10 }, null, 8, parentFilter(searcher.getIndexReader())),
                    10
                );
                assertEquals(8, results.scoreDocs.length);
                assertIdMatches(reader, "10", results.scoreDocs[0].doc);
                assertIdMatches(reader, "6", results.scoreDocs[7].doc);
            }
        }
    }

    public void testSlicesDense() throws IOException {
        doTestSlicesDense(false);
    }

    public void testSlicesDenseWithFilter() throws IOException {
        doTestSlicesDense(true);
    }

    public void testSlicesSparse() throws IOException {
        doTestSlicesSparse(false);
    }

    public void testSlicesSparseWithFilter() throws IOException {
        doTestSlicesSparse(true);
    }

    private void doTestSlicesSparse(boolean applyFilter) throws IOException {
        if (rarely()) {
            doTestSlices(() -> random().nextInt(1000) == 0, applyFilter);
        } else {
            int bound = random().nextInt(2, 50);
            doTestSlices(() -> random().nextInt(bound) == 0, applyFilter);
        }
    }

    private void doTestSlicesDense(boolean applyFilter) throws IOException {
        doTestSlices(() -> true, applyFilter);
    }

    private void doTestSlices(BooleanSupplier supplier, boolean applyFilter) throws IOException {
        int dimensions = random().nextInt(12, 500);
        int numParents = random().nextInt(100, 10_000);
        int numSlices = random().nextInt(1, numParents);
        // Diversified kNN returns at most one hit per parent; these count qualifying parents per slice.
        int[] parentsWithHitPerSlice = new int[numSlices];
        int[] parentsWithHitPerSliceFiltered = new int[numSlices];
        int[] blockSlice = new int[numParents];
        boolean[] blockCountsUnfiltered = new boolean[numParents];
        boolean[] blockCountsFiltered = new boolean[numParents];
        String filterField = "_filter";
        String filterValue = "match";
        String filterMiss = "miss";
        String docIdField = "_doc_id";
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexSort(new Sort(new SortField(RoutingFieldMapper.NAME, SortField.Type.STRING)));
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(format));
        iwc.setMergePolicy(newMergePolicy(random(), false));
        iwc.setParentField(Engine.ROOT_DOC_FIELD_NAME);

        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < numParents; i++) {
                int slice = random().nextInt(numSlices);
                BytesRef sliceRef = new BytesRef("" + slice);
                List<Document> block = new ArrayList<>();

                int numChildren = random().nextInt(1, 5);

                boolean blockHasQualifyingChild = false;
                boolean blockHasQualifyingChildFiltered = false;
                int[] childKeys = new int[numChildren];
                for (int c = 0; c < numChildren; c++) {
                    childKeys[c] = i * 32 + c;
                }

                for (int c = 0; c < numChildren; c++) {
                    Document child = new Document();
                    addRoutingSlice(child, sliceRef);
                    boolean filterMatch = random().nextBoolean();
                    String filterText = filterMatch ? filterValue : filterMiss;
                    child.add(new StringField(filterField, filterText, Field.Store.NO));
                    child.add(new StoredField(filterField, new BytesRef(filterText)));
                    child.add(new StringField(docIdField, "doc_" + i, Field.Store.NO));
                    boolean hasVector = supplier.getAsBoolean();
                    if (hasVector) {
                        child.add(
                            new KnnFloatVectorField("vector", randomDenseQueryVector(dimensions), VectorSimilarityFunction.EUCLIDEAN)
                        );
                        blockHasQualifyingChild = true;
                        if (filterMatch) {
                            blockHasQualifyingChildFiltered = true;
                        }
                    }
                    child.add(new StoredField(RoutingFieldMapper.NAME, sliceRef));
                    block.add(child);
                }
                if (blockHasQualifyingChild) {
                    parentsWithHitPerSlice[slice]++;
                }
                if (blockHasQualifyingChildFiltered) {
                    parentsWithHitPerSliceFiltered[slice]++;
                }
                blockSlice[i] = slice;
                blockCountsUnfiltered[i] = blockHasQualifyingChild;
                blockCountsFiltered[i] = blockHasQualifyingChildFiltered;

                Document parent = makeParent(childKeys);
                addRoutingSlice(parent, sliceRef);
                parent.add(new StringField(docIdField, "doc_" + i, Field.Store.NO));
                block.add(parent);

                w.addDocuments(block);
            }
            w.commit();
            if (random().nextBoolean()) {
                int deleteCount = random().nextInt(0, Math.max(1, numParents / 10));
                Set<Integer> parentsToDelete = new HashSet<>();
                while (parentsToDelete.size() < deleteCount) {
                    parentsToDelete.add(random().nextInt(numParents));
                }
                for (int blockId : parentsToDelete) {
                    if (blockCountsUnfiltered[blockId]) {
                        parentsWithHitPerSlice[blockSlice[blockId]]--;
                    }
                    if (blockCountsFiltered[blockId]) {
                        parentsWithHitPerSliceFiltered[blockSlice[blockId]]--;
                    }
                    w.deleteDocuments(new Term(docIdField, "doc_" + blockId));
                }
                if (parentsToDelete.isEmpty() == false) {
                    w.commit();
                }
            } else if (random().nextBoolean()) {
                w.forceMerge(1);
            }
            float[] vector = randomDenseQueryVector(dimensions);
            try (IndexReader reader = DirectoryReader.open(w)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                searcher.setQueryCachingPolicy(TrivialQueryCachingPolicy.ALWAYS);
                BitSetProducer parents = parentFilter(reader);
                Query filterQuery = null;
                if (applyFilter) {
                    filterQuery = new TermQuery(new Term(filterField, filterValue));
                }
                for (int iters = 0; iters < 2; iters++) {
                    // single slice
                    for (int slice = 0; slice < numSlices; slice++) {
                        int expectedDocs = applyFilter ? parentsWithHitPerSliceFiltered[slice] : parentsWithHitPerSlice[slice];
                        TopDocs topDocs = getTopDocs(expectedDocs, vector, filterQuery, parents, searcher, slice);
                        assertTopDocs(applyFilter, expectedDocs, topDocs, reader, filterField, filterValue, slice);
                    }
                    // multiple slice
                    for (int i = 0; i < 10; i++) {
                        int numQuerySlices = random().nextInt(numSlices) + 1;
                        int[] querySlices = new int[numQuerySlices];
                        int expectedDocs = 0;
                        int prevSlice = 0;
                        for (int j = 0; j < numQuerySlices; j++) {
                            int slice = random().nextInt(prevSlice, numSlices - numQuerySlices + j + 1);
                            querySlices[j] = slice;
                            expectedDocs += applyFilter ? parentsWithHitPerSliceFiltered[slice] : parentsWithHitPerSlice[slice];
                            prevSlice = slice + 1;
                        }
                        Arrays.sort(querySlices);
                        TopDocs topDocs = getTopDocs(expectedDocs, vector, filterQuery, parents, searcher, querySlices);
                        assertTopDocs(applyFilter, expectedDocs, topDocs, reader, filterField, filterValue, querySlices);
                    }
                    // non-existing slice
                    TopDocs topDocs = getTopDocs(0, vector, filterQuery, parents, searcher, -1);
                    assertEquals(0, topDocs.scoreDocs.length);
                }
            }
        }
    }

    private TopDocs getTopDocs(
        int expectedDocs,
        float[] vector,
        Query filterQuery,
        BitSetProducer parents,
        IndexSearcher searcher,
        int... slice
    ) throws IOException {
        BytesRef[] sliceRef = new BytesRef[slice.length];
        for (int i = 0; i < slice.length; i++) {
            sliceRef[i] = new BytesRef("" + slice[i]);
        }
        int k = 2 * Math.max(1, expectedDocs);
        Query kvq = new DiversifyingChildrenIVFKnnFloatSlicedVectorQuery(
            "vector",
            vector,
            k,
            k,
            filterQuery,
            parents,
            1.0f,
            testResolver(),
            RoutingFieldMapper.NAME,
            sliceRef
        );
        return searcher.search(kvq, k);
    }

    private static void assertTopDocs(
        boolean applyFilter,
        int expectedDocs,
        TopDocs topDocs,
        IndexReader reader,
        String filterField,
        String filterValue,
        int... slices
    ) throws IOException {
        assertEquals(expectedDocs, topDocs.scoreDocs.length);
        for (int j = 0; j < topDocs.scoreDocs.length; j++) {
            Document document = reader.storedFields().document(topDocs.scoreDocs[j].doc);
            int docSlice = Integer.parseInt(document.getField(RoutingFieldMapper.NAME).binaryValue().utf8ToString());
            assertTrue(Arrays.stream(slices).anyMatch(s -> s == docSlice));
            if (applyFilter) {
                assertThat(document.getField(filterField).binaryValue().utf8ToString(), equalTo(filterValue));
            }
        }
    }

    private float[] randomDenseQueryVector(int dim) {
        float[] vec = new float[dim];
        for (int i = 0; i < dim; i++) {
            vec[i] = randomFloat();
        }
        VectorUtil.l2normalize(vec);
        return vec;
    }

    public void testToString() {
        DiversifyingChildrenIVFKnnFloatSlicedVectorQuery q = new DiversifyingChildrenIVFKnnFloatSlicedVectorQuery(
            "vec",
            new float[] { 0.5f, 0.5f },
            4,
            4,
            null,
            parent -> null,
            0.1f,
            testResolver(),
            RoutingFieldMapper.NAME,
            SLICE_ZERO
        );
        assertEquals(
            "DiversifyingChildrenIVFKnnFloatSlicedVectorQuery:vec[0.5,...][4][" + RoutingFieldMapper.NAME + "=[0]]",
            q.toString("ignored")
        );
    }
}
