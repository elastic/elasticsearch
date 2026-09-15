/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.next;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.SortedDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.codec.vectors.diskbbq.IVFVectorsReader;
import org.elasticsearch.search.vectors.ESAcceptDocs;
import org.elasticsearch.search.vectors.ESAcceptDocs.SliceAcceptDocs;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BooleanSupplier;

import static org.elasticsearch.index.codec.vectors.diskbbq.next.ESNextDiskASHVectorsFormat.MIN_VECTORS_PER_CLUSTER;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Tests for {@link ESNextDiskASHVectorsFormat}.
 */
public class ESNextDiskASHVectorsFormatTests extends ESTestCase {

    public void testAshIndexAndSearch() throws IOException {
        int dimensions = 64;
        int numDocs = 200;
        Codec ashCodec = TestUtil.alwaysKnnVectorsFormat(ashTestFormat());
        try (Directory dir = newDirectory()) {
            IndexWriterConfig iwc = newIndexWriterConfig();
            iwc.setCodec(ashCodec);
            try (IndexWriter w = new IndexWriter(dir, iwc)) {
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    doc.add(new KnnFloatVectorField("f", randomVector(dimensions), VectorSimilarityFunction.DOT_PRODUCT));
                    w.addDocument(doc);
                }
                w.forceMerge(1);
                try (IndexReader reader = DirectoryReader.open(w)) {
                    for (LeafReaderContext ctx : reader.leaves()) {
                        LeafReader leafReader = ctx.reader();
                        float[] query = randomVector(dimensions);
                        TopDocs topDocs = leafReader.searchNearestVectors(
                            "f",
                            query,
                            10,
                            AcceptDocs.fromLiveDocs(leafReader.getLiveDocs(), leafReader.maxDoc()),
                            Integer.MAX_VALUE
                        );
                        assertThat(topDocs.scoreDocs, arrayWithSize(Math.min(leafReader.maxDoc(), 10)));
                        for (int i = 0; i < topDocs.scoreDocs.length - 1; i++) {
                            assertThat(
                                "Scores should be descending",
                                topDocs.scoreDocs[i].score,
                                greaterThanOrEqualTo(topDocs.scoreDocs[i + 1].score)
                            );
                        }
                    }
                }
            }
        }
    }

    public void testAshAllSimilarityFunctions() throws IOException {
        int dimensions = 64;
        int numDocs = 200;
        Codec ashCodec = TestUtil.alwaysKnnVectorsFormat(ashTestFormat());
        for (VectorSimilarityFunction sim : new VectorSimilarityFunction[] {
            VectorSimilarityFunction.DOT_PRODUCT,
            VectorSimilarityFunction.EUCLIDEAN,
            VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT }) {
            try (Directory dir = newDirectory()) {
                IndexWriterConfig iwc = newIndexWriterConfig();
                iwc.setCodec(ashCodec);
                try (IndexWriter w = new IndexWriter(dir, iwc)) {
                    for (int i = 0; i < numDocs; i++) {
                        Document doc = new Document();
                        doc.add(new KnnFloatVectorField("f", randomVector(dimensions), sim));
                        w.addDocument(doc);
                    }
                    w.forceMerge(1);
                    try (IndexReader reader = DirectoryReader.open(w)) {
                        for (LeafReaderContext ctx : reader.leaves()) {
                            LeafReader leafReader = ctx.reader();
                            float[] query = randomVector(dimensions);
                            TopDocs topDocs = leafReader.searchNearestVectors(
                                "f",
                                query,
                                10,
                                AcceptDocs.fromLiveDocs(leafReader.getLiveDocs(), leafReader.maxDoc()),
                                Integer.MAX_VALUE
                            );
                            assertThat("similarity=" + sim, topDocs.scoreDocs, arrayWithSize(Math.min(leafReader.maxDoc(), 10)));
                            for (int i = 0; i < topDocs.scoreDocs.length - 1; i++) {
                                assertThat(
                                    "Scores should be descending for " + sim,
                                    topDocs.scoreDocs[i].score,
                                    greaterThanOrEqualTo(topDocs.scoreDocs[i + 1].score)
                                );
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * A sliced segment produced by a single flush is not clustered per slice ({@code numSlices == 0}) and is
     * searched over a doc id range. Plain Lucene {@link AcceptDocs} (as used by {@code CheckIndex}) carry no slice
     * information and must fall back to the whole segment, while an {@link ESAcceptDocs} without a slice ordinal
     * violates the reader contract and is rejected by assertion rather than silently searching a wrong range.
     */
    public void testSlicedFlushedSegmentWithoutSliceOrdinal() throws IOException {
        String sliceField = "_slice";
        String vectorField = "vector";
        int numDocs = random().nextInt(10, 200);
        int dimensions = random().nextInt(12, 128);
        ESNextDiskASHVectorsFormat localFormat = ashSlicedFormat(sliceField);
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexSort(new Sort(new SortField(sliceField, SortField.Type.STRING)));
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(localFormat));
        iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                doc.add(SortedDocValuesField.indexedField(sliceField, new BytesRef("" + random().nextInt(5))));
                doc.add(new KnnFloatVectorField(vectorField, randomVector(dimensions), VectorSimilarityFunction.EUCLIDEAN));
                w.addDocument(doc);
            }
            w.commit();
            try (IndexReader reader = DirectoryReader.open(w)) {
                for (LeafReaderContext context : reader.leaves()) {
                    LeafReader leafReader = context.reader();
                    KnnVectorsReader vectorReader = ((CodecReader) leafReader).getVectorReader();
                    if (vectorReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
                        vectorReader = fieldsReader.getFieldReader(vectorField);
                    }
                    assertThat(vectorReader, instanceOf(ESNextDiskASHVectorsReader.class));
                    // a flushed sliced segment is written as a single flat posting list
                    try (
                        IVFVectorsReader.CentroidData<?> centroidData = ((ESNextDiskASHVectorsReader) vectorReader).readCentroidData(
                            vectorField
                        )
                    ) {
                        assertThat(centroidData, notNullValue());
                        assertThat(centroidData.numCentroids(), equalTo(1));
                    }
                    float[] vector = randomVector(dimensions);
                    KnnCollector collector = new TopKnnCollector(leafReader.maxDoc(), Integer.MAX_VALUE);
                    leafReader.searchNearestVectors(vectorField, vector, collector, AcceptDocs.fromLiveDocs(null, leafReader.maxDoc()));
                    Set<Integer> docIds = new HashSet<>();
                    for (ScoreDoc scoreDoc : collector.topDocs().scoreDocs) {
                        docIds.add(scoreDoc.doc);
                    }
                    assertThat(docIds, hasSize(leafReader.maxDoc()));

                    // Call getPostingVisitor directly via a package-private test helper, bypassing the
                    // assertion in getNumberOfVectors, to test the fix in the numSlices==0 branch itself.
                    ESNextDiskASHVectorsReader ashReader = (ESNextDiskASHVectorsReader) vectorReader;
                    AssertionError error = expectThrows(
                        AssertionError.class,
                        () -> ashReader.getPostingVisitorForTest(vectorField, vector, new ESAcceptDocs.ESAcceptDocsAll())
                    );
                    assertThat(error.getMessage(), equalTo("sliced segment searched without a slice ordinal"));
                }
            }
        }
    }

    public void testSlicedIndexOneVectorPerSlice() throws IOException {
        String sliceField = "_slice";
        String vectorField = "vector";
        int slices = random().nextInt(2, 100);
        int dimensions = random().nextInt(12, 128);
        ESNextDiskASHVectorsFormat localFormat = ashSlicedFormat(sliceField);
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexSort(new Sort(new SortField(sliceField, SortField.Type.STRING)));
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(localFormat));
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int slice = 0; slice < slices; slice++) {
                Document doc = new Document();
                doc.add(SortedDocValuesField.indexedField(sliceField, new BytesRef("" + slice)));
                doc.add(new KnnFloatVectorField(vectorField, randomVector(dimensions), VectorSimilarityFunction.EUCLIDEAN));
                w.addDocument(doc);
            }
            w.commit();
            w.forceMerge(1);
            try (IndexReader reader = DirectoryReader.open(w)) {
                assertEquals(1, reader.leaves().size());
                LeafReader leafReader = reader.leaves().get(0).reader();
                KnnVectorsReader vectorReader = ((CodecReader) leafReader).getVectorReader();
                if (vectorReader instanceof PerFieldKnnVectorsFormat.FieldsReader fieldsReader) {
                    vectorReader = fieldsReader.getFieldReader(vectorField);
                }
                assertThat(vectorReader, instanceOf(ESNextDiskASHVectorsReader.class));
                try (
                    IVFVectorsReader.CentroidData<?> centroidData = ((ESNextDiskASHVectorsReader) vectorReader).readCentroidData(
                        vectorField
                    )
                ) {
                    assertNotNull(centroidData);
                    assertThat(centroidData.numCentroids(), equalTo(1));
                    assertThat(centroidData.centroids().size(), equalTo(1));
                }
            }
        }
    }

    public void testSlicesDense() throws IOException {
        doTestSlices(() -> true, false);
    }

    public void testSlicesDenseWithFilter() throws IOException {
        doTestSlices(() -> true, true);
    }

    public void testSlicesSparse() throws IOException {
        int bound = random().nextInt(2, 50);
        doTestSlices(() -> random().nextInt(bound) == 0, false);
    }

    public void testSlicesSparseWithFilter() throws IOException {
        int bound = random().nextInt(2, 50);
        doTestSlices(() -> random().nextInt(bound) == 0, true);
    }

    private void doTestSlices(BooleanSupplier hasVectorSupplier, boolean applyFilter) throws IOException {
        String sliceField = "_slice";
        String filterField = "_filter";
        String filterValue = "match";
        String filterMiss = "miss";
        String docIdField = "_doc_id";
        String vectorField = "vector";
        ESNextDiskASHVectorsFormat localFormat = ashSlicedFormat(sliceField);
        int dimensions = random().nextInt(12, 128);
        int slices = random().nextInt(2, 50);
        int numDocs = random().nextInt(100, 5000);
        int[] docsPerSlice = new int[slices];
        int[] docsPerSliceFiltered = new int[slices];
        int[] docSlices = new int[numDocs];
        boolean[] docHasVector = new boolean[numDocs];
        boolean[] docFilterMatch = new boolean[numDocs];
        IndexWriterConfig iwc = newIndexWriterConfig();
        iwc.setIndexSort(new Sort(new SortField(sliceField, SortField.Type.STRING)));
        iwc.setCodec(TestUtil.alwaysKnnVectorsFormat(localFormat));
        try (Directory dir = newDirectory(); IndexWriter w = new IndexWriter(dir, iwc)) {
            for (int i = 0; i < numDocs; i++) {
                int slice = random().nextInt(slices);
                Document doc = new Document();
                doc.add(SortedDocValuesField.indexedField(sliceField, new BytesRef("" + slice)));
                boolean filterMatch = random().nextBoolean();
                String filterText = filterMatch ? filterValue : filterMiss;
                doc.add(new StringField(filterField, filterText, Field.Store.NO));
                doc.add(new StoredField(filterField, new BytesRef(filterText)));
                doc.add(new StringField(docIdField, "doc_" + i, Field.Store.NO));
                boolean hasVector = hasVectorSupplier.getAsBoolean();
                if (hasVector) {
                    docsPerSlice[slice]++;
                    if (filterMatch) {
                        docsPerSliceFiltered[slice]++;
                    }
                    doc.add(new KnnFloatVectorField(vectorField, randomVector(dimensions), VectorSimilarityFunction.EUCLIDEAN));
                }
                doc.add(new StoredField(sliceField, new BytesRef("" + slice)));
                w.addDocument(doc);
                docSlices[i] = slice;
                docHasVector[i] = hasVector;
                docFilterMatch[i] = filterMatch;
            }
            w.commit();
            if (random().nextBoolean()) {
                int deleteCount = random().nextInt(0, Math.max(1, numDocs / 10));
                Set<Integer> docsToDelete = new HashSet<>();
                while (docsToDelete.size() < deleteCount) {
                    docsToDelete.add(random().nextInt(numDocs));
                }
                for (int docId : docsToDelete) {
                    if (docHasVector[docId]) {
                        docsPerSlice[docSlices[docId]]--;
                        if (docFilterMatch[docId]) {
                            docsPerSliceFiltered[docSlices[docId]]--;
                        }
                    }
                    w.deleteDocuments(new Term(docIdField, "doc_" + docId));
                }
                if (docsToDelete.isEmpty() == false) {
                    w.commit();
                }
            } else if (random().nextBoolean()) {
                w.forceMerge(1);
            }
            float[] vector = randomVector(dimensions);
            try (IndexReader reader = DirectoryReader.open(w)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                Weight filterWeight = null;
                if (applyFilter) {
                    Query filterQuery = new TermQuery(new Term(filterField, filterValue));
                    filterWeight = filterQuery.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1);
                }
                for (int slice = 0; slice < slices; slice++) {
                    int expectedDocs = applyFilter ? docsPerSliceFiltered[slice] : docsPerSlice[slice];
                    Query query = SortedDocValuesField.newSlowExactQuery(sliceField, new BytesRef("" + slice));
                    Weight weight = query.createWeight(searcher, ScoreMode.COMPLETE_NO_SCORES, 1);
                    TopDocs[] topDocsArray = new TopDocs[reader.leaves().size()];
                    for (int i = 0; i < reader.leaves().size(); i++) {
                        LeafReaderContext context = reader.leaves().get(i);
                        LeafReader leafReader = context.reader();

                        int ord = leafReader.getSortedDocValues(sliceField).lookupTerm(new BytesRef("" + slice));
                        if (ord < 0) {
                            topDocsArray[i] = TopDocsCollector.EMPTY_TOPDOCS;
                            continue;
                        }

                        ScorerSupplier scorerSupplier = weight.scorerSupplier(context);
                        DocIdSetIterator iterator = scorerSupplier.get(DocIdSetIterator.NO_MORE_DOCS).iterator();
                        int minDoc = iterator.nextDoc();
                        if (minDoc == DocIdSetIterator.NO_MORE_DOCS) {
                            assertEquals(0, expectedDocs);
                            continue;
                        }
                        int maxDoc = minDoc;
                        while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
                            maxDoc = iterator.docID();
                        }
                        ESAcceptDocs.SliceAcceptDocs sliceAcceptDocs = new SliceAcceptDocs(minDoc, maxDoc + 1);
                        Bits liveDocs = leafReader.getLiveDocs();
                        ESAcceptDocs acceptDocs;
                        if (applyFilter) {
                            ScorerSupplier filterSupplier = filterWeight.scorerSupplier(context);
                            if (filterSupplier == null) {
                                topDocsArray[i] = TopDocsCollector.EMPTY_TOPDOCS;
                                continue;
                            }
                            acceptDocs = new ESAcceptDocs.ScorerSupplierAcceptDocs(
                                () -> filterSupplier.get(Long.MAX_VALUE).iterator(),
                                filterSupplier::cost,
                                liveDocs,
                                leafReader.maxDoc(),
                                ord,
                                () -> sliceAcceptDocs
                            );
                        } else if (liveDocs == null) {
                            acceptDocs = new ESAcceptDocs.ESAcceptDocsAll(ord, () -> sliceAcceptDocs);
                        } else {
                            acceptDocs = new ESAcceptDocs.BitsAcceptDocs(liveDocs, leafReader.maxDoc(), ord, () -> sliceAcceptDocs);
                        }

                        // we might collect the same document twice because of soar assignments
                        KnnCollector collector = new TopKnnCollector(2 * Math.max(1, expectedDocs), Integer.MAX_VALUE);
                        weight.scorer(context);
                        leafReader.searchNearestVectors(vectorField, vector, collector, acceptDocs);
                        TopDocs leafTopDocs = collector.topDocs();
                        ScoreDoc[] adjusted = new ScoreDoc[leafTopDocs.scoreDocs.length];
                        for (int docIndex = 0; docIndex < leafTopDocs.scoreDocs.length; docIndex++) {
                            ScoreDoc scoreDoc = leafTopDocs.scoreDocs[docIndex];
                            adjusted[docIndex] = new ScoreDoc(scoreDoc.doc + context.docBase, scoreDoc.score);
                        }
                        topDocsArray[i] = new TopDocs(leafTopDocs.totalHits, adjusted);
                    }
                    TopDocs topDocs = TopDocs.merge(2 * expectedDocs, topDocsArray);
                    Set<Integer> uniqueDocIds = new HashSet<>();
                    for (int i = 0; i < topDocs.scoreDocs.length; i++) {
                        uniqueDocIds.add(topDocs.scoreDocs[i].doc);
                        Document document = reader.storedFields().document(topDocs.scoreDocs[i].doc);
                        assertThat(document.getField(sliceField).binaryValue().utf8ToString(), equalTo("" + slice));
                        if (applyFilter) {
                            assertThat(document.getField(filterField).binaryValue().utf8ToString(), equalTo(filterValue));
                        }
                    }
                    assertThat(uniqueDocIds, hasSize(expectedDocs));
                }
            }
        }
    }

    private static float[] randomVector(int dims) {
        float[] v = new float[dims];
        for (int i = 0; i < dims; i++) {
            v[i] = random().nextFloat() * 2 - 1;
        }
        return v;
    }

    private static ESNextDiskASHVectorsFormat ashTestFormat() {
        return new ESNextDiskASHVectorsFormat(
            MIN_VECTORS_PER_CLUSTER,
            ESNextDiskASHVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER,
            null
        );
    }

    private static ESNextDiskASHVectorsFormat ashSlicedFormat(String sliceField) {
        return new ESNextDiskASHVectorsFormat(
            MIN_VECTORS_PER_CLUSTER,
            ESNextDiskASHVectorsFormat.DEFAULT_CENTROIDS_PER_PARENT_CLUSTER,
            sliceField
        );
    }
}
