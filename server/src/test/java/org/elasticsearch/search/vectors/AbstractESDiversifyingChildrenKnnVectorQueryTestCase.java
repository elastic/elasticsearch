/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.search.join.CheckJoinIndex;
import org.apache.lucene.search.join.QueryBitSetProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

abstract class AbstractESDiversifyingChildrenKnnVectorQueryTestCase extends ESTestCase {

    static byte[] fromFloat(float[] queryVector) {
        byte[] query = new byte[queryVector.length];
        for (int i = 0; i < queryVector.length; i++) {
            assert queryVector[i] == (byte) queryVector[i];
            query[i] = (byte) queryVector[i];
        }
        return query;
    }

    abstract Query getParentJoinKnnQuery(
        String fieldName,
        float[] queryVector,
        Query childFilter,
        int k,
        BitSetProducer parentBitSet,
        int numChildrenPerParent
    );

    abstract Field getKnnVectorField(String name, float[] vector);

    static BitSetProducer parentFilter(IndexReader r) throws IOException {
        // Create a filter that defines "parent" documents in the index
        BitSetProducer parentsFilter = new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));
        CheckJoinIndex.check(r, parentsFilter);
        return parentsFilter;
    }

    Document makeParent() {
        Document parent = new Document();
        parent.add(newStringField("docType", "_parent", Field.Store.NO));
        return parent;
    }

    public void testIndexWithNoVectorsNorParents() throws IOException {
        try (Directory d = newDirectory()) {
            try (IndexWriter w = new IndexWriter(d, newIndexWriterConfig().setMergePolicy(newMergePolicy(random(), false)))) {
                // Add some documents without a vector
                for (int i = 0; i < 5; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("other", "value", Field.Store.NO));
                    w.addDocument(doc);
                }
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                // Create parent filter directly, tests use "check" to verify parentIds exist. Production
                // may not
                // verify we handle it gracefully
                BitSetProducer parentFilter = new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));
                Query query = getParentJoinKnnQuery("field", new float[] { 2, 2 }, null, 3, parentFilter, randomIntBetween(1, 10));
                TopDocs topDocs = searcher.search(query, 3);
                assertEquals(0, topDocs.totalHits.value);
                assertEquals(0, topDocs.scoreDocs.length);
            }
        }
    }

    public void testIndexWithNoParents() throws IOException {
        try (Directory d = newDirectory()) {
            try (IndexWriter w = new IndexWriter(d, newIndexWriterConfig().setMergePolicy(newMergePolicy(random(), false)))) {
                for (int i = 0; i < 3; ++i) {
                    Document doc = new Document();
                    doc.add(getKnnVectorField("field", new float[] { 2, 2 }));
                    doc.add(newStringField("id", Integer.toString(i), Field.Store.YES));
                    w.addDocument(doc);
                }
                // Add some documents without a vector
                for (int i = 0; i < 5; i++) {
                    Document doc = new Document();
                    doc.add(new StringField("other", "value", Field.Store.NO));
                    w.addDocument(doc);
                }
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                IndexSearcher searcher = new IndexSearcher(reader);
                // Create parent filter directly, tests use "check" to verify parentIds exist. Production
                // may not
                // verify we handle it gracefully
                BitSetProducer parentFilter = new QueryBitSetProducer(new TermQuery(new Term("docType", "_parent")));
                Query query = getParentJoinKnnQuery("field", new float[] { 2, 2 }, null, 3, parentFilter, randomIntBetween(1, 10));
                TopDocs topDocs = searcher.search(query, 3);
                assertEquals(0, topDocs.totalHits.value);
                assertEquals(0, topDocs.scoreDocs.length);
            }
        }
    }

    public void testFilterWithNoVectorMatches() throws IOException {
        try (
            Directory indexStore = getIndexStore("field", new float[] { 0, 1 }, new float[] { 1, 2 }, new float[] { 0, 0 });
            IndexReader reader = DirectoryReader.open(indexStore)
        ) {
            IndexSearcher searcher = newSearcher(reader);
            Query filter = new TermQuery(new Term("other", "value"));
            BitSetProducer parentFilter = parentFilter(reader);
            Query kvq = getParentJoinKnnQuery("field", new float[] { 1, 2 }, filter, 2, parentFilter, randomIntBetween(1, 10));
            TopDocs topDocs = searcher.search(kvq, 3);
            assertEquals(0, topDocs.totalHits.value);
        }
    }

    public void testScoringWithMultipleChildrenButOnlySelecting1() throws IOException {
        try (Directory d = newDirectory()) {
            try (IndexWriter w = new IndexWriter(d, newIndexWriterConfig().setMergePolicy(newMergePolicy(random(), false)))) {
                List<Document> toAdd = new ArrayList<>();
                for (int j = 1; j <= 5; j++) {
                    Document doc = new Document();
                    doc.add(getKnnVectorField("field", new float[] { j, j }));
                    doc.add(newStringField("id", Integer.toString(j), Field.Store.YES));
                    toAdd.add(doc);
                }
                toAdd.add(makeParent());
                w.addDocuments(toAdd);

                toAdd = new ArrayList<>();
                for (int j = 7; j <= 11; j++) {
                    Document doc = new Document();
                    doc.add(getKnnVectorField("field", new float[] { j, j }));
                    doc.add(newStringField("id", Integer.toString(j), Field.Store.YES));
                    toAdd.add(doc);
                }
                toAdd.add(makeParent());
                w.addDocuments(toAdd);
                w.forceMerge(1);
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                assertEquals(1, reader.leaves().size());
                IndexSearcher searcher = new IndexSearcher(reader);
                BitSetProducer parentFilter = parentFilter(searcher.getIndexReader());
                Query query = getParentJoinKnnQuery("field", new float[] { 2, 2 }, null, 3, parentFilter, 1);
                assertScorerResults(searcher, query, new float[] { 1f, 1f / 51f }, new String[] { "2", "7" });

                query = getParentJoinKnnQuery("field", new float[] { 6, 6 }, null, 3, parentFilter, 1);
                assertScorerResults(searcher, query, new float[] { 1f / 3f, 1f / 3f }, new String[] { "5", "7" });

                query = getParentJoinKnnQuery("field", new float[] { 6, 6 }, new MatchAllDocsQuery(), 1, parentFilter, 1);
                assertScorerResults(searcher, query, new float[] { 1f / 3f }, new String[] { "5" });
            }
        }
    }

    public void testScoringWithMultipleChildren() throws IOException {
        try (Directory d = newDirectory()) {
            try (IndexWriter w = new IndexWriter(d, newIndexWriterConfig().setMergePolicy(newMergePolicy(random(), false)))) {
                List<Document> toAdd = new ArrayList<>();
                for (int j = 1; j <= 5; j++) {
                    Document doc = new Document();
                    doc.add(getKnnVectorField("field", new float[] { j, j }));
                    doc.add(newStringField("id", Integer.toString(j), Field.Store.YES));
                    toAdd.add(doc);
                    if (randomBoolean()) {
                        Document noVecDoc = new Document();
                        noVecDoc.add(new StringField("other", "value", Field.Store.NO));
                        toAdd.add(noVecDoc);
                    }
                }
                toAdd.add(makeParent());
                w.addDocuments(toAdd);

                toAdd = new ArrayList<>();
                for (int j = 7; j <= 11; j++) {
                    Document doc = new Document();
                    doc.add(getKnnVectorField("field", new float[] { j, j }));
                    doc.add(newStringField("id", Integer.toString(j), Field.Store.YES));
                    toAdd.add(doc);
                    if (randomBoolean()) {
                        Document noVecDoc = new Document();
                        noVecDoc.add(new StringField("other", "value", Field.Store.NO));
                        toAdd.add(noVecDoc);
                    }
                }
                toAdd.add(makeParent());
                w.addDocuments(toAdd);
                w.forceMerge(1);
            }
            try (IndexReader reader = DirectoryReader.open(d)) {
                assertEquals(1, reader.leaves().size());
                IndexSearcher searcher = new IndexSearcher(reader);
                BitSetProducer parentFilter = parentFilter(searcher.getIndexReader());
                Query query = getParentJoinKnnQuery("field", new float[] { 2, 2 }, null, 3, parentFilter, 2);
                assertScorerResults(searcher, query, new float[] { 1f / 3f, 1f, 1f / 51f, 1f / 73f }, new String[] { "1", "2", "7", "8" });

                query = getParentJoinKnnQuery("field", new float[] { 6, 6 }, null, 3, parentFilter, 2);
                assertScorerResults(
                    searcher,
                    query,
                    new float[] { 1f / 9f, 1f / 3f, 1f / 3f, 1f / 9f },
                    new String[] { "4", "5", "7", "8" }
                );
                query = getParentJoinKnnQuery("field", new float[] { 6, 6 }, new MatchAllDocsQuery(), 1, parentFilter, 2);
                assertScorerResults(searcher, query, new float[] { 1f / 9f, 1f / 3f }, new String[] { "4", "5" });
            }
        }
    }

    Directory getIndexStore(String field, float[]... contents) throws IOException {
        Directory indexStore = newDirectory();
        RandomIndexWriter writer = new RandomIndexWriter(
            random(),
            indexStore,
            newIndexWriterConfig().setMergePolicy(newMergePolicy(random(), false))
        );
        for (int i = 0; i < contents.length; ++i) {
            List<Document> toAdd = new ArrayList<>();
            Document doc = new Document();
            doc.add(getKnnVectorField(field, contents[i]));
            doc.add(newStringField("id", Integer.toString(i), Field.Store.YES));
            toAdd.add(doc);
            toAdd.add(makeParent());
            writer.addDocuments(toAdd);
        }
        // Add some documents without a vector
        for (int i = 0; i < 5; i++) {
            List<Document> toAdd = new ArrayList<>();
            Document doc = new Document();
            doc.add(new StringField("other", "value", Field.Store.NO));
            toAdd.add(doc);
            toAdd.add(makeParent());
            writer.addDocuments(toAdd);
        }
        writer.close();
        return indexStore;
    }

    void assertIdMatches(IndexReader reader, String expectedId, int docId) throws IOException {
        String actualId = reader.storedFields().document(docId).get("id");
        assertEquals(expectedId, actualId);
    }

    void assertScorerResults(IndexSearcher searcher, Query query, float[] scores, String[] ids) throws IOException {
        IndexReader reader = searcher.getIndexReader();
        Query rewritten = query.rewrite(searcher);
        Weight weight = searcher.createWeight(rewritten, ScoreMode.COMPLETE, 1);
        Scorer scorer = weight.scorer(searcher.getIndexReader().leaves().get(0));
        // prior to advancing, score is undefined
        assertEquals(-1, scorer.docID());
        expectThrows(ArrayIndexOutOfBoundsException.class, scorer::score);
        DocIdSetIterator it = scorer.iterator();
        for (int i = 0; i < scores.length; i++) {
            int docId = it.nextDoc();
            assertNotEquals(NO_MORE_DOCS, docId);
            assertEquals(scores[i], scorer.score(), 0.0001);
            assertIdMatches(reader, ids[i], docId);
        }
    }
}
