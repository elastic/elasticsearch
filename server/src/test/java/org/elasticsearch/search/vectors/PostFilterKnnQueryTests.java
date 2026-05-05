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
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.Weight;
import org.apache.lucene.search.join.BitSetProducer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.FixedBitSet;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class PostFilterKnnQueryTests extends ESTestCase {

    public void testMergeResults() {
        {
            ScoreDoc[] result = KnnQueryUtils.mergeResults(new ScoreDoc[0], new ScoreDoc[0]);
            assertEquals(0, result.length);
        }
        {
            ScoreDoc[] input = new ScoreDoc[] { new ScoreDoc(1, 0.9f), new ScoreDoc(2, 0.8f) };
            ScoreDoc[] result = KnnQueryUtils.mergeResults(new ScoreDoc[0], input);
            assertEquals(2, result.length);
            assertEquals(1, result[0].doc);
            assertEquals(2, result[1].doc);
        }
        {
            ScoreDoc[] existing = new ScoreDoc[] { new ScoreDoc(1, 0.9f), new ScoreDoc(3, 0.7f) };
            ScoreDoc[] incoming = new ScoreDoc[] { new ScoreDoc(2, 0.8f), new ScoreDoc(4, 0.6f) };
            ScoreDoc[] result = KnnQueryUtils.mergeResults(existing, incoming);
            assertEquals(4, result.length);
            assertEquals(1, result[0].doc);
            assertEquals(0.9f, result[0].score, 0.001f);
            assertEquals(2, result[1].doc);
            assertEquals(0.8f, result[1].score, 0.001f);
            assertEquals(3, result[2].doc);
            assertEquals(0.7f, result[2].score, 0.001f);
            assertEquals(4, result[3].doc);
            assertEquals(0.6f, result[3].score, 0.001f);
        }
        {
            ScoreDoc[] existing = new ScoreDoc[] { new ScoreDoc(1, 0.9f), new ScoreDoc(2, 0.7f) };
            ScoreDoc[] incoming = new ScoreDoc[] { new ScoreDoc(2, 0.8f), new ScoreDoc(3, 0.6f) };
            ScoreDoc[] result = KnnQueryUtils.mergeResults(existing, incoming);
            // doc 2 appears in both; the higher-ranked one (0.8 from incoming) wins
            // Merge order: 1(0.9) from existing, 2(0.8) from incoming, 2(0.7) from existing (dup skipped), 3(0.6)
            assertEquals(3, result.length);
            assertEquals(1, result[0].doc);
            assertEquals(0.9f, result[0].score, 0.001f);
        }
    }

    public void testApplyFilterPassesMatchingDocs() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 5; i++) {
                Document doc = new Document();
                doc.add(new StringField("tag", i % 2 == 0 ? "pass" : "fail", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                Weight filterWeight = searcher.createWeight(
                    searcher.rewrite(new TermQuery(new Term("tag", "pass"))),
                    ScoreMode.COMPLETE_NO_SCORES,
                    1f
                );

                ScoreDoc[] candidates = new ScoreDoc[] {
                    new ScoreDoc(0, 0.9f),
                    new ScoreDoc(1, 0.8f),
                    new ScoreDoc(2, 0.7f),
                    new ScoreDoc(3, 0.6f),
                    new ScoreDoc(4, 0.5f) };

                ScoreDoc[] result = KnnQueryUtils.applyFilter(candidates, filterWeight, searcher);
                // Even docs (0, 2, 4) pass, sorted by score descending
                assertEquals(3, result.length);
                assertEquals(0, result[0].doc);
                assertEquals(0.9f, result[0].score, 0.001f);
                assertEquals(2, result[1].doc);
                assertEquals(0.7f, result[1].score, 0.001f);
                assertEquals(4, result[2].doc);
                assertEquals(0.5f, result[2].score, 0.001f);
            }
        }
    }

    public void testApplyFilterReturnsEmptyWhenNoneMatch() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            Document doc = new Document();
            doc.add(new StringField("tag", "a", Field.Store.NO));
            writer.addDocument(doc);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                Weight filterWeight = searcher.createWeight(
                    searcher.rewrite(new TermQuery(new Term("tag", "nonexistent"))),
                    ScoreMode.COMPLETE_NO_SCORES,
                    1f
                );
                ScoreDoc[] candidates = new ScoreDoc[] { new ScoreDoc(0, 0.9f) };
                ScoreDoc[] result = KnnQueryUtils.applyFilter(candidates, filterWeight, searcher);
                assertEquals(0, result.length);
            }
        }
    }

    public void testDeduplicateByParentKeepsHighestScoring() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            // 6 docs: children 0,1 under parent 2; children 3,4 under parent 5
            for (int i = 0; i < 6; i++) {
                writer.addDocument(new Document());
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                BitSetProducer parentsFilter = context -> {
                    FixedBitSet bits = new FixedBitSet(context.reader().maxDoc());
                    bits.set(2);
                    bits.set(5);
                    return bits;
                };

                // Sorted by score descending
                ScoreDoc[] docs = new ScoreDoc[] {
                    new ScoreDoc(1, 0.9f),  // child → parent 2
                    new ScoreDoc(0, 0.8f),  // child → parent 2 (dup)
                    new ScoreDoc(4, 0.7f),  // child → parent 5
                    new ScoreDoc(3, 0.6f),  // child → parent 5 (dup)
                };

                ScoreDoc[] result = KnnQueryUtils.deduplicateByParent(docs, reader, parentsFilter);
                assertEquals(2, result.length);
                assertEquals(1, result[0].doc);
                assertEquals(0.9f, result[0].score, 0.001f);
                assertEquals(4, result[1].doc);
                assertEquals(0.7f, result[1].score, 0.001f);
            }
        }
    }

    public void testDeduplicateByParentNoParents() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                // No parent bits set → all docs filtered out (no parent found)
                BitSetProducer parentsFilter = context -> { return new FixedBitSet(context.reader().maxDoc()); };

                ScoreDoc[] docs = new ScoreDoc[] { new ScoreDoc(0, 0.9f), new ScoreDoc(1, 0.8f) };
                ScoreDoc[] result = KnnQueryUtils.deduplicateByParent(docs, reader, parentsFilter);
                // nextSetBit returns NO_MORE_DOCS for empty bitset, so all are filtered
                assertEquals(0, result.length);
            }
        }
    }

    public void testExcludeDocsQueryFiltersCorrectly() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 5; i++) {
                writer.addDocument(new Document());
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);

                int[] excluded = new int[] { 1, 3 };

                ExcludeDocsQuery query = new ExcludeDocsQuery(excluded, searcher.getIndexReader());
                Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);

                LeafReaderContext leaf = searcher.getIndexReader().leaves().get(0);
                ScorerSupplier ss = weight.scorerSupplier(leaf);
                assertNotNull(ss);

                Scorer scorer = ss.get(0);
                DocIdSetIterator iter = scorer.iterator();

                assertEquals(0, iter.nextDoc());
                assertEquals(2, iter.nextDoc());
                assertEquals(4, iter.nextDoc());
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, iter.nextDoc());
            }
        }
    }

    public void testExcludeDocsQueryReturnsNullWhenAllExcluded() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
            }
            writer.forceMerge(1);
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);

                int[] excluded = new int[] { 0, 1, 2, };

                ExcludeDocsQuery query = new ExcludeDocsQuery(excluded, searcher.getIndexReader());
                Weight weight = searcher.createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);

                LeafReaderContext leaf = searcher.getIndexReader().leaves().get(0);
                assertNull(weight.scorerSupplier(leaf));
            }
        }
    }

    public void testExcludeDocsQueryEquality() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 5; i++) {
                writer.addDocument(new Document());
            }
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                int[] excluded1 = new int[] { 1 };
                int[] excluded2 = new int[] { 1 };
                int[] excluded3 = new int[] { 2 };

                ExcludeDocsQuery q1 = new ExcludeDocsQuery(excluded1, reader);
                ExcludeDocsQuery q2 = new ExcludeDocsQuery(excluded2, reader);
                ExcludeDocsQuery q3 = new ExcludeDocsQuery(excluded3, reader);

                assertEquals(q1, q2);
                assertEquals(q1.hashCode(), q2.hashCode());
                assertNotEquals(q1, q3);
            }
        }
    }
}
