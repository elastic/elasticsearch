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
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ExcludeDocsQueryTests extends ESTestCase {

    public void testFiltersCorrectly() throws IOException {
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

    public void testReturnsNullWhenAllExcluded() throws IOException {
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

    public void testEquality() throws IOException {
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

    public void testExplain() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
            }
            writer.forceMerge(1);
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                ExcludeDocsQuery q = new ExcludeDocsQuery(new int[] { 1 }, searcher.getIndexReader());
                Weight weight = searcher.createWeight(q, ScoreMode.COMPLETE_NO_SCORES, 1f);
                LeafReaderContext leaf = searcher.getIndexReader().leaves().get(0);

                Explanation excluded = weight.explain(leaf, 1);
                assertFalse(excluded.isMatch());

                Explanation kept = weight.explain(leaf, 0);
                assertTrue(kept.isMatch());
            }
        }
    }

    public void testAcrossLeaves() throws IOException {
        IndexWriterConfig cfg = new IndexWriterConfig();
        cfg.setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, cfg)) {
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
            }
            writer.commit();
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
            }
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                assertEquals(2, searcher.getIndexReader().leaves().size());

                // Exclude global doc 1 (seg 0) and global doc 4 (= local 1 in seg 1) — verifies
                // that the per-leaf binary-search slice correctly maps global to local IDs.
                ExcludeDocsQuery q = new ExcludeDocsQuery(new int[] { 1, 4 }, searcher.getIndexReader());
                Weight weight = searcher.createWeight(q, ScoreMode.COMPLETE_NO_SCORES, 1f);

                LeafReaderContext leaf0 = searcher.getIndexReader().leaves().get(0);
                DocIdSetIterator it0 = weight.scorerSupplier(leaf0).get(0).iterator();
                assertEquals(0, it0.nextDoc());
                assertEquals(2, it0.nextDoc());
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, it0.nextDoc());

                LeafReaderContext leaf1 = searcher.getIndexReader().leaves().get(1);
                DocIdSetIterator it1 = weight.scorerSupplier(leaf1).get(0).iterator();
                assertEquals(0, it1.nextDoc());
                assertEquals(2, it1.nextDoc());
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, it1.nextDoc());
            }
        }
    }

    public void testLeafWithNoExcludedDocs() throws IOException {
        IndexWriterConfig cfg = new IndexWriterConfig();
        cfg.setMergePolicy(NoMergePolicy.INSTANCE);
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, cfg)) {
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
            }
            writer.commit();
            for (int i = 0; i < 3; i++) {
                writer.addDocument(new Document());
            }
            writer.commit();

            try (IndexReader reader = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(reader);
                assertEquals(2, searcher.getIndexReader().leaves().size());

                // Only seg 0 docs are excluded — seg 1 sees an empty slice of excludedDocs and
                // should pass every doc through unchanged.
                ExcludeDocsQuery q = new ExcludeDocsQuery(new int[] { 0, 1 }, searcher.getIndexReader());
                Weight weight = searcher.createWeight(q, ScoreMode.COMPLETE_NO_SCORES, 1f);

                LeafReaderContext leaf1 = searcher.getIndexReader().leaves().get(1);
                DocIdSetIterator it = weight.scorerSupplier(leaf1).get(0).iterator();
                assertEquals(0, it.nextDoc());
                assertEquals(1, it.nextDoc());
                assertEquals(2, it.nextDoc());
                assertEquals(DocIdSetIterator.NO_MORE_DOCS, it.nextDoc());
            }
        }
    }

    public void testRejectsForeignReader() throws IOException {
        try (Directory dir1 = newDirectory(); Directory dir2 = newDirectory()) {
            try (IndexWriter w1 = new IndexWriter(dir1, new IndexWriterConfig())) {
                w1.addDocument(new Document());
                w1.commit();
            }
            try (IndexWriter w2 = new IndexWriter(dir2, new IndexWriterConfig())) {
                w2.addDocument(new Document());
                w2.commit();
            }
            try (IndexReader r1 = DirectoryReader.open(dir1); IndexReader r2 = DirectoryReader.open(dir2)) {
                ExcludeDocsQuery q = new ExcludeDocsQuery(new int[] { 0 }, r1);
                IndexSearcher s2 = newSearcher(r2);
                expectThrows(IllegalStateException.class, () -> s2.createWeight(q, ScoreMode.COMPLETE_NO_SCORES, 1f));
            }
        }
    }

    public void testToString() throws IOException {
        try (Directory dir = newDirectory(); IndexWriter writer = new IndexWriter(dir, new IndexWriterConfig())) {
            writer.addDocument(new Document());
            writer.commit();
            try (IndexReader reader = DirectoryReader.open(dir)) {
                ExcludeDocsQuery q = new ExcludeDocsQuery(new int[] { 1, 2, 3 }, reader);
                assertEquals("ExcludeDocsQuery[count=3]", q.toString("any-field"));
            }
        }
    }
}
