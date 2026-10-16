/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.FeatureField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.test.ESTestCase;

/**
 * A {@link FeatureField} keeps its weight in the term frequency, so prefixing the term must leave that weight alone and the
 * query must still score the document it belongs to. Covers the shape {@code sparse_vector} and {@code rank_features} use.
 */
public class SliceFeatureFieldTests extends ESTestCase {

    public void testPrefixedFeatureKeepsItsWeightAndScores() throws Exception {
        final String slice = "tenant-1";
        final String prefix = SliceIndexing.termPrefix(slice);
        try (Directory dir = newDirectory()) {
            try (IndexWriter w = new IndexWriter(dir, new IndexWriterConfig(new StandardAnalyzer()))) {
                LuceneDocument doc = new LuceneDocument(new SliceTermPrefix(slice));
                doc.add(new FeatureField("sparse", "mytoken", 3.0f));
                w.addDocument(doc);
                LuceneDocument other = new LuceneDocument(new SliceTermPrefix("tenant-2"));
                other.add(new FeatureField("sparse", "mytoken", 7.0f));
                w.addDocument(other);
            }
            try (DirectoryReader r = DirectoryReader.open(dir)) {
                IndexSearcher searcher = newSearcher(r);
                // The slice's own term matches only its document.
                TopDocs mine = searcher.search(FeatureField.newLinearQuery("sparse", prefix + "mytoken", 1.0f), 10);
                assertEquals(1, mine.totalHits.value());
                assertTrue("the feature scored nothing", mine.scoreDocs[0].score > 0f);

                // The other slice's copy of the same feature is a different term, and carries its own weight.
                TopDocs theirs = searcher.search(
                    FeatureField.newLinearQuery("sparse", SliceIndexing.termPrefix("tenant-2") + "mytoken", 1.0f),
                    10
                );
                assertEquals(1, theirs.totalHits.value());
                // Weight 7 outscores weight 3, so the term frequency survived the prefix.
                assertTrue(
                    "the weight did not survive prefixing: " + theirs.scoreDocs[0].score + " vs " + mine.scoreDocs[0].score,
                    theirs.scoreDocs[0].score > mine.scoreDocs[0].score
                );

                // The plain term is not in the dictionary at all.
                assertEquals(0, r.docFreq(new Term("sparse", "mytoken")));
                assertEquals(1, r.docFreq(new Term("sparse", prefix + "mytoken")));
            }
        }
    }
}
