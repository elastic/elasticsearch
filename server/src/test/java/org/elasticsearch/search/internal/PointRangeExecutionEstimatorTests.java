/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.internal;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;

import static org.elasticsearch.search.internal.BreakerWeightTestUtils.assertChargeIsCeilingOnRetainedSet;

public class PointRangeExecutionEstimatorTests extends ESTestCase {

    private static final String FIELD = "p";
    /** Large enough that the leaf's bit set dwarfs the BKD traversal scratch, which is charged on top of it. */
    private static final int NUM_DOCS = 1 << 18;

    private Directory directory;
    private DirectoryReader reader;
    private int valuesPerDoc;

    /** One segment in which every doc has the same number of distinct, consecutive values, so range costs are near exact. */
    @Before
    public void indexConsecutivePoints() throws IOException {
        directory = newDirectory();
        valuesPerDoc = between(1, 4);
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(null))) {
            for (int docId = 0; docId < NUM_DOCS; docId++) {
                Document doc = new Document();
                for (int v = 0; v < valuesPerDoc; v++) {
                    doc.add(new IntPoint(FIELD, docId * valuesPerDoc + v));
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        reader = DirectoryReader.open(directory);
    }

    @After
    public void closeReader() throws IOException {
        IOUtils.close(reader, directory);
    }

    /**
     * A range matching more points than {@code maxDoc >>> 7} makes {@code DocIdSetBuilder} upgrade to a bit set, which the scorer
     * keeps, even though {@code 4 * cost} bytes of buffered doc ids would still be well below the size of that bit set.
     */
    public void testChargeIsCeilingOnBitSetPastBuilderThreshold() throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        int totalValues = NUM_DOCS * valuesPerDoc;
        for (int iter = 0; iter < 10; iter++) {
            int matchingPoints = between((NUM_DOCS >>> 7) + 1, NUM_DOCS / 40);
            int lower = between(1, totalValues - matchingPoints - 1);
            assertChargeIsCeilingOnRetainedSet(searcher, IntPoint.newRangeQuery(FIELD, lower, lower + matchingPoints - 1));
        }
    }

    /** Covers the sparse builder, the dense inverse path of single-valued fields, and everything in between. */
    public void testChargeIsCeilingOnRetainedSet() throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        int totalValues = NUM_DOCS * valuesPerDoc;
        for (int iter = 0; iter < 10; iter++) {
            int matchingPoints = between(1, totalValues - 2);
            int lower = between(1, totalValues - matchingPoints - 1);
            assertChargeIsCeilingOnRetainedSet(searcher, IntPoint.newRangeQuery(FIELD, lower, lower + matchingPoints - 1));
        }
    }
}
