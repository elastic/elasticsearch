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
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.LongRange;
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

public class PointsExecutionEstimatorTests extends ESTestCase {

    private static final String POINT_FIELD = "p";
    private static final String RANGE_FIELD = "r";
    private static final long MAX_VALUE = 1000;

    private Directory directory;
    private DirectoryReader reader;

    @Before
    public void indexRandomPoints() throws IOException {
        directory = newDirectory();
        boolean multiValued = randomBoolean();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(null))) {
            int numDocs = between(1000, 20_000);
            for (int docId = 0; docId < numDocs; docId++) {
                Document doc = new Document();
                int values = multiValued ? between(1, 4) : 1;
                for (int v = 0; v < values; v++) {
                    doc.add(new LongPoint(POINT_FIELD, randomLongBetween(0, MAX_VALUE)));
                    long min = randomLongBetween(0, MAX_VALUE);
                    doc.add(new LongRange(RANGE_FIELD, new long[] { min }, new long[] { min + randomLongBetween(0, 50) }));
                }
                writer.addDocument(doc);
            }
            if (randomBoolean()) {
                writer.forceMerge(1);
            }
        }
        reader = DirectoryReader.open(directory);
    }

    @After
    public void closeReader() throws IOException {
        IOUtils.close(reader, directory);
    }

    /** Checks the charge is a ceiling on the doc id set each leaf's scorer keeps, for set queries and range field queries. */
    public void testChargeIsCeilingOnRetainedSet() throws IOException {
        IndexSearcher searcher = new IndexSearcher(reader);
        searcher.setQueryCache(null);
        for (int iter = 0; iter < 20; iter++) {
            long lower = randomLongBetween(0, MAX_VALUE);
            long upper = lower + randomLongBetween(0, MAX_VALUE);
            long[] set = randomLongsBetween(between(1, 200), -10, MAX_VALUE + 10).toArray();
            assertChargeIsCeilingOnRetainedSet(searcher, LongPoint.newSetQuery(POINT_FIELD, set));
            assertChargeIsCeilingOnRetainedSet(
                searcher,
                LongRange.newIntersectsQuery(RANGE_FIELD, new long[] { lower }, new long[] { upper })
            );
            assertChargeIsCeilingOnRetainedSet(searcher, LongRange.newWithinQuery(RANGE_FIELD, new long[] { lower }, new long[] { upper }));
        }
    }
}
