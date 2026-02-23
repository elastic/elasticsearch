/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopKnnCollector;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.tests.util.LuceneTestCase;

import java.util.Random;

public class AdaptiveHnswQueueSaturationCollectorTests extends LuceneTestCase {

    public void testDelegate() {
        Random random = random();
        int numDocs = 100;
        int k = random.nextInt(1, 10);
        KnnCollector delegate = new TopKnnCollector(k, numDocs);
        AdaptiveHnswQueueSaturationCollector queueSaturationCollector = new AdaptiveHnswQueueSaturationCollector(delegate);
        for (int i = 0; i < random.nextInt(numDocs); i++) {
            queueSaturationCollector.collect(random.nextInt(numDocs), random.nextFloat(1.0f));
        }
        assertEquals(delegate.k(), queueSaturationCollector.k());
        assertEquals(delegate.visitedCount(), queueSaturationCollector.visitedCount());
        assertEquals(delegate.visitLimit(), queueSaturationCollector.visitLimit());
        assertEquals(delegate.minCompetitiveSimilarity(), queueSaturationCollector.minCompetitiveSimilarity(), 1e-3);
    }

    public void testEarlyExpectedExit() {
        int numDocs = 1000;
        int k = 10;
        KnnCollector delegate = new TopKnnCollector(k, numDocs);
        AdaptiveHnswQueueSaturationCollector queueSaturationCollector = new AdaptiveHnswQueueSaturationCollector(delegate);
        for (int i = 0; i < numDocs; i++) {
            queueSaturationCollector.collect(i, 1.0f - i * 1e-3f);
            if (i % 10 == 0) {
                queueSaturationCollector.nextCandidate();
            }
            if (queueSaturationCollector.earlyTerminated()) {
                assertEquals(110, i);
                break;
            }
        }
    }

    public void testDelegateVsSaturateEarlyExit() {
        Random random = random();
        int numDocs = 10000;
        int k = random.nextInt(1, 100);
        KnnCollector delegate = new TopKnnCollector(k, numDocs);
        AdaptiveHnswQueueSaturationCollector queueSaturationCollector = new AdaptiveHnswQueueSaturationCollector(delegate);
        for (int i = 0; i < random.nextInt(numDocs); i++) {
            queueSaturationCollector.collect(random.nextInt(numDocs), random.nextFloat(1.0f));
            if (i % 10 == 0) {
                queueSaturationCollector.nextCandidate();
            }
            boolean earlyTerminatedSaturation = queueSaturationCollector.earlyTerminated();
            boolean earlyTerminatedDelegate = delegate.earlyTerminated();
            assertTrue(earlyTerminatedSaturation || earlyTerminatedDelegate == false);
        }
    }

    public void testEarlyExitRelation() {
        Random random = random();
        int numDocs = 10000;
        int k = random.nextInt(1, 100);
        KnnCollector delegate = new TopKnnCollector(k, random.nextInt(numDocs));
        AdaptiveHnswQueueSaturationCollector queueSaturationCollector = new AdaptiveHnswQueueSaturationCollector(delegate);
        for (int i = 0; i < random.nextInt(numDocs); i++) {
            queueSaturationCollector.collect(random.nextInt(numDocs), random.nextFloat(1.0f));
            if (i % 10 == 0) {
                queueSaturationCollector.nextCandidate();
            }
            if (delegate.earlyTerminated()) {
                TopDocs topDocs = queueSaturationCollector.topDocs();
                assertEquals(TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO, topDocs.totalHits.relation());
                break;
            }
            if (queueSaturationCollector.earlyTerminated()) {
                TopDocs topDocs = queueSaturationCollector.topDocs();
                assertEquals(TotalHits.Relation.EQUAL_TO, topDocs.totalHits.relation());
                break;
            }
        }
    }

}
