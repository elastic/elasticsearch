/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.index.codec.vectors.cluster.NeighborQueue;
import org.elasticsearch.test.ESTestCase;

import static org.elasticsearch.search.vectors.AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE;
import static org.elasticsearch.search.vectors.IVFKnnSearchStrategyTests.sharedBound;

public class SharedMinCompetitiveScoreTests extends ESTestCase {

    public void testReadOnlyViewCannotPublish() {
        SharedMinCompetitiveScore shared = new SharedMinCompetitiveScore();
        SharedMinCompetitiveScore readOnly = shared.readOnly();
        assertTrue(shared.canPublish());
        assertFalse(readOnly.canPublish());

        readOnly.publish(NeighborQueue.encodeRaw(randomIntBetween(0, 1000), randomFloatBetween(0f, 1f, true)));
        assertEquals(LEAST_COMPETITIVE, readOnly.get());
        assertEquals(LEAST_COMPETITIVE, shared.get());
    }

    /** The source is the publishable instance a view was created from with {@link SharedMinCompetitiveScore#readOnly()}. */
    public void testReadOnlyViewReadsScorePublishedBySource() {
        SharedMinCompetitiveScore shared = new SharedMinCompetitiveScore();
        SharedMinCompetitiveScore readOnly = shared.readOnly();

        float first = randomFloatBetween(0f, 1f, true);
        shared.publish(NeighborQueue.encodeRaw(randomIntBetween(0, 1000), first));
        assertEquals(sharedBound(first), readOnly.get());

        // the view reads the live score, not a copy taken when it was created
        float second = randomFloatBetween(1.5f, 2f, true);
        shared.publish(NeighborQueue.encodeRaw(randomIntBetween(0, 1000), second));
        assertEquals(sharedBound(second), readOnly.get());

        // a view of a view is the same view, and a view created after publishing also reads the current score
        assertSame(readOnly, readOnly.readOnly());
        assertEquals(sharedBound(second), shared.readOnly().get());
    }

    /** The publisher's doc id is segment-local, so only the score is kept and a less competitive score is ignored. */
    public void testPublishKeepsOnlyTheMaxScore() {
        SharedMinCompetitiveScore shared = new SharedMinCompetitiveScore();
        shared.publish(NeighborQueue.encodeRaw(7, 1f));
        // same score, more competitive doc id: no change
        shared.publish(NeighborQueue.encodeRaw(0, 1f));
        shared.publish(NeighborQueue.encodeRaw(0, 0.5f));
        assertEquals(sharedBound(1f), shared.get());
        assertEquals(Math.nextDown(1f), NeighborQueue.decodeScoreRaw(shared.get()), 0f);
    }
}
