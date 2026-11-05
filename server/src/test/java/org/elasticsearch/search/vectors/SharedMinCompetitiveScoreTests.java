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

        long first = NeighborQueue.encodeRaw(randomIntBetween(0, 1000), randomFloatBetween(0f, 1f, true));
        shared.publish(first);
        assertEquals(first, readOnly.get());

        // the view reads the live score, not a copy taken when it was created
        long second = NeighborQueue.encodeRaw(randomIntBetween(0, 1000), randomFloatBetween(1.5f, 2f, true));
        assertTrue(second > first);
        shared.publish(second);
        assertEquals(second, readOnly.get());

        // a view of a view is the same view, and a view created after publishing also reads the current score
        assertSame(readOnly, readOnly.readOnly());
        assertEquals(second, shared.readOnly().get());
    }
}
