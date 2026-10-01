/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.querysampling.storage;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.querysampling.capture.CapturedQuery;
import org.elasticsearch.xpack.querysampling.capture.CapturedSearch;
import org.elasticsearch.xpack.querysampling.dedup.QueryFingerprint;
import org.elasticsearch.xpack.querysampling.dedup.TrackedQuery;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class Tier1BufferTests extends ESTestCase {

    public void testStoresUpToItsCapacityAndTurnsTheRestAway() {
        int capacity = between(1, 20);
        Tier1Buffer buffer = new Tier1Buffer(capacity);

        for (int i = 0; i < capacity; i++) {
            assertTrue(buffer.add(sampled(i)));
        }
        int overflow = between(1, 10);
        for (int i = 0; i < overflow; i++) {
            assertFalse(buffer.add(sampled(capacity + i)));
        }

        assertThat(buffer.size(), equalTo(capacity));
        assertThat(buffer.rejected(), equalTo((long) overflow));
    }

    private static SampledQuery sampled(long id) {
        CapturedQuery query = new CapturedQuery(new String[] { "idx" }, "vec", new float[] { id }, 10, 100, null, null, List.of(), null);
        return new SampledQuery(new QueryFingerprint(id, id), new CapturedSearch(query, List.of(), 1), new TrackedQuery());
    }
}
