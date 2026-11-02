/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.data;

import org.elasticsearch.compute.test.RandomBlock;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class DocOriginsTests extends ESTestCase {
    /**
     * A builder estimates the dictionary before it builds it, so the estimate must be what the dictionary charges.
     */
    public void testRamBytesEstimatedMatchesRamBytesUsed() {
        List<DocOrigin> origins = RandomBlock.randomDocOrigins(between(1, 50));
        long originBytes = origins.stream().mapToLong(DocOrigin::ramBytesUsed).sum();
        assertThat(DocOrigins.ramBytesEstimated(origins.size(), originBytes), equalTo(DocOrigins.of(origins).ramBytesUsed()));
    }

    public void testRamBytesEstimatedForEmpty() {
        assertThat(DocOrigins.ramBytesEstimated(0, 0), equalTo(DocOrigins.EMPTY.ramBytesUsed()));
    }
}
