/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.allocation;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class AllocationStateTests extends ESTestCase {

    public void testToAndFromString() {
        for (AllocationState state : AllocationState.values()) {
            String value = state.toString();
            assertThat(AllocationState.fromString(value), equalTo(state));
        }
    }

}
