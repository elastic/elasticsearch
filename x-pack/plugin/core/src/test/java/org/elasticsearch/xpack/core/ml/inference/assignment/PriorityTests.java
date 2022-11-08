/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ml.inference.assignment;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.equalTo;

public class PriorityTests extends ESTestCase {

    public void testToAndFromString() {
        for (Priority priority : Priority.values()) {
            String value = priority.toString();
            assertThat(Priority.fromString(value), equalTo(priority));
        }
    }
}
