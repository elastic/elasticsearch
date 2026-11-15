/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.single.shard;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.emptyArray;

public class SingleShardRequestTests extends ESTestCase {

    public void testIndicesWithoutIndex() {
        // The index is optional for some requests, and they must not report an array with a null element as their indices
        assertThat(new TestRequest().indices(), emptyArray());
    }

    public void testIndicesWithIndex() {
        String index = randomAlphaOfLengthBetween(1, 10);

        assertThat(new TestRequest().index(index).indices(), arrayContaining(index));
    }

    private static class TestRequest extends SingleShardRequest<TestRequest> {
        @Override
        public ActionRequestValidationException validate() {
            return null;
        }
    }
}
