/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.async;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

public class AsyncExecutionIdTests extends ESTestCase {
    public void testEncode() {
        for (int i = 0; i < 10; i++) {
            AsyncExecutionId instance = new AsyncExecutionId(UUIDs.randomBase64UUID(),
                new TaskId(randomAlphaOfLengthBetween(5, 20), randomNonNegativeLong()));
            String encoded = AsyncExecutionId.encode(instance.getDocId(), instance.getTaskId());
            AsyncExecutionId same = AsyncExecutionId.decode(encoded);
            assertEquals(same, instance);

            AsyncExecutionId mutate = mutate(instance);
            assertNotEquals(mutate, instance);
            assertNotEquals(mutate, same);
        }
    }

    private AsyncExecutionId mutate(AsyncExecutionId id) {
        int rand = randomIntBetween(0, 1);
        switch (rand) {
            case 0:
                return new AsyncExecutionId(randomAlphaOfLength(id.getDocId().length()+1), id.getTaskId());

            case 1:
                return new AsyncExecutionId(id.getDocId(),
                    new TaskId(randomAlphaOfLength(id.getTaskId().getNodeId().length()), randomNonNegativeLong()));

            default:
                throw new AssertionError();
        }
    }
}
