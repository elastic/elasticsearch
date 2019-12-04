/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

public class AsyncSearchIdTests extends ESTestCase {
    public void testEncode() throws Exception {
        for (int i = 0; i < 10; i++) {
            AsyncSearchId instance = new AsyncSearchId(randomAlphaOfLengthBetween(5, 20), UUIDs.randomBase64UUID(),
                new TaskId(randomAlphaOfLengthBetween(5, 20), randomNonNegativeLong()));
            String encoded = AsyncSearchId.encode(instance.getIndexName(), instance.getDocId(), instance.getTaskId());
            AsyncSearchId same = AsyncSearchId.decode(encoded);
            assertEquals(same, instance);

            AsyncSearchId mutate = mutate(instance);
            assertNotEquals(mutate, instance);
            assertNotEquals(mutate, same);
        }
    }

    private AsyncSearchId mutate(AsyncSearchId id) {
        int rand = randomIntBetween(0, 2);
        switch (rand) {
            case 0:
                return new AsyncSearchId(randomAlphaOfLength(id.getIndexName().length()+1), id.getDocId(), id.getTaskId());

            case 1:
                return new AsyncSearchId(id.getIndexName(), randomAlphaOfLength(id.getDocId().length()+1), id.getTaskId());

            case 2:
                return new AsyncSearchId(id.getIndexName(), id.getDocId(),
                    new TaskId(randomAlphaOfLength(id.getTaskId().getNodeId().length()), randomNonNegativeLong()));

            default:
                throw new AssertionError();
        }
    }
}
