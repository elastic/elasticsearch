/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.async;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;

import java.io.IOException;
import java.util.Base64;

import static org.hamcrest.CoreMatchers.instanceOf;

public class AsyncExecutionIdTests extends ESTestCase {
    public void testEncodeAndDecode() {
        for (int i = 0; i < 10; i++) {
            AsyncExecutionId instance = randomAsyncId();
            String encoded = AsyncExecutionId.encode(instance.getDocId(), instance.getTaskId());
            AsyncExecutionId same = AsyncExecutionId.decode(encoded);
            assertEquals(same, instance);
        }
    }

    private static AsyncExecutionId randomAsyncId() {
        return new AsyncExecutionId(UUIDs.randomBase64UUID(), new TaskId(randomAlphaOfLengthBetween(5, 20), randomNonNegativeLong()));
    }

    private static AsyncExecutionId mutate(AsyncExecutionId id) {
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

    public void testEqualsAndHashcode() {
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(randomAsyncId(),
            instance -> new AsyncExecutionId(instance.getDocId(), instance.getTaskId()),
            AsyncExecutionIdTests::mutate);
    }

    public void testDecodeInvalidId() throws IOException {
        {
            IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> AsyncExecutionId.decode("wrong"));
            assertEquals("invalid id: [wrong]", exc.getMessage());
            assertThat(exc.getCause(), instanceOf(IllegalArgumentException.class));
        }
        {
            IllegalArgumentException exc = expectThrows(IllegalArgumentException.class,
                () -> AsyncExecutionId.decode("FmhEOGQtRWVpVGplSXRtOVZudXZCOVEaYjFVZjZNWndRa3V0VmJvNV8tQmRpZzoxMzM=?pretty"));
            assertEquals("invalid id: [FmhEOGQtRWVpVGplSXRtOVZudXZCOVEaYjFVZjZNWndRa3V0VmJvNV8tQmRpZzoxMzM=?pretty]", exc.getMessage());
            assertThat(exc.getCause(), instanceOf(IllegalArgumentException.class));
        }
        {
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.writeString(randomAlphaOfLengthBetween(5, 10));
                out.writeString(new TaskId(randomAlphaOfLengthBetween(5, 10), randomLong()).toString());
                out.writeString("wrong");
                String encoded = Base64.getUrlEncoder().encodeToString(BytesReference.toBytes(out.bytes()));
                IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> AsyncExecutionId.decode(encoded));
                assertEquals("invalid id: [" + encoded + "]", exc.getMessage());
                assertNull(exc.getCause());
            }
        }
    }
}
