/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.exchange;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class CcsExchangeRequestTests extends ESTestCase {

    public void testSerializationRoundTrip() throws IOException {
        String exchangeId = randomAlphaOfLength(10);
        boolean sourcesFinished = randomBoolean();
        String[] indices = generateRandomStringArray(5, 8, false, false);
        IndicesOptions indicesOptions = IndicesOptions.fromOptions(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean()
        );

        CcsExchangeRequest original = new CcsExchangeRequest(exchangeId, sourcesFinished, indices, indicesOptions);
        if (sourcesFinished == false) {
            original.setParentTask(new TaskId("node-1", randomNonNegativeLong()));
        }

        CcsExchangeRequest deserialized = copyInstance(original);

        assertThat(deserialized.exchangeId(), equalTo(original.exchangeId()));
        assertThat(deserialized.sourcesFinished(), equalTo(original.sourcesFinished()));
        assertArrayEquals(original.indices(), deserialized.indices());
        assertArrayEquals(original.originalQueryIndices(), deserialized.originalQueryIndices());
        assertThat(deserialized.indicesOptions(), equalTo(original.indicesOptions()));
    }

    public void testOriginalQueryIndicesPreservedAfterReplace() throws IOException {
        String[] originalIndices = new String[] { "index-a", "index-b" };
        CcsExchangeRequest request = new CcsExchangeRequest(
            "ex-1",
            true,
            originalIndices,
            IndicesOptions.STRICT_EXPAND_OPEN
        );
        request.indices("replacement-index");

        CcsExchangeRequest deserialized = copyInstance(request);

        assertThat(deserialized.indices(), equalTo(new String[] { "replacement-index" }));
        assertThat(deserialized.originalQueryIndices(), equalTo(originalIndices));
    }

    public void testParentTaskSourcesFinished() {
        CcsExchangeRequest request = new CcsExchangeRequest("ex-1", true, new String[] { "idx" }, IndicesOptions.STRICT_EXPAND_OPEN);
        request.setParentTask(new TaskId("node-1", 1));
        assertSame(TaskId.EMPTY_TASK_ID, request.getParentTask());
    }

    public void testParentTaskSourcesNotFinished() {
        CcsExchangeRequest request = new CcsExchangeRequest("ex-1", false, new String[] { "idx" }, IndicesOptions.STRICT_EXPAND_OPEN);
        TaskId parentTask = new TaskId("node-2", 2);
        request.setParentTask(parentTask);
        assertThat(request.getParentTask(), equalTo(parentTask));
    }

    private static CcsExchangeRequest copyInstance(CcsExchangeRequest original) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new CcsExchangeRequest(in);
            }
        }
    }
}
