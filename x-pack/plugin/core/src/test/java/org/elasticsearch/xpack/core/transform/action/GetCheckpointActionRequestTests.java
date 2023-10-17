/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction.Request;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class GetCheckpointActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return randomRequest(10);
    }

    @Override
    protected Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request mutateInstance(Request instance) {
        List<String> indices = instance.indices() != null ? new ArrayList<>(Arrays.asList(instance.indices())) : new ArrayList<>();
        IndicesOptions indicesOptions = instance.indicesOptions();

        switch (between(0, 1)) {
            case 0:
                indices.add(randomAlphaOfLengthBetween(1, 20));
                break;
            case 1:
                indicesOptions = IndicesOptions.fromParameters(
                    randomFrom(IndicesOptions.WildcardStates.values()).name().toLowerCase(Locale.ROOT),
                    Boolean.toString(instance.indicesOptions().ignoreUnavailable() == false),
                    Boolean.toString(instance.indicesOptions().allowNoIndices() == false),
                    Boolean.toString(instance.indicesOptions().ignoreThrottled() == false),
                    SearchRequest.DEFAULT_INDICES_OPTIONS
                );
                break;
            default:
                throw new AssertionError("Illegal randomization branch");
        }

        return new Request(indices.toArray(new String[0]), indicesOptions);
    }

    public void testCreateTask() {
        GetCheckpointAction.Request request = randomRequest(17);
        CancellableTask task = request.createTask(123, "type", "action", new TaskId("dummy-node:456"), Map.of());
        assertThat(task.getDescription(), is(equalTo("get_checkpoint[17]")));
    }

    public void testCreateTaskWithNullIndices() {
        GetCheckpointAction.Request request = new Request(null, null);
        CancellableTask task = request.createTask(123, "type", "action", new TaskId("dummy-node:456"), Map.of());
        assertThat(task.getDescription(), is(equalTo("get_checkpoint[0]")));
    }

    private static GetCheckpointAction.Request randomRequest(int numIndices) {
        return new Request(
            randomBoolean() ? null : Stream.generate(() -> randomAlphaOfLength(10)).limit(numIndices).toArray(String[]::new),
            IndicesOptions.fromParameters(
                randomFrom(IndicesOptions.WildcardStates.values()).name().toLowerCase(Locale.ROOT),
                Boolean.toString(randomBoolean()),
                Boolean.toString(randomBoolean()),
                Boolean.toString(randomBoolean()),
                SearchRequest.DEFAULT_INDICES_OPTIONS
            )
        );
    }
}
