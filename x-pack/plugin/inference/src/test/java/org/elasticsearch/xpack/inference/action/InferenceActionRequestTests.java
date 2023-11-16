/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.inference.TaskType;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import static org.hamcrest.collection.IsIterableContainingInOrder.contains;

public class InferenceActionRequestTests extends AbstractWireSerializingTestCase<InferenceAction.Request> {

    @Override
    protected Writeable.Reader<InferenceAction.Request> instanceReader() {
        return InferenceAction.Request::new;
    }

    @Override
    protected InferenceAction.Request createTestInstance() {
        return new InferenceAction.Request(
            randomFrom(TaskType.values()),
            randomAlphaOfLength(6),
            randomList(1, 5, () -> randomAlphaOfLength(8)),
            randomMap(0, 3, () -> new Tuple<>(randomAlphaOfLength(4), randomAlphaOfLength(4)))
        );
    }

    public void testParsing() throws IOException {
        String singleInputRequest = """
            {
              "input": "single text input"
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, singleInputRequest)) {
            var request = InferenceAction.Request.parseRequest("model_id", "sparse_embedding", parser);
            assertThat(request.getInput(), contains("single text input"));
        }

        String multiInputRequest = """
            {
              "input": ["an array", "of", "inputs"]
            }
            """;
        try (var parser = createParser(JsonXContent.jsonXContent, multiInputRequest)) {
            var request = InferenceAction.Request.parseRequest("model_id", "sparse_embedding", parser);
            assertThat(request.getInput(), contains("an array", "of", "inputs"));
        }
    }

    @Override
    protected InferenceAction.Request mutateInstance(InferenceAction.Request instance) throws IOException {
        int select = randomIntBetween(0, 3);
        return switch (select) {
            case 0 -> {
                var nextTask = TaskType.values()[(instance.getTaskType().ordinal() + 1) % TaskType.values().length];
                yield new InferenceAction.Request(nextTask, instance.getModelId(), instance.getInput(), instance.getTaskSettings());
            }
            case 1 -> new InferenceAction.Request(
                instance.getTaskType(),
                instance.getModelId() + "foo",
                instance.getInput(),
                instance.getTaskSettings()
            );
            case 2 -> {
                var changedInputs = new ArrayList<String>(instance.getInput());
                changedInputs.add("bar");
                yield new InferenceAction.Request(instance.getTaskType(), instance.getModelId(), changedInputs, instance.getTaskSettings());
            }
            case 3 -> {
                var taskSettings = new HashMap<>(instance.getTaskSettings());
                if (taskSettings.isEmpty()) {
                    taskSettings.put("foo", "bar");
                } else {
                    var keyToRemove = taskSettings.keySet().iterator().next();
                    taskSettings.remove(keyToRemove);
                }
                yield new InferenceAction.Request(instance.getTaskType(), instance.getModelId(), instance.getInput(), taskSettings);
            }
            default -> {
                throw new UnsupportedOperationException();
            }
        };
    }
}
