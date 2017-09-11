/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plugin;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils.MutateFunction;
import org.elasticsearch.xpack.sql.plugin.SqlGetIndicesAction.Request;

import java.util.Arrays;
import java.util.function.Supplier;

public class SqlGetIndicesRequestTests extends AbstractStreamableTestCase<SqlGetIndicesAction.Request> {
    @Override
    protected Request createTestInstance() {
        Request request = new Request(randomIndicesOptions(), randomIndices());
        request.local(randomBoolean());
        request.masterNodeTimeout(randomTimeValue());
        request.setParentTask(randomTaskId());
        return request;
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected MutateFunction<Request> getMutateFunction() {
        return SqlGetIndicesRequestTests::mutate;
    }

    private static Request mutate(Request request) {
        @SuppressWarnings("unchecked")
        Supplier<Request> supplier = randomFrom(
                () -> {
                    Request mutant = new Request(
                        randomValueOtherThan(request.indicesOptions(), SqlGetIndicesRequestTests::randomIndicesOptions),
                        request.indices());
                    mutant.local(request.local());
                    mutant.masterNodeTimeout(request.masterNodeTimeout());
                    mutant.setParentTask(request.getParentTask());
                    return mutant;
                },
                () -> {
                    Request mutant = new Request(
                        request.indicesOptions(),
                        randomValueOtherThanMany(i -> Arrays.equals(request.indices(), i), SqlGetIndicesRequestTests::randomIndices));
                    mutant.local(request.local());
                    mutant.masterNodeTimeout(request.masterNodeTimeout());
                    mutant.setParentTask(request.getParentTask());
                    return mutant;
                }, () -> {
                    Request mutant = new Request(request.indicesOptions(), request.indices());
                    mutant.local(false == request.local());
                    mutant.masterNodeTimeout(request.masterNodeTimeout());
                    mutant.setParentTask(request.getParentTask());
                    return mutant;
                }, () -> {
                    Request mutant = new Request(request.indicesOptions(), request.indices());
                    mutant.local(request.local());
                    mutant.masterNodeTimeout(randomValueOtherThan(request.masterNodeTimeout(),
                            () -> TimeValue.parseTimeValue(randomTimeValue(), "test")));
                    mutant.setParentTask(request.getParentTask());
                    return mutant;
                }, () -> {
                    Request mutant = new Request(request.indicesOptions(), request.indices());
                    mutant.local(false == request.local());
                    mutant.masterNodeTimeout(request.masterNodeTimeout());
                    mutant.setParentTask(randomValueOtherThan(request.getParentTask(), SqlGetIndicesRequestTests::randomTaskId));
                    return mutant;
                });
        return supplier.get();
    }

    private static IndicesOptions randomIndicesOptions() {
        return IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(),
                randomBoolean(), randomBoolean());
    }

    private static String[] randomIndices() {
        String[] indices = new String[between(1, 10)];
        for (int i = 0; i < indices.length; i++) {
            indices[i] = randomAlphaOfLength(5);
        }
        return indices;
    }

    private static TaskId randomTaskId() {
        return randomBoolean() ? TaskId.EMPTY_TASK_ID : new TaskId(randomAlphaOfLength(5), randomLong());
    }
}
