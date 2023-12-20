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
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointAction.Request;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class GetCheckpointActionRequestTests extends AbstractWireSerializingTransformTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        return randomRequest(randomBoolean() ? 10 : null);
    }

    @Override
    protected Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request mutateInstance(Request instance) {
        List<String> indices = instance.indices() != null ? new ArrayList<>(Arrays.asList(instance.indices())) : new ArrayList<>();
        IndicesOptions indicesOptions = instance.indicesOptions();
        QueryBuilder query = instance.getQuery();
        String cluster = instance.getCluster();
        TimeValue timeout = instance.getTimeout();

        switch (between(0, 4)) {
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
            case 2:
                query = query != null ? null : QueryBuilders.matchAllQuery();
                break;
            case 3:
                cluster = cluster != null ? null : randomAlphaOfLengthBetween(1, 10);
                break;
            case 4:
                timeout = timeout != null ? null : TimeValue.timeValueSeconds(randomIntBetween(1, 300));
                break;
            default:
                throw new AssertionError("Illegal randomization branch");
        }

        return new Request(indices.toArray(new String[0]), indicesOptions, query, cluster, timeout);
    }

    public void testCreateTask() {
        Request request = randomRequest(17);
        CancellableTask task = request.createTask(123, "type", "action", new TaskId("dummy-node:456"), Map.of());
        assertThat(task.getDescription(), is(equalTo("get_checkpoint[17]")));
    }

    public void testCreateTaskWithNullIndices() {
        Request request = new Request(null, null, null, null, null);
        CancellableTask task = request.createTask(123, "type", "action", new TaskId("dummy-node:456"), Map.of());
        assertThat(task.getDescription(), is(equalTo("get_checkpoint[0]")));
    }

    private static Request randomRequest(Integer numIndices) {
        return new Request(
            numIndices != null ? Stream.generate(() -> randomAlphaOfLength(10)).limit(numIndices).toArray(String[]::new) : null,
            IndicesOptions.fromParameters(
                randomFrom(IndicesOptions.WildcardStates.values()).name().toLowerCase(Locale.ROOT),
                Boolean.toString(randomBoolean()),
                Boolean.toString(randomBoolean()),
                Boolean.toString(randomBoolean()),
                SearchRequest.DEFAULT_INDICES_OPTIONS
            ),
            randomBoolean() ? QueryBuilders.matchAllQuery() : null,
            randomBoolean() ? randomAlphaOfLengthBetween(1, 10) : null,
            randomBoolean() ? TimeValue.timeValueSeconds(randomIntBetween(1, 300)) : null
        );
    }
}
