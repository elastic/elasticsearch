/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.transform.action.GetTransformStatsAction.Request;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class GetTransformStatsActionRequestTests extends AbstractWireSerializingTestCase<Request> {
    @Override
    protected Request createTestInstance() {
        return new Request(
            randomBoolean() ? randomAlphaOfLengthBetween(1, 20) : randomBoolean() ? Metadata.ALL : null,
            randomBoolean() ? TimeValue.parseTimeValue(randomTimeValue(), "timeout") : null
        );
    }

    @Override
    protected Request mutateInstance(Request instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Request> instanceReader() {
        return Request::new;
    }

    public void testCreateTask() {
        Request request = new Request("some-transform", null);
        Task task = request.createTask(123, "type", "action", TaskId.EMPTY_TASK_ID, Map.of());
        assertThat(task, is(instanceOf(CancellableTask.class)));
        assertThat(task.getDescription(), is(equalTo("get_transform_stats[some-transform]")));
    }
}
