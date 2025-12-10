/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.sampling;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class PutSampleConfigurationActionTests extends AbstractWireSerializingTestCase<PutSampleConfigurationAction.Request> {

    @Override
    protected Writeable.Reader<PutSampleConfigurationAction.Request> instanceReader() {
        return PutSampleConfigurationAction.Request::new;
    }

    @Override
    protected PutSampleConfigurationAction.Request createTestInstance() {
        return createRandomRequest();
    }

    @Override
    protected PutSampleConfigurationAction.Request mutateInstance(PutSampleConfigurationAction.Request instance) {
        return switch (randomIntBetween(0, 1)) {
            case 0 -> {
                PutSampleConfigurationAction.Request mutated = new PutSampleConfigurationAction.Request(
                    randomValueOtherThan(instance.getSampleConfiguration(), () -> createRandomSampleConfig()),
                    instance.masterNodeTimeout(),
                    instance.ackTimeout()
                );
                mutated.indices(instance.indices());
                yield mutated;
            }
            case 1 -> {
                PutSampleConfigurationAction.Request mutated = new PutSampleConfigurationAction.Request(
                    instance.getSampleConfiguration(),
                    instance.masterNodeTimeout(),
                    instance.ackTimeout()
                );
                mutated.indices(randomArrayOtherThan(instance.indices(), () -> new String[] { randomAlphaOfLengthBetween(1, 10) }));
                yield mutated;
            }
            default -> throw new IllegalStateException("Invalid mutation case");
        };
    }

    private PutSampleConfigurationAction.Request createRandomRequest() {
        PutSampleConfigurationAction.Request request = new PutSampleConfigurationAction.Request(
            createRandomSampleConfig(),
            TimeValue.timeValueSeconds(randomIntBetween(1, 60)),
            TimeValue.timeValueSeconds(randomIntBetween(1, 60))
        );

        // Randomly set some indices
        request.indices(randomAlphaOfLengthBetween(1, 10));

        return request;
    }

    public void testActionName() {
        assertThat(PutSampleConfigurationAction.NAME, equalTo("indices:admin/sample/config/update"));
        assertThat(PutSampleConfigurationAction.INSTANCE.name(), equalTo(PutSampleConfigurationAction.NAME));
    }

    public void testActionInstance() {
        assertThat(PutSampleConfigurationAction.INSTANCE, notNullValue());
        assertThat(PutSampleConfigurationAction.INSTANCE, sameInstance(PutSampleConfigurationAction.INSTANCE));
    }

    public void testRequestIndicesOptions() {
        PutSampleConfigurationAction.Request request = createRandomRequest();
        assertThat(request.indicesOptions(), equalTo(IndicesOptions.STRICT_SINGLE_INDEX_NO_EXPAND_FORBID_CLOSED_ALLOW_SELECTORS));
        assertThat(request.includeDataStreams(), is(true));
    }

    public void testRequestTaskCreation() {
        PutSampleConfigurationAction.Request request = createRandomRequest();

        long id = randomLong();
        String type = randomAlphaOfLength(5);
        String action = randomAlphaOfLength(10);
        TaskId parentTaskId = randomBoolean() ? null : new TaskId(randomAlphaOfLength(5), randomLong());
        Map<String, String> headers = Map.of("header1", "value1");

        Task task = request.createTask(id, type, action, parentTaskId, headers);

        assertThat(task, notNullValue());
        assertThat(task.getId(), equalTo(id));
        assertThat(task.getType(), equalTo(type));
        assertThat(task.getAction(), equalTo(action));
        assertThat(task.getParentTaskId(), equalTo(parentTaskId));
        assertThat(task.headers(), equalTo(headers));
    }

    private static SamplingConfiguration createRandomSampleConfig() {
        return new SamplingConfiguration(
            randomDoubleBetween(0.0, 1.0, true),
            randomBoolean() ? null : randomIntBetween(1, 1000),
            randomBoolean() ? null : ByteSizeValue.ofKb(randomIntBetween(50, 100)),
            randomBoolean() ? null : new TimeValue(randomIntBetween(1, 30), TimeUnit.DAYS),
            randomBoolean() ? randomAlphaOfLength(10) : null
        );
    }

}
