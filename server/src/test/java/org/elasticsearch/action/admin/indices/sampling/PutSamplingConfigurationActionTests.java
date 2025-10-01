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

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;

public class PutSamplingConfigurationActionTests extends AbstractWireSerializingTestCase<PutSamplingConfigurationAction.Request> {

    @Override
    protected Writeable.Reader<PutSamplingConfigurationAction.Request> instanceReader() {
        return PutSamplingConfigurationAction.Request::new;
    }

    @Override
    protected PutSamplingConfigurationAction.Request createTestInstance() {
        return createRandomRequest();
    }

    @Override
    protected PutSamplingConfigurationAction.Request mutateInstance(PutSamplingConfigurationAction.Request instance) {
        return switch (randomIntBetween(0, 5)) {
            case 0 -> {
                PutSamplingConfigurationAction.Request mutated = new PutSamplingConfigurationAction.Request(
                    randomValueOtherThan(instance.getSampleConfiguration().rate(), () -> randomDoubleBetween(0.0, 1.0, true)),
                    instance.getSampleConfiguration().maxSamples(),
                    instance.getSampleConfiguration().maxSize(),
                    instance.getSampleConfiguration().timeToLive(),
                    instance.getSampleConfiguration().condition(),
                    instance.masterNodeTimeout(),
                    instance.ackTimeout()
                );
                mutated.indices(instance.indices());
                yield mutated;
            }
            case 1 -> {
                PutSamplingConfigurationAction.Request mutated = new PutSamplingConfigurationAction.Request(
                    instance.getSampleConfiguration().rate(),
                    randomValueOtherThan(instance.getSampleConfiguration().maxSamples(), () -> randomIntBetween(1, 1000)),
                    instance.getSampleConfiguration().maxSize(),
                    instance.getSampleConfiguration().timeToLive(),
                    instance.getSampleConfiguration().condition(),
                    instance.masterNodeTimeout(),
                    instance.ackTimeout()
                );
                mutated.indices(instance.indices());
                yield mutated;
            }
            case 2 -> {
                PutSamplingConfigurationAction.Request mutated = new PutSamplingConfigurationAction.Request(
                    instance.getSampleConfiguration().rate(),
                    instance.getSampleConfiguration().maxSamples(),
                    randomValueOtherThan(instance.getSampleConfiguration().maxSize(), () -> ByteSizeValue.ofMb(randomIntBetween(1, 100))),
                    instance.getSampleConfiguration().timeToLive(),
                    instance.getSampleConfiguration().condition(),
                    instance.masterNodeTimeout(),
                    instance.ackTimeout()
                );
                mutated.indices(instance.indices());
                yield mutated;
            }
            case 3 -> {
                PutSamplingConfigurationAction.Request mutated = new PutSamplingConfigurationAction.Request(
                    instance.getSampleConfiguration().rate(),
                    instance.getSampleConfiguration().maxSamples(),
                    instance.getSampleConfiguration().maxSize(),
                    randomValueOtherThan(
                        instance.getSampleConfiguration().timeToLive(),
                        () -> TimeValue.timeValueDays(randomIntBetween(1, 30))
                    ),
                    instance.getSampleConfiguration().condition(),
                    instance.masterNodeTimeout(),
                    instance.ackTimeout()
                );
                mutated.indices(instance.indices());
                yield mutated;
            }
            case 4 -> {
                PutSamplingConfigurationAction.Request mutated = new PutSamplingConfigurationAction.Request(
                    instance.getSampleConfiguration().rate(),
                    instance.getSampleConfiguration().maxSamples(),
                    instance.getSampleConfiguration().maxSize(),
                    instance.getSampleConfiguration().timeToLive(),
                    randomValueOtherThan(instance.getSampleConfiguration().condition(), () -> randomAlphaOfLength(10)),
                    instance.masterNodeTimeout(),
                    instance.ackTimeout()
                );
                mutated.indices(instance.indices());
                yield mutated;
            }
            case 5 -> {
                PutSamplingConfigurationAction.Request mutated = new PutSamplingConfigurationAction.Request(
                    instance.getSampleConfiguration().rate(),
                    instance.getSampleConfiguration().maxSamples(),
                    instance.getSampleConfiguration().maxSize(),
                    instance.getSampleConfiguration().timeToLive(),
                    instance.getSampleConfiguration().condition(),
                    instance.masterNodeTimeout(),
                    instance.ackTimeout()
                );
                mutated.indices(
                    randomValueOtherThan(
                        instance.indices(),
                        () -> randomArray(1, 5, String[]::new, () -> randomAlphaOfLengthBetween(1, 10))
                    )
                );
                yield mutated;
            }
            default -> throw new IllegalStateException("Invalid mutation case");
        };
    }

    private PutSamplingConfigurationAction.Request createRandomRequest() {
        PutSamplingConfigurationAction.Request request = new PutSamplingConfigurationAction.Request(
            randomDoubleBetween(0.0, 1.0, true),
            randomBoolean() ? null : randomIntBetween(1, 1000),
            randomBoolean() ? null : ByteSizeValue.ofMb(randomIntBetween(1, 100)),
            randomBoolean() ? null : TimeValue.timeValueDays(randomIntBetween(1, 30)),
            randomBoolean() ? null : randomAlphaOfLength(10),
            TimeValue.timeValueSeconds(randomIntBetween(1, 60)),
            TimeValue.timeValueSeconds(randomIntBetween(1, 60))
        );

        // Randomly set some indices
        if (randomBoolean()) {
            request.indices(randomArray(0, 3, String[]::new, () -> randomAlphaOfLengthBetween(1, 10)));
        }

        return request;
    }

    public void testActionName() {
        assertThat(PutSamplingConfigurationAction.NAME, equalTo("indices:admin/sampling/config/update"));
        assertThat(PutSamplingConfigurationAction.INSTANCE.name(), equalTo(PutSamplingConfigurationAction.NAME));
    }

    public void testActionInstance() {
        assertThat(PutSamplingConfigurationAction.INSTANCE, notNullValue());
        assertThat(PutSamplingConfigurationAction.INSTANCE, sameInstance(PutSamplingConfigurationAction.INSTANCE));
    }

    public void testRequestIndicesOptions() {
        PutSamplingConfigurationAction.Request request = createRandomRequest();
        assertThat(request.indicesOptions(), equalTo(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED));
        assertThat(request.includeDataStreams(), is(true));
    }

    public void testRequestTaskCreation() {
        PutSamplingConfigurationAction.Request request = createRandomRequest();

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
}
