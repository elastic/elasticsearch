/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.action;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.transform.action.GetCheckpointNodeAction.Request;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class GetCheckpointNodeActionRequestTests extends AbstractWireSerializingTestCase<Request> {

    @Override
    protected Reader<Request> instanceReader() {
        return Request::new;
    }

    @Override
    protected Request createTestInstance() {
        return new Request(
            randomShards(randomInt(10)),
            randomOriginalIndices(randomIntBetween(0, 20)),
            randomBoolean() ? randomTimeout() : null
        );
    }

    @Override
    protected Request mutateInstance(Request instance) {

        switch (random().nextInt(2)) {
            case 0 -> {
                Set<ShardId> shards = new HashSet<>(instance.getShards());
                if (randomBoolean() && shards.size() > 0) {
                    ShardId firstShard = shards.iterator().next();
                    shards.remove(firstShard);
                    if (randomBoolean()) {
                        shards.add(new ShardId(randomAlphaOfLength(8), randomAlphaOfLength(4), randomInt(5)));
                    }
                } else {
                    shards.add(new ShardId(randomAlphaOfLength(8), randomAlphaOfLength(4), randomInt(5)));
                }
                return new Request(shards, instance.getOriginalIndices(), instance.getTimeout());
            }
            case 1 -> {
                OriginalIndices originalIndices = randomOriginalIndices(instance.indices().length + 1);
                return new Request(instance.getShards(), originalIndices, instance.getTimeout());
            }
            case 2 -> {
                return new Request(
                    instance.getShards(),
                    instance.getOriginalIndices(),
                    instance.getTimeout() != null ? null : randomTimeout()
                );
            }
            default -> throw new IllegalStateException("The test should only allow 1 parameters mutated");
        }
    }

    public void testCreateTask() {
        Request request = new Request(randomShards(7), randomOriginalIndices(19), null);
        CancellableTask task = request.createTask(123, "type", "action", new TaskId("dummy-node:456"), Map.of());
        assertThat(task.getDescription(), is(equalTo("get_checkpoint_node[19;7]")));
    }

    public void testCreateTaskWithNullShardsAndIndices() {
        Request request = new Request(null, OriginalIndices.NONE, null);
        CancellableTask task = request.createTask(123, "type", "action", new TaskId("dummy-node:456"), Map.of());
        assertThat(task.getDescription(), is(equalTo("get_checkpoint_node[0;0]")));
    }

    public void testCreateTaskWithNullShards() {
        Request request = new Request(null, randomOriginalIndices(13), null);
        CancellableTask task = request.createTask(123, "type", "action", new TaskId("dummy-node:456"), Map.of());
        assertThat(task.getDescription(), is(equalTo("get_checkpoint_node[13;0]")));
    }

    public void testCreateTaskWithNullIndices() {
        Request request = new Request(randomShards(11), OriginalIndices.NONE, null);
        CancellableTask task = request.createTask(123, "type", "action", new TaskId("dummy-node:456"), Map.of());
        assertThat(task.getDescription(), is(equalTo("get_checkpoint_node[0;11]")));
    }

    private static Set<ShardId> randomShards(int numShards) {
        Set<ShardId> shards = new HashSet<>();
        for (int i = 0; i < numShards; ++i) {
            shards.add(new ShardId(randomAlphaOfLength(4) + i, randomAlphaOfLength(4), randomInt(5)));
        }
        return shards;
    }

    private static OriginalIndices randomOriginalIndices(int numIndices) {
        String[] randomIndices = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            randomIndices[i] = randomAlphaOfLengthBetween(5, 10);
        }
        IndicesOptions indicesOptions = randomBoolean() ? IndicesOptions.strictExpand() : IndicesOptions.lenientExpandOpen();
        return new OriginalIndices(randomIndices, indicesOptions);
    }

    private static TimeValue randomTimeout() {
        return TimeValue.timeValueSeconds(randomIntBetween(1, 300));
    }
}
