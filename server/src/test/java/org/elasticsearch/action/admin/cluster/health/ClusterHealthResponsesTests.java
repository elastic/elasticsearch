/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.health;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.health.ClusterIndexHealthTests;
import org.elasticsearch.cluster.health.ClusterStateHealth;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ClusterHealthResponsesTests extends AbstractSerializingTestCase<ClusterHealthResponse> {
    private final ClusterHealthRequest.Level level = randomFrom(ClusterHealthRequest.Level.values());

    public void testIsTimeout() {
        ClusterHealthResponse res = new ClusterHealthResponse();
        for (int i = 0; i < 5; i++) {
            res.setTimedOut(randomBoolean());
            if (res.isTimedOut()) {
                assertEquals(RestStatus.REQUEST_TIMEOUT, res.status());
            } else {
                assertEquals(RestStatus.OK, res.status());
            }
        }
    }

    public void testClusterHealth() throws IOException {
        ClusterState clusterState = ClusterState.builder(ClusterName.CLUSTER_NAME_SETTING.getDefault(Settings.EMPTY)).build();
        int pendingTasks = randomIntBetween(0, 200);
        int inFlight = randomIntBetween(0, 200);
        int delayedUnassigned = randomIntBetween(0, 200);
        TimeValue pendingTaskInQueueTime = TimeValue.timeValueMillis(randomIntBetween(1000, 100000));
        ClusterHealthResponse clusterHealth = new ClusterHealthResponse(
            "bla",
            new String[] { Metadata.ALL },
            clusterState,
            pendingTasks,
            inFlight,
            delayedUnassigned,
            pendingTaskInQueueTime
        );
        clusterHealth = maybeSerialize(clusterHealth);
        assertClusterHealth(clusterHealth);
        assertThat(clusterHealth.getNumberOfPendingTasks(), Matchers.equalTo(pendingTasks));
        assertThat(clusterHealth.getNumberOfInFlightFetch(), Matchers.equalTo(inFlight));
        assertThat(clusterHealth.getDelayedUnassignedShards(), Matchers.equalTo(delayedUnassigned));
        assertThat(clusterHealth.getTaskMaxWaitingTime().millis(), is(pendingTaskInQueueTime.millis()));
        assertThat(clusterHealth.getActiveShardsPercent(), is(allOf(greaterThanOrEqualTo(0.0), lessThanOrEqualTo(100.0))));
    }

    private void assertClusterHealth(ClusterHealthResponse clusterHealth) {
        ClusterStateHealth clusterStateHealth = clusterHealth.getClusterStateHealth();

        assertThat(clusterHealth.getActiveShards(), Matchers.equalTo(clusterStateHealth.getActiveShards()));
        assertThat(clusterHealth.getRelocatingShards(), Matchers.equalTo(clusterStateHealth.getRelocatingShards()));
        assertThat(clusterHealth.getActivePrimaryShards(), Matchers.equalTo(clusterStateHealth.getActivePrimaryShards()));
        assertThat(clusterHealth.getInitializingShards(), Matchers.equalTo(clusterStateHealth.getInitializingShards()));
        assertThat(clusterHealth.getUnassignedShards(), Matchers.equalTo(clusterStateHealth.getUnassignedShards()));
        assertThat(clusterHealth.getNumberOfNodes(), Matchers.equalTo(clusterStateHealth.getNumberOfNodes()));
        assertThat(clusterHealth.getNumberOfDataNodes(), Matchers.equalTo(clusterStateHealth.getNumberOfDataNodes()));
    }

    ClusterHealthResponse maybeSerialize(ClusterHealthResponse clusterHealth) throws IOException {
        if (randomBoolean()) {
            BytesStreamOutput out = new BytesStreamOutput();
            clusterHealth.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            clusterHealth = ClusterHealthResponse.readResponseFrom(in);
        }
        return clusterHealth;
    }

    @Override
    protected ClusterHealthResponse doParseInstance(XContentParser parser) {
        return ClusterHealthResponse.fromXContent(parser);
    }

    @Override
    protected ClusterHealthResponse createTestInstance() {
        int indicesSize = randomInt(20);
        Map<String, ClusterIndexHealth> indices = Maps.newMapWithExpectedSize(indicesSize);
        if (ClusterHealthRequest.Level.INDICES.equals(level) || ClusterHealthRequest.Level.SHARDS.equals(level)) {
            for (int i = 0; i < indicesSize; i++) {
                String indexName = randomAlphaOfLengthBetween(1, 5) + i;
                indices.put(indexName, ClusterIndexHealthTests.randomIndexHealth(indexName, level));
            }
        }
        ClusterStateHealth stateHealth = new ClusterStateHealth(
            randomInt(100),
            randomInt(100),
            randomInt(100),
            randomInt(100),
            randomInt(100),
            randomInt(100),
            randomInt(100),
            randomDoubleBetween(0d, 100d, true),
            randomFrom(ClusterHealthStatus.values()),
            indices
        );

        return new ClusterHealthResponse(
            randomAlphaOfLengthBetween(1, 10),
            randomInt(100),
            randomInt(100),
            randomInt(100),
            TimeValue.timeValueMillis(randomInt(10000)),
            randomBoolean(),
            stateHealth
        );
    }

    @Override
    protected Writeable.Reader<ClusterHealthResponse> instanceReader() {
        return ClusterHealthResponse::new;
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap("level", level.name().toLowerCase(Locale.ROOT)));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    // Ignore all paths which looks like "indices.RANDOMINDEXNAME.shards"
    private static final Pattern SHARDS_IN_XCONTENT = Pattern.compile("^indices\\.\\w+\\.shards$");

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> "indices".equals(field) || SHARDS_IN_XCONTENT.matcher(field).find();
    }

    @Override
    protected ClusterHealthResponse mutateInstance(ClusterHealthResponse instance) {
        String mutate = randomFrom(
            "clusterName",
            "numberOfPendingTasks",
            "numberOfInFlightFetch",
            "delayedUnassignedShards",
            "taskMaxWaitingTime",
            "timedOut",
            "clusterStateHealth"
        );
        switch (mutate) {
            case "clusterName":
                return new ClusterHealthResponse(
                    instance.getClusterName() + randomAlphaOfLengthBetween(2, 5),
                    instance.getNumberOfPendingTasks(),
                    instance.getNumberOfInFlightFetch(),
                    instance.getDelayedUnassignedShards(),
                    instance.getTaskMaxWaitingTime(),
                    instance.isTimedOut(),
                    instance.getClusterStateHealth()
                );
            case "numberOfPendingTasks":
                return new ClusterHealthResponse(
                    instance.getClusterName(),
                    instance.getNumberOfPendingTasks() + between(1, 10),
                    instance.getNumberOfInFlightFetch(),
                    instance.getDelayedUnassignedShards(),
                    instance.getTaskMaxWaitingTime(),
                    instance.isTimedOut(),
                    instance.getClusterStateHealth()
                );
            case "numberOfInFlightFetch":
                return new ClusterHealthResponse(
                    instance.getClusterName(),
                    instance.getNumberOfPendingTasks(),
                    instance.getNumberOfInFlightFetch() + between(1, 10),
                    instance.getDelayedUnassignedShards(),
                    instance.getTaskMaxWaitingTime(),
                    instance.isTimedOut(),
                    instance.getClusterStateHealth()
                );
            case "delayedUnassignedShards":
                return new ClusterHealthResponse(
                    instance.getClusterName(),
                    instance.getNumberOfPendingTasks(),
                    instance.getNumberOfInFlightFetch(),
                    instance.getDelayedUnassignedShards() + between(1, 10),
                    instance.getTaskMaxWaitingTime(),
                    instance.isTimedOut(),
                    instance.getClusterStateHealth()
                );
            case "taskMaxWaitingTime":

                return new ClusterHealthResponse(
                    instance.getClusterName(),
                    instance.getNumberOfPendingTasks(),
                    instance.getNumberOfInFlightFetch(),
                    instance.getDelayedUnassignedShards(),
                    new TimeValue(instance.getTaskMaxWaitingTime().millis() + between(1, 10)),
                    instance.isTimedOut(),
                    instance.getClusterStateHealth()
                );
            case "timedOut":
                return new ClusterHealthResponse(
                    instance.getClusterName(),
                    instance.getNumberOfPendingTasks(),
                    instance.getNumberOfInFlightFetch(),
                    instance.getDelayedUnassignedShards(),
                    instance.getTaskMaxWaitingTime(),
                    instance.isTimedOut() == false,
                    instance.getClusterStateHealth()
                );
            case "clusterStateHealth":
                ClusterStateHealth state = instance.getClusterStateHealth();
                ClusterStateHealth newState = new ClusterStateHealth(
                    state.getActivePrimaryShards() + between(1, 10),
                    state.getActiveShards(),
                    state.getRelocatingShards(),
                    state.getInitializingShards(),
                    state.getUnassignedShards(),
                    state.getNumberOfNodes(),
                    state.getNumberOfDataNodes(),
                    state.getActiveShardsPercent(),
                    state.getStatus(),
                    state.getIndices()
                );
                return new ClusterHealthResponse(
                    instance.getClusterName(),
                    instance.getNumberOfPendingTasks(),
                    instance.getNumberOfInFlightFetch(),
                    instance.getDelayedUnassignedShards(),
                    instance.getTaskMaxWaitingTime(),
                    instance.isTimedOut(),
                    newState
                );
            default:
                throw new UnsupportedOperationException();
        }
    }
}
