/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.health;

import org.elasticsearch.action.ClusterStatsLevel;
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
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ClusterHealthResponsesTests extends AbstractXContentSerializingTestCase<ClusterHealthResponse> {

    private static final ConstructingObjectParser<ClusterHealthResponse, Void> PARSER = new ConstructingObjectParser<>(
        "cluster_health_response",
        true,
        parsedObjects -> {
            int i = 0;
            // ClusterStateHealth fields
            int numberOfNodes = (int) parsedObjects[i++];
            int numberOfDataNodes = (int) parsedObjects[i++];
            int activeShards = (int) parsedObjects[i++];
            int relocatingShards = (int) parsedObjects[i++];
            int activePrimaryShards = (int) parsedObjects[i++];
            int initializingShards = (int) parsedObjects[i++];
            int unassignedShards = (int) parsedObjects[i++];
            int unassignedPrimaryShards = (int) parsedObjects[i++];
            double activeShardsPercent = (double) parsedObjects[i++];
            String statusStr = (String) parsedObjects[i++];
            ClusterHealthStatus status = ClusterHealthStatus.fromString(statusStr);
            @SuppressWarnings("unchecked")
            List<ClusterIndexHealth> indexList = (List<ClusterIndexHealth>) parsedObjects[i++];
            final Map<String, ClusterIndexHealth> indices;
            if (indexList == null || indexList.isEmpty()) {
                indices = emptyMap();
            } else {
                indices = Maps.newMapWithExpectedSize(indexList.size());
                for (ClusterIndexHealth indexHealth : indexList) {
                    indices.put(indexHealth.getIndex(), indexHealth);
                }
            }
            ClusterStateHealth stateHealth = new ClusterStateHealth(
                activePrimaryShards,
                activeShards,
                relocatingShards,
                initializingShards,
                unassignedShards,
                unassignedPrimaryShards,
                numberOfNodes,
                numberOfDataNodes,
                activeShardsPercent,
                status,
                indices
            );

            // ClusterHealthResponse fields
            String clusterName = (String) parsedObjects[i++];
            int numberOfPendingTasks = (int) parsedObjects[i++];
            int numberOfInFlightFetch = (int) parsedObjects[i++];
            int delayedUnassignedShards = (int) parsedObjects[i++];
            long taskMaxWaitingTimeMillis = (long) parsedObjects[i++];
            boolean timedOut = (boolean) parsedObjects[i];
            return new ClusterHealthResponse(
                clusterName,
                numberOfPendingTasks,
                numberOfInFlightFetch,
                delayedUnassignedShards,
                TimeValue.timeValueMillis(taskMaxWaitingTimeMillis),
                timedOut,
                stateHealth
            );
        }
    );

    private static final ObjectParser.NamedObjectParser<ClusterIndexHealth, Void> INDEX_PARSER = (
        XContentParser parser,
        Void context,
        String index) -> ClusterIndexHealthTests.parseInstance(parser, index);

    static {
        // ClusterStateHealth fields
        PARSER.declareInt(constructorArg(), new ParseField(ClusterHealthResponse.NUMBER_OF_NODES));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterHealthResponse.NUMBER_OF_DATA_NODES));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterHealthResponse.ACTIVE_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterHealthResponse.RELOCATING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterHealthResponse.ACTIVE_PRIMARY_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterHealthResponse.INITIALIZING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterHealthResponse.UNASSIGNED_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterHealthResponse.UNASSIGNED_PRIMARY_SHARDS));
        PARSER.declareDouble(constructorArg(), new ParseField(ClusterHealthResponse.ACTIVE_SHARDS_PERCENT_AS_NUMBER));
        PARSER.declareString(constructorArg(), new ParseField(ClusterHealthResponse.STATUS));
        // Can be absent if LEVEL == 'cluster'
        PARSER.declareNamedObjects(optionalConstructorArg(), INDEX_PARSER, new ParseField(ClusterHealthResponse.INDICES));

        // ClusterHealthResponse fields
        PARSER.declareString(constructorArg(), new ParseField(ClusterHealthResponse.CLUSTER_NAME));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterHealthResponse.NUMBER_OF_PENDING_TASKS));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterHealthResponse.NUMBER_OF_IN_FLIGHT_FETCH));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterHealthResponse.DELAYED_UNASSIGNED_SHARDS));
        PARSER.declareLong(constructorArg(), new ParseField(ClusterHealthResponse.TASK_MAX_WAIT_TIME_IN_QUEUE_IN_MILLIS));
        PARSER.declareBoolean(constructorArg(), new ParseField(ClusterHealthResponse.TIMED_OUT));
    }

    private final ClusterStatsLevel level = randomFrom(ClusterStatsLevel.values());

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
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).build();
        int pendingTasks = randomIntBetween(0, 200);
        int inFlight = randomIntBetween(0, 200);
        int delayedUnassigned = randomIntBetween(0, 200);
        TimeValue pendingTaskInQueueTime = TimeValue.timeValueMillis(randomIntBetween(1000, 100000));
        ClusterHealthResponse clusterHealth = new ClusterHealthResponse(
            "bla",
            new String[] { Metadata.ALL },
            clusterState,
            clusterState.metadata().getProject().id(),
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
        return PARSER.apply(parser, null);
    }

    @Override
    protected ClusterHealthResponse createTestInstance() {
        int indicesSize = randomInt(20);
        Map<String, ClusterIndexHealth> indices = Maps.newMapWithExpectedSize(indicesSize);
        if (level == ClusterStatsLevel.INDICES || level == ClusterStatsLevel.SHARDS) {
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
        String clusterName = instance.getClusterName();
        int numberOfPendingTasks = instance.getNumberOfPendingTasks();
        int numberOfInFlightFetch = instance.getNumberOfInFlightFetch();
        int delayedUnassignedShards = instance.getDelayedUnassignedShards();
        TimeValue taskMaxWaitingTime = instance.getTaskMaxWaitingTime();
        boolean timedOut = instance.isTimedOut();
        ClusterStateHealth clusterStateHealth = instance.getClusterStateHealth();

        switch (randomIntBetween(0, 6)) {
            case 0 -> clusterName += randomAlphaOfLengthBetween(2, 5);
            case 1 -> numberOfPendingTasks += between(1, 10);
            case 2 -> numberOfInFlightFetch += between(1, 10);
            case 3 -> delayedUnassignedShards += between(1, 10);
            case 4 -> taskMaxWaitingTime = new TimeValue(instance.getTaskMaxWaitingTime().millis() + between(1, 10));
            case 5 -> timedOut = timedOut ? false : true;
            case 6 -> clusterStateHealth = new ClusterStateHealth(
                clusterStateHealth.getActivePrimaryShards() + between(1, 10),
                clusterStateHealth.getActiveShards(),
                clusterStateHealth.getRelocatingShards(),
                clusterStateHealth.getInitializingShards(),
                clusterStateHealth.getUnassignedShards(),
                clusterStateHealth.getUnassignedPrimaryShards(),
                clusterStateHealth.getNumberOfNodes(),
                clusterStateHealth.getNumberOfDataNodes(),
                clusterStateHealth.getActiveShardsPercent(),
                clusterStateHealth.getStatus(),
                clusterStateHealth.getIndices()
            );
            default -> throw new UnsupportedOperationException();
        }

        return new ClusterHealthResponse(
            clusterName,
            numberOfPendingTasks,
            numberOfInFlightFetch,
            delayedUnassignedShards,
            taskMaxWaitingTime,
            timedOut,
            clusterStateHealth
        );
    }

}
