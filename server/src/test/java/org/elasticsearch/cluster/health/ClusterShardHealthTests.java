/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.health;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Predicate;

import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;

public class ClusterShardHealthTests extends AbstractXContentSerializingTestCase<ClusterShardHealth> {

    public static final ConstructingObjectParser<ClusterShardHealth, Integer> PARSER = new ConstructingObjectParser<>(
        "cluster_shard_health",
        true,
        (parsedObjects, shardId) -> {
            int i = 0;
            boolean primaryActive = (boolean) parsedObjects[i++];
            int activeShards = (int) parsedObjects[i++];
            int relocatingShards = (int) parsedObjects[i++];
            int initializingShards = (int) parsedObjects[i++];
            int unassignedShards = (int) parsedObjects[i++];
            String statusStr = (String) parsedObjects[i];
            ClusterHealthStatus status = ClusterHealthStatus.fromString(statusStr);
            return new ClusterShardHealth(
                shardId,
                status,
                activeShards,
                relocatingShards,
                initializingShards,
                unassignedShards,
                primaryActive
            );
        }
    );

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField(ClusterShardHealth.PRIMARY_ACTIVE));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterShardHealth.ACTIVE_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterShardHealth.RELOCATING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterShardHealth.INITIALIZING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterShardHealth.UNASSIGNED_SHARDS));
        PARSER.declareString(constructorArg(), new ParseField(ClusterShardHealth.STATUS));
    }

    @Override
    protected ClusterShardHealth doParseInstance(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        String shardIdStr = parser.currentName();
        ClusterShardHealth parsed = PARSER.apply(parser, Integer.valueOf(shardIdStr));
        ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return parsed;
    }

    @Override
    protected ClusterShardHealth createTestInstance() {
        return randomShardHealth(randomInt(1000));
    }

    static ClusterShardHealth randomShardHealth(int id) {
        return new ClusterShardHealth(
            id,
            randomFrom(ClusterHealthStatus.values()),
            randomInt(1000),
            randomInt(1000),
            randomInt(1000),
            randomInt(1000),
            randomBoolean()
        );
    }

    @Override
    protected Writeable.Reader<ClusterShardHealth> instanceReader() {
        return ClusterShardHealth::new;
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // don't inject random fields at the root, which contains arbitrary shard ids
        return ""::equals;
    }

    @Override
    protected ClusterShardHealth mutateInstance(final ClusterShardHealth instance) {
        String mutate = randomFrom(
            "shardId",
            "status",
            "activeShards",
            "relocatingShards",
            "initializingShards",
            "unassignedShards",
            "primaryActive"
        );
        switch (mutate) {
            case "shardId":
                return new ClusterShardHealth(
                    instance.getShardId() + between(1, 10),
                    instance.getStatus(),
                    instance.getActiveShards(),
                    instance.getRelocatingShards(),
                    instance.getInitializingShards(),
                    instance.getUnassignedShards(),
                    instance.isPrimaryActive()
                );
            case "status":
                ClusterHealthStatus status = randomFrom(
                    Arrays.stream(ClusterHealthStatus.values()).filter(value -> value.equals(instance.getStatus()) == false).toList()
                );
                return new ClusterShardHealth(
                    instance.getShardId(),
                    status,
                    instance.getActiveShards(),
                    instance.getRelocatingShards(),
                    instance.getInitializingShards(),
                    instance.getUnassignedShards(),
                    instance.isPrimaryActive()
                );
            case "activeShards":
                return new ClusterShardHealth(
                    instance.getShardId(),
                    instance.getStatus(),
                    instance.getActiveShards() + between(1, 10),
                    instance.getRelocatingShards(),
                    instance.getInitializingShards(),
                    instance.getUnassignedShards(),
                    instance.isPrimaryActive()
                );
            case "relocatingShards":
                return new ClusterShardHealth(
                    instance.getShardId(),
                    instance.getStatus(),
                    instance.getActiveShards(),
                    instance.getRelocatingShards() + between(1, 10),
                    instance.getInitializingShards(),
                    instance.getUnassignedShards(),
                    instance.isPrimaryActive()
                );
            case "initializingShards":
                return new ClusterShardHealth(
                    instance.getShardId(),
                    instance.getStatus(),
                    instance.getActiveShards(),
                    instance.getRelocatingShards(),
                    instance.getInitializingShards() + between(1, 10),
                    instance.getUnassignedShards(),
                    instance.isPrimaryActive()
                );
            case "unassignedShards":
                return new ClusterShardHealth(
                    instance.getShardId(),
                    instance.getStatus(),
                    instance.getActiveShards(),
                    instance.getRelocatingShards(),
                    instance.getInitializingShards(),
                    instance.getUnassignedShards() + between(1, 10),
                    instance.isPrimaryActive()
                );
            case "primaryActive":
                return new ClusterShardHealth(
                    instance.getShardId(),
                    instance.getStatus(),
                    instance.getActiveShards(),
                    instance.getRelocatingShards(),
                    instance.getInitializingShards(),
                    instance.getUnassignedShards(),
                    instance.isPrimaryActive() == false
                );
            default:
                throw new UnsupportedOperationException();
        }
    }
}
