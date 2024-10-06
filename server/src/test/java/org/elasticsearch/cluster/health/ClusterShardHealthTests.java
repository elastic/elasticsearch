/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.health;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
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
            int unassignedPrimaryShards = (int) parsedObjects[i++];
            String statusStr = (String) parsedObjects[i];
            ClusterHealthStatus status = ClusterHealthStatus.fromString(statusStr);
            return new ClusterShardHealth(
                shardId,
                status,
                activeShards,
                relocatingShards,
                initializingShards,
                unassignedShards,
                unassignedPrimaryShards,
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
        PARSER.declareInt(constructorArg(), new ParseField(ClusterShardHealth.UNASSIGNED_PRIMARY_SHARDS));
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
    protected ClusterShardHealth mutateInstance(ClusterShardHealth instance) {
        int shardId = instance.getShardId();
        ClusterHealthStatus status = instance.getStatus();
        int activeShards = instance.getActiveShards();
        int relocatingShards = instance.getRelocatingShards();
        int initializingShards = instance.getInitializingShards();
        int unassignedShards = instance.getUnassignedShards();
        int unassignedPrimaryShards = instance.getUnassignedPrimaryShards();
        boolean primaryActive = instance.isPrimaryActive();

        switch (randomIntBetween(0, 7)) {
            case 0 -> shardId += between(1, 10);
            case 1 -> status = randomValueOtherThan(status, () -> randomFrom(ClusterHealthStatus.values()));
            case 2 -> activeShards += between(1, 10);
            case 3 -> relocatingShards += between(1, 10);
            case 4 -> initializingShards += between(1, 10);
            case 5 -> unassignedShards += between(1, 10);
            case 6 -> unassignedPrimaryShards += between(1, 10);
            case 7 -> primaryActive = primaryActive ? false : true;
            default -> throw new UnsupportedOperationException();
        }

        return new ClusterShardHealth(
            shardId,
            status,
            activeShards,
            relocatingShards,
            initializingShards,
            unassignedShards,
            unassignedPrimaryShards,
            primaryActive
        );
    }
}
