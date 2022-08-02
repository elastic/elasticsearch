/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.health;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Predicate;

public class ClusterShardHealthTests extends AbstractSerializingTestCase<ClusterShardHealth> {

    @Override
    protected ClusterShardHealth doParseInstance(XContentParser parser) throws IOException {
        return ClusterShardHealth.fromXContent(parser);
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
