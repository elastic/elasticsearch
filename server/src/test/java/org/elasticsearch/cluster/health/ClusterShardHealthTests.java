/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster.health;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.function.Predicate;
import java.util.stream.Collectors;

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
        return new ClusterShardHealth(id, randomFrom(ClusterHealthStatus.values()),  randomInt(1000), randomInt(1000),
            randomInt(1000), randomInt(1000), randomBoolean());
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
        //don't inject random fields at the root, which contains arbitrary shard ids
        return ""::equals;
    }

    @Override
    protected ClusterShardHealth mutateInstance(final ClusterShardHealth instance) {
        String mutate = randomFrom("shardId", "status", "activeShards", "relocatingShards", "initializingShards",
                "unassignedShards", "primaryActive");
        switch (mutate) {
            case "shardId":
                return new ClusterShardHealth(instance.getShardId() + between(1, 10), instance.getStatus(),
                        instance.getActiveShards(), instance.getRelocatingShards(),
                        instance.getInitializingShards(), instance.getUnassignedShards(),
                        instance.isPrimaryActive());
            case "status":
                ClusterHealthStatus status = randomFrom(
                    Arrays.stream(ClusterHealthStatus.values()).filter(
                        value -> !value.equals(instance.getStatus())
                    ).collect(Collectors.toList())
                );
                return new ClusterShardHealth(instance.getShardId(), status,
                        instance.getActiveShards(), instance.getRelocatingShards(),
                        instance.getInitializingShards(), instance.getUnassignedShards(),
                        instance.isPrimaryActive());
            case "activeShards":
                return new ClusterShardHealth(instance.getShardId(), instance.getStatus(),
                        instance.getActiveShards() + between(1, 10), instance.getRelocatingShards(),
                        instance.getInitializingShards(), instance.getUnassignedShards(),
                        instance.isPrimaryActive());
            case "relocatingShards":
                return new ClusterShardHealth(instance.getShardId(), instance.getStatus(),
                        instance.getActiveShards(), instance.getRelocatingShards() + between(1, 10),
                        instance.getInitializingShards(), instance.getUnassignedShards(), instance.isPrimaryActive());
            case "initializingShards":
                return new ClusterShardHealth(instance.getShardId(), instance.getStatus(),
                        instance.getActiveShards(), instance.getRelocatingShards(),
                        instance.getInitializingShards() + between(1, 10), instance.getUnassignedShards(),
                        instance.isPrimaryActive());
            case "unassignedShards":
                return new ClusterShardHealth(instance.getShardId(), instance.getStatus(),
                        instance.getActiveShards(), instance.getRelocatingShards(),
                        instance.getInitializingShards(), instance.getUnassignedShards() + between(1, 10),
                        instance.isPrimaryActive());
            case "primaryActive":
                return new ClusterShardHealth(instance.getShardId(), instance.getStatus(),
                        instance.getActiveShards(), instance.getRelocatingShards(),
                        instance.getInitializingShards(), instance.getUnassignedShards(),
                        instance.isPrimaryActive() == false);
            default:
                throw new UnsupportedOperationException();
        }
    }
}
