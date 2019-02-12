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

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTableGenerator;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;

public class ClusterIndexHealthTests extends AbstractSerializingTestCase<ClusterIndexHealth> {
    private final ClusterHealthRequest.Level level = randomFrom(ClusterHealthRequest.Level.SHARDS, ClusterHealthRequest.Level.INDICES);

    public void testClusterIndexHealth() {
        RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
        int numberOfShards = randomInt(3) + 1;
        int numberOfReplicas = randomInt(4);
        IndexMetaData indexMetaData = IndexMetaData.builder("test1").settings(settings(Version.CURRENT))
                .numberOfShards(numberOfShards).numberOfReplicas(numberOfReplicas).build();
        RoutingTableGenerator.ShardCounter counter = new RoutingTableGenerator.ShardCounter();
        IndexRoutingTable indexRoutingTable = routingTableGenerator.genIndexRoutingTable(indexMetaData, counter);

        ClusterIndexHealth indexHealth = new ClusterIndexHealth(indexMetaData, indexRoutingTable);
        assertIndexHealth(indexHealth, counter, indexMetaData);
    }

    private void assertIndexHealth(ClusterIndexHealth indexHealth, RoutingTableGenerator.ShardCounter counter,
                                   IndexMetaData indexMetaData) {
        assertThat(indexHealth.getStatus(), equalTo(counter.status()));
        assertThat(indexHealth.getNumberOfShards(), equalTo(indexMetaData.getNumberOfShards()));
        assertThat(indexHealth.getNumberOfReplicas(), equalTo(indexMetaData.getNumberOfReplicas()));
        assertThat(indexHealth.getActiveShards(), equalTo(counter.active));
        assertThat(indexHealth.getRelocatingShards(), equalTo(counter.relocating));
        assertThat(indexHealth.getInitializingShards(), equalTo(counter.initializing));
        assertThat(indexHealth.getUnassignedShards(), equalTo(counter.unassigned));
        assertThat(indexHealth.getShards().size(), equalTo(indexMetaData.getNumberOfShards()));
        int totalShards = 0;
        for (ClusterShardHealth shardHealth : indexHealth.getShards().values()) {
            totalShards += shardHealth.getActiveShards() + shardHealth.getInitializingShards() + shardHealth.getUnassignedShards();
        }

        assertThat(totalShards, equalTo(indexMetaData.getNumberOfShards() * (1 + indexMetaData.getNumberOfReplicas())));
    }

    @Override
    protected ClusterIndexHealth createTestInstance() {
        return randomIndexHealth(randomAlphaOfLengthBetween(1, 10), level);
    }

    public static ClusterIndexHealth randomIndexHealth(String indexName, ClusterHealthRequest.Level level) {
        Map<Integer, ClusterShardHealth> shards = new HashMap<>();
        if (level == ClusterHealthRequest.Level.SHARDS) {
            for (int i = 0; i < randomInt(5); i++) {
                shards.put(i, ClusterShardHealthTests.randomShardHealth(i));
            }
        }
        return new ClusterIndexHealth(indexName, randomInt(1000), randomInt(1000), randomInt(1000), randomInt(1000),
                randomInt(1000), randomInt(1000), randomInt(1000), randomFrom(ClusterHealthStatus.values()), shards);
    }

    @Override
    protected Writeable.Reader<ClusterIndexHealth> instanceReader() {
        return ClusterIndexHealth::new;
    }

    @Override
    protected ClusterIndexHealth doParseInstance(XContentParser parser) throws IOException {
        return ClusterIndexHealth.fromXContent(parser);
    }

    @Override
    protected ToXContent.Params getToXContentParams() {
        return new ToXContent.MapParams(Collections.singletonMap("level", level.name().toLowerCase(Locale.ROOT)));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    // Ignore all paths which looks like "RANDOMINDEXNAME.shards"
    private static final Pattern SHARDS_IN_XCONTENT = Pattern.compile("^\\w+\\.shards$");

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        return field -> "".equals(field) || SHARDS_IN_XCONTENT.matcher(field).find();
    }
    @Override
    protected ClusterIndexHealth mutateInstance(ClusterIndexHealth instance) throws IOException {
        String mutate = randomFrom("index", "numberOfShards", "numberOfReplicas", "activeShards", "relocatingShards",
                "initializingShards", "unassignedShards", "activePrimaryShards", "status", "shards");
        switch (mutate) {
            case "index":
                return new ClusterIndexHealth(instance.getIndex() + randomAlphaOfLengthBetween(2, 5), instance.getNumberOfShards(),
                        instance.getNumberOfReplicas(), instance.getActiveShards(), instance.getRelocatingShards(),
                        instance.getInitializingShards(), instance.getUnassignedShards(),
                        instance.getActivePrimaryShards(), instance.getStatus(), instance.getShards());
            case "numberOfShards":
                return new ClusterIndexHealth(instance.getIndex(), instance.getNumberOfShards() + between(1, 10),
                        instance.getNumberOfReplicas(), instance.getActiveShards(), instance.getRelocatingShards(),
                        instance.getInitializingShards(), instance.getUnassignedShards(),
                        instance.getActivePrimaryShards(), instance.getStatus(), instance.getShards());
            case "numberOfReplicas":
                return new ClusterIndexHealth(instance.getIndex(), instance.getNumberOfShards(),
                        instance.getNumberOfReplicas() + between(1, 10), instance.getActiveShards(),
                        instance.getRelocatingShards(), instance.getInitializingShards(), instance.getUnassignedShards(),
                        instance.getActivePrimaryShards(), instance.getStatus(), instance.getShards());
            case "activeShards":
                return new ClusterIndexHealth(instance.getIndex(), instance.getNumberOfShards(),
                        instance.getNumberOfReplicas(), instance.getActiveShards() + between(1, 10),
                        instance.getRelocatingShards(), instance.getInitializingShards(), instance.getUnassignedShards(),
                        instance.getActivePrimaryShards(), instance.getStatus(), instance.getShards());
            case "relocatingShards":
                return new ClusterIndexHealth(instance.getIndex(), instance.getNumberOfShards(),
                        instance.getNumberOfReplicas(), instance.getActiveShards(),
                        instance.getRelocatingShards() + between(1, 10), instance.getInitializingShards(),
                        instance.getUnassignedShards(), instance.getActivePrimaryShards(), instance.getStatus(), instance.getShards());
            case "initializingShards":
                return new ClusterIndexHealth(instance.getIndex(), instance.getNumberOfShards(),
                        instance.getNumberOfReplicas(), instance.getActiveShards(), instance.getRelocatingShards(),
                        instance.getInitializingShards() + between(1, 10), instance.getUnassignedShards(),
                        instance.getActivePrimaryShards(), instance.getStatus(), instance.getShards());
            case "unassignedShards":
                return new ClusterIndexHealth(instance.getIndex(), instance.getNumberOfShards(),
                        instance.getNumberOfReplicas(), instance.getActiveShards(), instance.getRelocatingShards(),
                        instance.getInitializingShards(), instance.getUnassignedShards() + between(1, 10),
                        instance.getActivePrimaryShards(), instance.getStatus(), instance.getShards());
            case "activePrimaryShards":
                return new ClusterIndexHealth(instance.getIndex(), instance.getNumberOfShards(),
                        instance.getNumberOfReplicas(), instance.getActiveShards(), instance.getRelocatingShards(),
                        instance.getInitializingShards(), instance.getUnassignedShards(),
                        instance.getActivePrimaryShards() + between(1, 10), instance.getStatus(), instance.getShards());
            case "status":
                ClusterHealthStatus status = randomFrom(
                    Arrays.stream(ClusterHealthStatus.values()).filter(
                        value -> !value.equals(instance.getStatus())
                    ).collect(Collectors.toList())
                );
                return new ClusterIndexHealth(instance.getIndex(), instance.getNumberOfShards(),
                        instance.getNumberOfReplicas(), instance.getActiveShards(), instance.getRelocatingShards(),
                        instance.getInitializingShards(), instance.getUnassignedShards(),
                        instance.getActivePrimaryShards(), status, instance.getShards());
            case "shards":
                Map<Integer, ClusterShardHealth> map;
                if (instance.getShards().isEmpty()) {
                    map = Collections.singletonMap(0, ClusterShardHealthTests.randomShardHealth(0));
                } else {
                    map = new HashMap<>(instance.getShards());
                    map.remove(map.keySet().iterator().next());
                }
                return new ClusterIndexHealth(instance.getIndex(), instance.getNumberOfShards(),
                        instance.getNumberOfReplicas(), instance.getActiveShards(), instance.getRelocatingShards(),
                        instance.getInitializingShards(), instance.getUnassignedShards(),
                        instance.getActivePrimaryShards(), instance.getStatus(), map);
            default:
                throw new UnsupportedOperationException();
        }
    }
}
