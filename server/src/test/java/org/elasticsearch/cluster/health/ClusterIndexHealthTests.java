/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.cluster.health;

import org.elasticsearch.action.ClusterStatsLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTableGenerator;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Predicate;
import java.util.regex.Pattern;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;
import static org.hamcrest.CoreMatchers.equalTo;

public class ClusterIndexHealthTests extends AbstractXContentSerializingTestCase<ClusterIndexHealth> {
    private final ClusterStatsLevel level = randomFrom(ClusterStatsLevel.SHARDS, ClusterStatsLevel.INDICES);

    public void testClusterIndexHealth() {
        RoutingTableGenerator routingTableGenerator = new RoutingTableGenerator();
        int numberOfShards = randomInt(3) + 1;
        int numberOfReplicas = randomInt(4);
        IndexMetadata indexMetadata = IndexMetadata.builder("test1")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(numberOfShards)
            .numberOfReplicas(numberOfReplicas)
            .build();
        RoutingTableGenerator.ShardCounter counter = new RoutingTableGenerator.ShardCounter();
        IndexRoutingTable indexRoutingTable = routingTableGenerator.genIndexRoutingTable(indexMetadata, counter);

        ClusterIndexHealth indexHealth = new ClusterIndexHealth(indexMetadata, indexRoutingTable);
        assertIndexHealth(indexHealth, counter, indexMetadata);
    }

    private void assertIndexHealth(
        ClusterIndexHealth indexHealth,
        RoutingTableGenerator.ShardCounter counter,
        IndexMetadata indexMetadata
    ) {
        assertThat(indexHealth.getStatus(), equalTo(counter.status()));
        assertThat(indexHealth.getNumberOfShards(), equalTo(indexMetadata.getNumberOfShards()));
        assertThat(indexHealth.getNumberOfReplicas(), equalTo(indexMetadata.getNumberOfReplicas()));
        assertThat(indexHealth.getActiveShards(), equalTo(counter.active));
        assertThat(indexHealth.getRelocatingShards(), equalTo(counter.relocating));
        assertThat(indexHealth.getInitializingShards(), equalTo(counter.initializing));
        assertThat(indexHealth.getUnassignedShards(), equalTo(counter.unassigned));
        assertThat(indexHealth.getUnassignedPrimaryShards(), equalTo(counter.unassignedPrimary));
        assertThat(indexHealth.getShards().size(), equalTo(indexMetadata.getNumberOfShards()));
        int totalShards = 0;
        for (ClusterShardHealth shardHealth : indexHealth.getShards().values()) {
            totalShards += shardHealth.getActiveShards() + shardHealth.getInitializingShards() + shardHealth.getUnassignedShards();
        }

        assertThat(totalShards, equalTo(indexMetadata.getNumberOfShards() * (1 + indexMetadata.getNumberOfReplicas())));
    }

    @Override
    protected ClusterIndexHealth createTestInstance() {
        return randomIndexHealth(randomAlphaOfLengthBetween(1, 10), level);
    }

    public static ClusterIndexHealth randomIndexHealth(String indexName, ClusterStatsLevel level) {
        Map<Integer, ClusterShardHealth> shards = new HashMap<>();
        if (level == ClusterStatsLevel.SHARDS) {
            for (int i = 0; i < randomInt(5); i++) {
                shards.put(i, ClusterShardHealthTests.randomShardHealth(i));
            }
        }
        return new ClusterIndexHealth(
            indexName,
            randomInt(1000),
            randomInt(1000),
            randomInt(1000),
            randomInt(1000),
            randomInt(1000),
            randomInt(1000),
            randomInt(1000),
            randomInt(1000),
            randomFrom(ClusterHealthStatus.values()),
            shards
        );
    }

    @Override
    protected Writeable.Reader<ClusterIndexHealth> instanceReader() {
        return ClusterIndexHealth::new;
    }

    @Override
    protected ClusterIndexHealth doParseInstance(XContentParser parser) throws IOException {
        ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        XContentParser.Token token = parser.nextToken();
        ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser);
        String index = parser.currentName();
        ClusterIndexHealth parsed = parseInstance(parser, index);
        ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        return parsed;
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
    protected ClusterIndexHealth mutateInstance(ClusterIndexHealth instance) {
        String index = instance.getIndex();
        int numberOfShards = instance.getNumberOfShards();
        int numberOfReplicas = instance.getNumberOfReplicas();
        int activeShards = instance.getActiveShards();
        int relocatingShards = instance.getRelocatingShards();
        int initializingShards = instance.getInitializingShards();
        int unassignedShards = instance.getUnassignedShards();
        int unassignedPrimaryShards = instance.getUnassignedPrimaryShards();
        int activePrimaryShards = instance.getActivePrimaryShards();
        ClusterHealthStatus status = instance.getStatus();
        Map<Integer, ClusterShardHealth> shards = new HashMap<>(instance.getShards());

        switch (randomIntBetween(0, 10)) {
            case 0 -> index += randomAlphaOfLengthBetween(2, 5);
            case 1 -> numberOfShards += between(1, 10);
            case 2 -> numberOfReplicas += between(1, 10);
            case 3 -> activeShards += between(1, 10);
            case 4 -> relocatingShards += between(1, 10);
            case 5 -> initializingShards += between(1, 10);
            case 6 -> unassignedShards += between(1, 10);
            case 7 -> unassignedPrimaryShards += between(1, 10);
            case 8 -> activePrimaryShards += between(1, 10);
            case 9 -> status = randomValueOtherThan(instance.getStatus(), () -> randomFrom(ClusterHealthStatus.values()));
            case 10 -> {
                if (instance.getShards().isEmpty()) {
                    shards = Map.of(0, ClusterShardHealthTests.randomShardHealth(0));
                } else {
                    shards.remove(shards.keySet().iterator().next());
                }
            }
            default -> throw new UnsupportedOperationException();
        }

        return new ClusterIndexHealth(
            index,
            numberOfShards,
            numberOfReplicas,
            activeShards,
            relocatingShards,
            initializingShards,
            unassignedShards,
            unassignedPrimaryShards,
            activePrimaryShards,
            status,
            shards
        );
    }

    private static final ConstructingObjectParser<ClusterIndexHealth, String> PARSER = new ConstructingObjectParser<>(
        "cluster_index_health",
        true,
        (parsedObjects, index) -> {
            int i = 0;
            int numberOfShards = (int) parsedObjects[i++];
            int numberOfReplicas = (int) parsedObjects[i++];
            int activeShards = (int) parsedObjects[i++];
            int relocatingShards = (int) parsedObjects[i++];
            int initializingShards = (int) parsedObjects[i++];
            int unassignedShards = (int) parsedObjects[i++];
            int unassignedPrimaryShards = (int) parsedObjects[i++];
            int activePrimaryShards = (int) parsedObjects[i++];
            String statusStr = (String) parsedObjects[i++];
            ClusterHealthStatus status = ClusterHealthStatus.fromString(statusStr);
            @SuppressWarnings("unchecked")
            List<ClusterShardHealth> shardList = (List<ClusterShardHealth>) parsedObjects[i];
            final Map<Integer, ClusterShardHealth> shards;
            if (shardList == null || shardList.isEmpty()) {
                shards = emptyMap();
            } else {
                shards = Maps.newMapWithExpectedSize(shardList.size());
                for (ClusterShardHealth shardHealth : shardList) {
                    shards.put(shardHealth.getShardId(), shardHealth);
                }
            }
            return new ClusterIndexHealth(
                index,
                numberOfShards,
                numberOfReplicas,
                activeShards,
                relocatingShards,
                initializingShards,
                unassignedShards,
                unassignedPrimaryShards,
                activePrimaryShards,
                status,
                shards
            );
        }
    );

    public static final ObjectParser.NamedObjectParser<ClusterShardHealth, String> SHARD_PARSER = (
        XContentParser p,
        String indexIgnored,
        String shardId) -> ClusterShardHealthTests.PARSER.apply(p, Integer.valueOf(shardId));

    static {
        PARSER.declareInt(constructorArg(), new ParseField(ClusterIndexHealth.NUMBER_OF_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterIndexHealth.NUMBER_OF_REPLICAS));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterIndexHealth.ACTIVE_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterIndexHealth.RELOCATING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterIndexHealth.INITIALIZING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterIndexHealth.UNASSIGNED_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterIndexHealth.UNASSIGNED_PRIMARY_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(ClusterIndexHealth.ACTIVE_PRIMARY_SHARDS));
        PARSER.declareString(constructorArg(), new ParseField(ClusterIndexHealth.STATUS));
        // Can be absent if LEVEL == 'indices' or 'cluster'
        PARSER.declareNamedObjects(optionalConstructorArg(), SHARD_PARSER, new ParseField(ClusterIndexHealth.SHARDS));
    }

    public static ClusterIndexHealth parseInstance(XContentParser parser, String index) {
        return PARSER.apply(parser, index);
    }
}
