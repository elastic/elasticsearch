/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip.direct;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.ingest.geoip.direct.GetDatabaseConfigurationAction.NodeResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class TransportGetDatabaseConfigurationActionTests extends ESTestCase {
    public void testDeduplicateNodeResponses() {
        {
            List<NodeResponse> nodeResponses = new ArrayList<>();
            Set<String> preExistingNames = Set.of();
            Collection<DatabaseConfigurationMetadata> deduplicated = TransportGetDatabaseConfigurationAction.deduplicateNodeResponses(
                nodeResponses,
                preExistingNames
            );
            assertTrue(deduplicated.isEmpty());
        }
        {
            List<NodeResponse> nodeResponses = List.of(
                generateTestNodeResponse(List.of()),
                generateTestNodeResponse(List.of()),
                generateTestNodeResponse(List.of())
            );
            Set<String> preExistingNames = Set.of();
            Collection<DatabaseConfigurationMetadata> deduplicated = TransportGetDatabaseConfigurationAction.deduplicateNodeResponses(
                nodeResponses,
                preExistingNames
            );
            assertTrue(deduplicated.isEmpty());
        }
        {
            // 3 nodes with 3 overlapping responses. We expect the deduplicated collection to include 1, 2, 3, and 4.
            List<NodeResponse> nodeResponses = List.of(
                generateTestNodeResponse(List.of("1", "2", "3")),
                generateTestNodeResponse(List.of("1", "2", "3")),
                generateTestNodeResponse(List.of("1", "4"))
            );
            Set<String> preExistingNames = Set.of();
            Collection<DatabaseConfigurationMetadata> deduplicated = TransportGetDatabaseConfigurationAction.deduplicateNodeResponses(
                nodeResponses,
                preExistingNames
            );
            assertThat(deduplicated.size(), equalTo(4));
            assertThat(
                deduplicated.stream().map(database -> database.database().name()).collect(Collectors.toSet()),
                equalTo(Set.of("1", "2", "3", "4"))
            );
        }
        {
            /*
             * 3 nodes with 3 overlapping responses, but this time we're also passing in a set of pre-existing names that overlap with
             * two of them. So we expect the deduplicated collection to include 1 and 4.
             */
            List<NodeResponse> nodeResponses = List.of(
                generateTestNodeResponse(List.of("1", "2", "3")),
                generateTestNodeResponse(List.of("1", "2", "3")),
                generateTestNodeResponse(List.of("1", "4"))
            );
            Set<String> preExistingNames = Set.of("2", "3", "5");
            Collection<DatabaseConfigurationMetadata> deduplicated = TransportGetDatabaseConfigurationAction.deduplicateNodeResponses(
                nodeResponses,
                preExistingNames
            );
            assertThat(deduplicated.size(), equalTo(2));
            assertThat(
                deduplicated.stream().map(database -> database.database().name()).collect(Collectors.toSet()),
                equalTo(Set.of("1", "4"))
            );
        }
        {
            /*
             * Here 3 nodes report the same database, but with different modified dates and versions. We expect the one with the highest
             * modified date to win out.
             */
            List<NodeResponse> nodeResponses = List.of(
                generateTestNodeResponseFromDatabases(List.of(generateTestDatabase("1", 1))),
                generateTestNodeResponseFromDatabases(List.of(generateTestDatabase("1", 1000))),
                generateTestNodeResponseFromDatabases(List.of(generateTestDatabase("1", 3)))
            );
            Set<String> preExistingNames = Set.of("2", "3", "5");
            Collection<DatabaseConfigurationMetadata> deduplicated = TransportGetDatabaseConfigurationAction.deduplicateNodeResponses(
                nodeResponses,
                preExistingNames
            );
            assertThat(deduplicated.size(), equalTo(1));
            DatabaseConfigurationMetadata result = deduplicated.iterator().next();
            assertThat(result, equalTo(nodeResponses.get(1).getDatabases().get(0)));
        }
    }

    private NodeResponse generateTestNodeResponse(List<String> databaseNames) {
        List<DatabaseConfigurationMetadata> databases = databaseNames.stream().map(this::generateTestDatabase).toList();
        return generateTestNodeResponseFromDatabases(databases);
    }

    private NodeResponse generateTestNodeResponseFromDatabases(List<DatabaseConfigurationMetadata> databases) {
        DiscoveryNode discoveryNode = mock(DiscoveryNode.class);
        return new NodeResponse(discoveryNode, databases);
    }

    private DatabaseConfigurationMetadata generateTestDatabase(String databaseName) {
        return generateTestDatabase(databaseName, randomLongBetween(0, Long.MAX_VALUE));
    }

    private DatabaseConfigurationMetadata generateTestDatabase(String databaseName, long modifiedDate) {
        DatabaseConfiguration databaseConfiguration = new DatabaseConfiguration(
            randomAlphaOfLength(50),
            databaseName,
            new DatabaseConfiguration.Local(randomAlphaOfLength(20))
        );
        return new DatabaseConfigurationMetadata(databaseConfiguration, randomLongBetween(0, Long.MAX_VALUE), modifiedDate);
    }
}
