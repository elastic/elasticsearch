/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip.direct;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.ingest.geoip.IngestGeoIpMetadata;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.Map;

public class TransportPutDatabaseConfigurationActionTests extends ESTestCase {

    public void testValidatePrerequisites() {
        ProjectId projectId = randomProjectIdOrDefault();
        // Test that we reject two configurations with the same database name but different ids:
        String name = randomAlphaOfLengthBetween(1, 50);
        IngestGeoIpMetadata ingestGeoIpMetadata = randomIngestGeoIpMetadata(name);
        ClusterState state = ClusterState.builder(ClusterState.EMPTY_STATE)
            .putProjectMetadata(ProjectMetadata.builder(projectId)
                .putCustom(IngestGeoIpMetadata.TYPE, ingestGeoIpMetadata)
                .build())
            .build();
        DatabaseConfiguration databaseConfiguration = randomDatabaseConfiguration(randomIdentifier(), name);
        expectThrows(
            IllegalArgumentException.class,
            () -> TransportPutDatabaseConfigurationAction.validatePrerequisites(projectId, databaseConfiguration, state)
        );

        // Test that we do not reject two configurations with different database names:
        String differentName = randomValueOtherThan(name, () -> randomAlphaOfLengthBetween(1, 50));
        DatabaseConfiguration databaseConfigurationForDifferentName = randomDatabaseConfiguration(randomIdentifier(), differentName);
        TransportPutDatabaseConfigurationAction.validatePrerequisites(projectId, databaseConfigurationForDifferentName, state);

        // Test that we do not reject a configuration if none already exists:
        TransportPutDatabaseConfigurationAction.validatePrerequisites(projectId, databaseConfiguration, ClusterState.EMPTY_STATE);

        // Test that we do not reject a configuration if one with the same database name AND id already exists:
        DatabaseConfiguration databaseConfigurationSameNameSameId = ingestGeoIpMetadata.getDatabases()
            .values()
            .iterator()
            .next()
            .database();
        TransportPutDatabaseConfigurationAction.validatePrerequisites(projectId, databaseConfigurationSameNameSameId, state);
    }

    private IngestGeoIpMetadata randomIngestGeoIpMetadata(String name) {
        Map<String, DatabaseConfigurationMetadata> databases = new HashMap<>();
        String databaseId = randomIdentifier();
        databases.put(databaseId, randomDatabaseConfigurationMetadata(databaseId, name));
        return new IngestGeoIpMetadata(databases);
    }

    private DatabaseConfigurationMetadata randomDatabaseConfigurationMetadata(String id, String name) {
        return new DatabaseConfigurationMetadata(
            randomDatabaseConfiguration(id, name),
            randomNonNegativeLong(),
            randomPositiveTimeValue().millis()
        );
    }

    private DatabaseConfiguration randomDatabaseConfiguration(String id, String name) {
        return new DatabaseConfiguration(id, name, new DatabaseConfiguration.Maxmind(randomAlphaOfLength(10)));
    }
}
