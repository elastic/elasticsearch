/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.geoip.direct.DatabaseConfiguration;
import org.elasticsearch.ingest.geoip.direct.DatabaseConfigurationMetadata;
import org.elasticsearch.test.AbstractChunkedSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IngestGeoIpMetadataTests extends AbstractChunkedSerializingTestCase<IngestGeoIpMetadata> {
    @Override
    protected IngestGeoIpMetadata doParseInstance(XContentParser parser) throws IOException {
        return IngestGeoIpMetadata.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<IngestGeoIpMetadata> instanceReader() {
        return IngestGeoIpMetadata::new;
    }

    @Override
    protected IngestGeoIpMetadata createTestInstance() {
        return randomIngestGeoIpMetadata();
    }

    @Override
    protected IngestGeoIpMetadata mutateInstance(IngestGeoIpMetadata instance) throws IOException {
        Map<String, DatabaseConfigurationMetadata> databases = new HashMap<>(instance.getDatabases());
        switch (between(0, 2)) {
            case 0 -> {
                String databaseId = randomValueOtherThanMany(databases::containsKey, ESTestCase::randomIdentifier);
                databases.put(databaseId, randomDatabaseConfigurationMetadata(databaseId));
                return new IngestGeoIpMetadata(databases);
            }
            case 1 -> {
                if (databases.size() > 0) {
                    String randomDatabaseId = databases.keySet().iterator().next();
                    databases.put(randomDatabaseId, randomDatabaseConfigurationMetadata(randomDatabaseId));
                } else {
                    String databaseId = randomIdentifier();
                    databases.put(databaseId, randomDatabaseConfigurationMetadata(databaseId));
                }
                return new IngestGeoIpMetadata(databases);
            }
            case 2 -> {
                if (databases.size() > 0) {
                    String randomDatabaseId = databases.keySet().iterator().next();
                    databases.remove(randomDatabaseId);
                } else {
                    String databaseId = randomIdentifier();
                    databases.put(databaseId, randomDatabaseConfigurationMetadata(databaseId));
                }
                return new IngestGeoIpMetadata(databases);
            }
            default -> throw new AssertionError("failure, got illegal switch case");
        }
    }

    private IngestGeoIpMetadata randomIngestGeoIpMetadata() {
        Map<String, DatabaseConfigurationMetadata> databases = new HashMap<>();
        for (int i = 0; i < randomIntBetween(0, 20); i++) {
            String databaseId = randomIdentifier();
            databases.put(databaseId, randomDatabaseConfigurationMetadata(databaseId));
        }
        return new IngestGeoIpMetadata(databases);
    }

    private DatabaseConfigurationMetadata randomDatabaseConfigurationMetadata(String id) {
        return new DatabaseConfigurationMetadata(
            randomDatabaseConfiguration(id),
            randomNonNegativeLong(),
            randomPositiveTimeValue().millis()
        );
    }

    private DatabaseConfiguration randomDatabaseConfiguration(String id) {
        return new DatabaseConfiguration(id, randomAlphaOfLength(10), new DatabaseConfiguration.Maxmind(randomAlphaOfLength(10)));
    }
}
