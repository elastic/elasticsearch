/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.direct;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.ingest.geoip.direct.DatabaseConfiguration.MAXMIND_NAMES;
import static org.elasticsearch.ingest.geoip.direct.DatabaseConfigurationTests.randomDatabaseConfiguration;

public class DatabaseConfigurationMetadataTests extends AbstractXContentSerializingTestCase<DatabaseConfigurationMetadata> {

    private String id;

    @Override
    protected DatabaseConfigurationMetadata doParseInstance(XContentParser parser) throws IOException {
        return DatabaseConfigurationMetadata.parse(parser, id);
    }

    @Override
    protected DatabaseConfigurationMetadata createTestInstance() {
        id = randomAlphaOfLength(5);
        return randomDatabaseConfigurationMetadata(id);
    }

    public static DatabaseConfigurationMetadata randomDatabaseConfigurationMetadata(String id) {
        return new DatabaseConfigurationMetadata(
            new DatabaseConfiguration(id, randomFrom(MAXMIND_NAMES), new DatabaseConfiguration.Maxmind(randomAlphaOfLength(5))),
            randomNonNegativeLong(),
            randomPositiveTimeValue().millis()
        );
    }

    @Override
    protected DatabaseConfigurationMetadata mutateInstance(DatabaseConfigurationMetadata instance) {
        switch (between(0, 2)) {
            case 0:
                return new DatabaseConfigurationMetadata(
                    randomValueOtherThan(instance.database(), () -> randomDatabaseConfiguration(randomAlphaOfLength(5))),
                    instance.version(),
                    instance.modifiedDate()
                );
            case 1:
                return new DatabaseConfigurationMetadata(
                    instance.database(),
                    randomValueOtherThan(instance.version(), ESTestCase::randomNonNegativeLong),
                    instance.modifiedDate()
                );
            case 2:
                return new DatabaseConfigurationMetadata(
                    instance.database(),
                    instance.version(),
                    randomValueOtherThan(instance.modifiedDate(), () -> ESTestCase.randomPositiveTimeValue().millis())
                );
            default:
                throw new AssertionError("failure, got illegal switch case");
        }
    }

    @Override
    protected Writeable.Reader<DatabaseConfigurationMetadata> instanceReader() {
        return DatabaseConfigurationMetadata::new;
    }
}
