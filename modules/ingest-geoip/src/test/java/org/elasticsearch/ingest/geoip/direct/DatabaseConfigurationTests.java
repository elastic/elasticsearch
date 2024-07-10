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
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.elasticsearch.ingest.geoip.direct.DatabaseConfiguration.MAXMIND_NAMES;

public class DatabaseConfigurationTests extends AbstractXContentSerializingTestCase<DatabaseConfiguration> {

    private String id;

    // TODO we also need to test the validation logic

    @Override
    protected DatabaseConfiguration doParseInstance(XContentParser parser) throws IOException {
        return DatabaseConfiguration.parse(parser, id);
    }

    @Override
    protected DatabaseConfiguration createTestInstance() {
        id = randomAlphaOfLength(5);
        return randomDatabaseConfiguration(id);
    }

    public static DatabaseConfiguration randomDatabaseConfiguration(String id) {
        return new DatabaseConfiguration(id, randomFrom(MAXMIND_NAMES));
    }

    @Override
    protected DatabaseConfiguration mutateInstance(DatabaseConfiguration instance) {
        switch (between(0, 1)) {
            case 0:
                return new DatabaseConfiguration(instance.id() + randomAlphaOfLength(2), instance.name());
            case 1:
                return new DatabaseConfiguration(instance.id(), randomValueOtherThan(instance.name(), () -> randomFrom(MAXMIND_NAMES)));
            default:
                throw new AssertionError("failure, got illegal switch case");
        }
    }

    @Override
    protected Writeable.Reader<DatabaseConfiguration> instanceReader() {
        return DatabaseConfiguration::new;
    }
}
