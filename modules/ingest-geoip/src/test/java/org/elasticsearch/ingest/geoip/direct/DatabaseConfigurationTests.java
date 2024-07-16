/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.direct;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.geoip.direct.DatabaseConfiguration.Maxmind;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.ingest.geoip.direct.DatabaseConfiguration.MAXMIND_NAMES;

public class DatabaseConfigurationTests extends AbstractXContentSerializingTestCase<DatabaseConfiguration> {

    private String id;

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
        return new DatabaseConfiguration(id, randomFrom(MAXMIND_NAMES), new Maxmind(randomAlphaOfLength(5)));
    }

    @Override
    protected DatabaseConfiguration mutateInstance(DatabaseConfiguration instance) {
        switch (between(0, 2)) {
            case 0:
                return new DatabaseConfiguration(instance.id() + randomAlphaOfLength(2), instance.name(), instance.maxmind());
            case 1:
                return new DatabaseConfiguration(
                    instance.id(),
                    randomValueOtherThan(instance.name(), () -> randomFrom(MAXMIND_NAMES)),
                    instance.maxmind()
                );
            case 2:
                return new DatabaseConfiguration(
                    instance.id(),
                    instance.name(),
                    new Maxmind(instance.maxmind().accountId() + randomAlphaOfLength(2))
                );
            default:
                throw new AssertionError("failure, got illegal switch case");
        }
    }

    @Override
    protected Writeable.Reader<DatabaseConfiguration> instanceReader() {
        return DatabaseConfiguration::new;
    }

    public void testValidateId() {
        Set<String> invalidIds = Set.of("-foo", "_foo", "foo,bar", "foo bar", "foo*bar", "foo.bar");
        for (String id : invalidIds) {
            expectThrows(IllegalArgumentException.class, "expected exception for " + id, () -> DatabaseConfiguration.validateId(id));
        }
        Set<String> validIds = Set.of("f-oo", "f_oo", "foobar");
        for (String id : validIds) {
            DatabaseConfiguration.validateId(id);
        }
        // Note: the code checks for byte length, but randomAlphoOfLength is only using characters in the ascii subset
        String longId = randomAlphaOfLength(128);
        expectThrows(IllegalArgumentException.class, "expected exception for " + longId, () -> DatabaseConfiguration.validateId(longId));
        String longestAllowedId = randomAlphaOfLength(127);
        DatabaseConfiguration.validateId(longestAllowedId);
        String shortId = randomAlphaOfLengthBetween(1, 127);
        DatabaseConfiguration.validateId(shortId);
        expectThrows(IllegalArgumentException.class, "expected exception for empty string", () -> DatabaseConfiguration.validateId(""));
        expectThrows(IllegalArgumentException.class, "expected exception for null string", () -> DatabaseConfiguration.validateId(null));
    }
}
