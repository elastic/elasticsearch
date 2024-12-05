/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip.direct;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.ingest.geoip.IngestGeoIpPlugin;
import org.elasticsearch.ingest.geoip.direct.DatabaseConfiguration.Ipinfo;
import org.elasticsearch.ingest.geoip.direct.DatabaseConfiguration.Local;
import org.elasticsearch.ingest.geoip.direct.DatabaseConfiguration.Maxmind;
import org.elasticsearch.ingest.geoip.direct.DatabaseConfiguration.Web;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Set;

import static org.elasticsearch.ingest.geoip.direct.DatabaseConfiguration.IPINFO_NAMES;
import static org.elasticsearch.ingest.geoip.direct.DatabaseConfiguration.MAXMIND_NAMES;

public class DatabaseConfigurationTests extends AbstractXContentSerializingTestCase<DatabaseConfiguration> {

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new IngestGeoIpPlugin().getNamedWriteables());
    }

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
        boolean useIpinfo = randomBoolean();
        DatabaseConfiguration.Provider provider = switch (between(0, 2)) {
            case 0 -> useIpinfo ? new Ipinfo() : new Maxmind(randomAlphaOfLength(5));
            case 1 -> new Web();
            case 2 -> new Local(randomAlphaOfLength(10));
            default -> throw new AssertionError("failure, got illegal switch case");
        };
        return new DatabaseConfiguration(id, useIpinfo ? randomFrom(IPINFO_NAMES) : randomFrom(MAXMIND_NAMES), provider);
    }

    @Override
    protected DatabaseConfiguration mutateInstance(DatabaseConfiguration instance) {
        switch (between(0, 2)) {
            case 0:
                return new DatabaseConfiguration(instance.id() + randomAlphaOfLength(2), instance.name(), instance.provider());
            case 1:
                return new DatabaseConfiguration(
                    instance.id(),
                    randomValueOtherThan(
                        instance.name(),
                        () -> instance.provider() instanceof Ipinfo ? randomFrom(IPINFO_NAMES) : randomFrom(MAXMIND_NAMES)
                    ),
                    instance.provider()
                );
            case 2:
                DatabaseConfiguration.Provider provider = instance.provider();
                DatabaseConfiguration.Provider modifiedProvider = switch (provider) {
                    case Maxmind maxmind -> new Maxmind(maxmind.accountId() + randomAlphaOfLength(2));
                    case Ipinfo ignored -> new Local(randomAlphaOfLength(20)); // can't modify Ipinfo
                    case Web ignored -> new Local(randomAlphaOfLength(20)); // can't modify a Web
                    case Local local -> new Local(local.type() + randomAlphaOfLength(2));
                    default -> throw new AssertionError("Unexpected provider type: " + provider.getClass());
                };
                return new DatabaseConfiguration(instance.id(), instance.name(), modifiedProvider);
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
