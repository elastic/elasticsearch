/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class EnterpriseGeoIpTaskStateSerializationTests extends AbstractXContentSerializingTestCase<GeoIpTaskState> {
    @Override
    protected GeoIpTaskState doParseInstance(XContentParser parser) throws IOException {
        return GeoIpTaskState.fromXContent(parser);
    }

    @Override
    protected Writeable.Reader<GeoIpTaskState> instanceReader() {
        return GeoIpTaskState::new;
    }

    @Override
    protected GeoIpTaskState createTestInstance() {
        GeoIpTaskState state = GeoIpTaskState.EMPTY;
        int databaseCount = randomInt(20);
        for (int i = 0; i < databaseCount; i++) {
            state = state.put(randomAlphaOfLengthBetween(5, 10), createRandomMetadata());
        }
        return state;
    }

    @Override
    protected GeoIpTaskState mutateInstance(GeoIpTaskState instance) {
        Map<String, GeoIpTaskState.Metadata> databases = new HashMap<>(instance.getDatabases());
        switch (between(0, 2)) {
            case 0:
                String databaseName = randomValueOtherThanMany(databases::containsKey, () -> randomAlphaOfLengthBetween(5, 10));
                databases.put(databaseName, createRandomMetadata());
                return new GeoIpTaskState(databases);
            case 1:
                if (databases.size() > 0) {
                    String randomDatabaseName = databases.keySet().iterator().next();
                    databases.put(randomDatabaseName, createRandomMetadata());
                } else {
                    databases.put(randomAlphaOfLengthBetween(5, 10), createRandomMetadata());
                }
                return new GeoIpTaskState(databases);
            case 2:
                if (databases.size() > 0) {
                    String randomDatabaseName = databases.keySet().iterator().next();
                    databases.remove(randomDatabaseName);
                } else {
                    databases.put(randomAlphaOfLengthBetween(5, 10), createRandomMetadata());
                }
                return new GeoIpTaskState(databases);
            default:
                throw new AssertionError("failure, got illegal switch case");
        }
    }

    private GeoIpTaskState.Metadata createRandomMetadata() {
        return new GeoIpTaskState.Metadata(randomLong(), randomInt(), randomInt(), randomAlphaOfLength(32), randomLong());
    }
}
