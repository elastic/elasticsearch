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

public class GeoIpTaskStateSerializationTests extends AbstractXContentSerializingTestCase<GeoIpTaskState> {
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
            GeoIpTaskState.Metadata metadata = new GeoIpTaskState.Metadata(
                randomLong(),
                randomInt(),
                randomInt(),
                randomAlphaOfLength(32),
                randomLong()
            );
            state = state.put(randomAlphaOfLengthBetween(5, 10), metadata);
        }
        return state;
    }

    @Override
    protected GeoIpTaskState mutateInstance(GeoIpTaskState instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
