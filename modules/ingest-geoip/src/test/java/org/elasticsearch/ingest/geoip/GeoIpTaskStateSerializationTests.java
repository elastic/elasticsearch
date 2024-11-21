/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;

public class GeoIpTaskStateSerializationTests extends AbstractXContentSerializingTestCase<GeoIpTaskState> {
    @Override
    protected GeoIpTaskState doParseInstance(XContentParser parser) throws IOException {
        return GeoIpTaskState.fromXContent(parser);
    }


    public void testStuff() throws IOException {
        HttpURLConnection connection = createConnection("http://invalid.endpoint");
        connection.getResponseCode();
    }

    private static HttpURLConnection createConnection(final String url) throws IOException {
        final HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(10000);
        conn.setDoOutput(false);
        conn.setInstanceFollowRedirects(false);
        return conn;
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
