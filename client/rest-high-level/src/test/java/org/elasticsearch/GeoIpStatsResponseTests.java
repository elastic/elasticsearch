/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch;

import org.elasticsearch.client.GeoIpStatsResponse;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

public class GeoIpStatsResponseTests extends AbstractXContentTestCase<GeoIpStatsResponse> {

    @Override
    protected GeoIpStatsResponse createTestInstance() {
        HashMap<String, GeoIpStatsResponse.NodeInfo> nodes = new HashMap<>();
        int nodeCount = randomInt(10);
        for (int i = 0; i < nodeCount; i++) {
            List<GeoIpStatsResponse.DatabaseInfo> databases = randomList(5,
                () -> new GeoIpStatsResponse.DatabaseInfo(randomAlphaOfLength(5)));
            nodes.put(randomAlphaOfLength(5), new GeoIpStatsResponse.NodeInfo(randomList(5, () -> randomAlphaOfLength(5)),
                databases.stream().collect(Collectors.toMap(GeoIpStatsResponse.DatabaseInfo::getName, d -> d))));
        }
        return new GeoIpStatsResponse(randomInt(), randomInt(), randomNonNegativeLong(), randomInt(), randomInt(), nodes);
    }

    @Override
    protected GeoIpStatsResponse doParseInstance(XContentParser parser) throws IOException {
        return GeoIpStatsResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
