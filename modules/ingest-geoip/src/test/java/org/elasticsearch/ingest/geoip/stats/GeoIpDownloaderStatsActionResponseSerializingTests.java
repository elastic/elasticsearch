/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest.geoip.stats;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Collections;
import java.util.List;

public class GeoIpDownloaderStatsActionResponseSerializingTests extends AbstractWireSerializingTestCase<
    GeoIpDownloaderStatsAction.Response> {

    @Override
    protected Writeable.Reader<GeoIpDownloaderStatsAction.Response> instanceReader() {
        return GeoIpDownloaderStatsAction.Response::new;
    }

    @Override
    protected GeoIpDownloaderStatsAction.Response createTestInstance() {
        List<GeoIpDownloaderStatsAction.NodeResponse> nodeResponses = randomList(
            10,
            GeoIpDownloaderStatsActionNodeResponseSerializingTests::createRandomInstance
        );
        return new GeoIpDownloaderStatsAction.Response(ClusterName.DEFAULT, nodeResponses, Collections.emptyList());
    }
}
