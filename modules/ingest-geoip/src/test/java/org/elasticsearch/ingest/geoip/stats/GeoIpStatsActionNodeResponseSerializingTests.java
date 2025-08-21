/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip.stats;

import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.Set;

public class GeoIpStatsActionNodeResponseSerializingTests extends AbstractWireSerializingTestCase<GeoIpStatsAction.NodeResponse> {

    @Override
    protected Writeable.Reader<GeoIpStatsAction.NodeResponse> instanceReader() {
        return GeoIpStatsAction.NodeResponse::new;
    }

    @Override
    protected GeoIpStatsAction.NodeResponse createTestInstance() {
        return createRandomInstance();
    }

    @Override
    protected GeoIpStatsAction.NodeResponse mutateInstance(GeoIpStatsAction.NodeResponse instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    static GeoIpStatsAction.NodeResponse createRandomInstance() {
        DiscoveryNode node = DiscoveryNodeUtils.create("id");
        Set<String> databases = Set.copyOf(randomList(10, () -> randomAlphaOfLengthBetween(5, 10)));
        Set<String> files = Set.copyOf(randomList(10, () -> randomAlphaOfLengthBetween(5, 10)));
        Set<String> configDatabases = Set.copyOf(randomList(10, () -> randomAlphaOfLengthBetween(5, 10)));
        return new GeoIpStatsAction.NodeResponse(
            node,
            GeoIpDownloaderStatsSerializingTests.createRandomInstance(),
            randomBoolean() ? null : CacheStatsSerializingTests.createRandomInstance(),
            databases,
            files,
            configDatabases
        );
    }
}
