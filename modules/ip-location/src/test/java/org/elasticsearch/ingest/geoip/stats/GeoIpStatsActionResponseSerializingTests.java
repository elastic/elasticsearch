/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.geoip.stats;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

import java.util.List;

public class GeoIpStatsActionResponseSerializingTests extends AbstractWireSerializingTestCase<GeoIpStatsAction.Response> {

    @Override
    protected Writeable.Reader<GeoIpStatsAction.Response> instanceReader() {
        return GeoIpStatsAction.Response::new;
    }

    @Override
    protected GeoIpStatsAction.Response createTestInstance() {
        List<GeoIpStatsAction.NodeResponse> nodeResponses = randomList(
            10,
            GeoIpStatsActionNodeResponseSerializingTests::createRandomInstance
        );
        return new GeoIpStatsAction.Response(ClusterName.DEFAULT, nodeResponses, List.of());
    }

    @Override
    protected GeoIpStatsAction.Response mutateInstance(GeoIpStatsAction.Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }
}
