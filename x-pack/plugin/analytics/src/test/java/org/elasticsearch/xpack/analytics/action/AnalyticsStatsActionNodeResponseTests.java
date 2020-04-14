/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.action;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.analytics.action.AnalyticsStatsAction;

public class AnalyticsStatsActionNodeResponseTests extends AbstractWireSerializingTestCase<AnalyticsStatsAction.NodeResponse> {

    @Override
    protected Writeable.Reader<AnalyticsStatsAction.NodeResponse> instanceReader() {
        return AnalyticsStatsAction.NodeResponse::new;
    }

    @Override
    protected AnalyticsStatsAction.NodeResponse createTestInstance() {
        String nodeName = randomAlphaOfLength(10);
        DiscoveryNode node = new DiscoveryNode(nodeName, buildNewFakeTransportAddress(), Version.CURRENT);
        return new AnalyticsStatsAction.NodeResponse(node, randomLongBetween(0, 1000), randomLongBetween(0, 1000),
            randomLongBetween(0, 1000), randomLongBetween(0, 1000));
    }
}
