/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.apmintegration;

import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestHandler;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class ApmIntegrationPlugin extends Plugin implements ActionPlugin {
    private final TestApmIntegrationRestHandler testApmIntegrationRestHandler = new TestApmIntegrationRestHandler();

    @Override
    public List<RestHandler> getRestHandlers(
        RestHandlersServices restHandlersServices,
        final Supplier<DiscoveryNodes> nodesInCluster,
        Predicate<NodeFeature> clusterSupportsFeature
    ) {
        return Collections.singletonList(testApmIntegrationRestHandler);
    }

    @Override
    public Collection<?> createComponents(PluginServices services) {
        TestMeterUsages testMeterUsages = new TestMeterUsages(services.telemetryProvider().getMeterRegistry());
        testApmIntegrationRestHandler.setTestMeterUsages(testMeterUsages);
        return super.createComponents(services);
    }
}
