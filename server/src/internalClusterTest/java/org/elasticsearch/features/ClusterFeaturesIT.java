/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.features;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class ClusterFeaturesIT extends ESIntegTestCase {

    public static class FeatureSpecPlugin extends Plugin implements FeatureSpecification {
        @Override
        public Set<NodeFeature> getFeatures() {
            return Set.of(new NodeFeature("f1"));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getMockPlugins());
        plugins.add(FeatureSpecPlugin.class);
        return plugins;
    }

    public void testClusterHasFeatures() {
        internalCluster().startNodes(2);
        ensureGreen();

        FeatureService service = internalCluster().getCurrentMasterNodeInstance(FeatureService.class);

        assertThat(service.getNodeFeatures(), containsInAnyOrder("f1"));
    }
}
