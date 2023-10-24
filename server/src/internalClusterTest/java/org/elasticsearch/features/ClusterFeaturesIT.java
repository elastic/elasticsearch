/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.features;

import org.elasticsearch.test.ESIntegTestCase;

import static org.hamcrest.Matchers.hasItem;

public class ClusterFeaturesIT extends ESIntegTestCase {

    public void testClusterHasFeatures() {
        internalCluster().startNodes(2);
        ensureGreen();

        FeatureService service = internalCluster().getCurrentMasterNodeInstance(FeatureService.class);

        assertThat(service.getNodeFeatures(), hasItem("features_supported"));
    }
}
