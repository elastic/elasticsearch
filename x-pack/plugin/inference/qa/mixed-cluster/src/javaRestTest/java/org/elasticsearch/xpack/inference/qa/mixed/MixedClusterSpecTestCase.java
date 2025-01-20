/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.qa.mixed;

import org.elasticsearch.Version;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.TestFeatureService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;

public abstract class MixedClusterSpecTestCase extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = MixedClustersSpec.mixedVersionCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    static final Version bwcVersion = Version.fromString(System.getProperty("tests.old_cluster_version"));

    private static TestFeatureService oldClusterTestFeatureService = null;

    @Before
    public void extractOldClusterFeatures() {
        if (oldClusterTestFeatureService == null) {
            oldClusterTestFeatureService = testFeatureService;
        }
    }

    protected static boolean oldClusterHasFeature(String featureId) {
        assert oldClusterTestFeatureService != null;
        return oldClusterTestFeatureService.clusterHasFeature(featureId);
    }

    protected static boolean oldClusterHasFeature(NodeFeature feature) {
        return oldClusterHasFeature(feature.id());
    }

    @AfterClass
    public static void cleanUp() {
        oldClusterTestFeatureService = null;
    }

}
