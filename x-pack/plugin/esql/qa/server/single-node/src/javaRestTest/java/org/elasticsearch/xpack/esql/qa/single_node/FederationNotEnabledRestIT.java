/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.ClassRule;

/**
 * End-to-end REST coverage for the default deployment shape: the feature is registered, but nobody enabled it, so it is
 * unavailable and looks exactly like the operator-unregistered case in {@link FederationDisabledRestIT}. The shared
 * cluster builder turns the setting on for the other suites in this project, so this cluster turns it back off, which
 * is equivalent to leaving it out of {@code elasticsearch.yml}.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FederationNotEnabledRestIT extends AbstractFederationUnavailableRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(
        spec -> spec.setting(Federation.FEDERATION_ENABLED.getKey(), "false")
    );

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
