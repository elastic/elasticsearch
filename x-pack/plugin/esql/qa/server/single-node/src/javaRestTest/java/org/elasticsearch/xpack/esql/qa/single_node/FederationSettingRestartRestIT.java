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

import java.util.concurrent.atomic.AtomicReference;

/**
 * The user lever of {@link AbstractFederationRestartRestTestCase}: {@code esql.federation.enabled} is set back to
 * {@code false} and the node bounced, which is how a user opts out again after opting in.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FederationSettingRestartRestIT extends AbstractFederationRestartRestTestCase {

    private static final AtomicReference<String> federationEnabled = new AtomicReference<>("true");

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(
        spec -> spec.setting(Federation.FEDERATION_ENABLED.getKey(), federationEnabled::get)
    );

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    @Override
    protected void turnFederationOff() {
        federationEnabled.set("false");
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
