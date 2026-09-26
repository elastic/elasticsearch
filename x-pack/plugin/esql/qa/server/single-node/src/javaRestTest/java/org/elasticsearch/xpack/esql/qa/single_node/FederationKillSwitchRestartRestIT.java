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
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.ClassRule;

import java.util.concurrent.atomic.AtomicReference;

/**
 * The operator lever of {@link AbstractFederationRestartRestTestCase}: federation is unregistered with
 * {@code -Des.esql.register_federation_feature=false} and the node bounced.
 *
 * <p>Turning the lever off drops {@link Federation#FEDERATION_ENABLED} from the node's yml along with it, so the
 * restarted node is the shape an operator actually deploys. An unregistered feature registers no settings, so a node
 * that still carried the key would not start at all, which is what
 * {@link FederationSettingRejectedWhenUnregisteredRestIT} covers.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FederationKillSwitchRestartRestIT extends AbstractFederationRestartRestTestCase {

    private static final AtomicReference<String> registerFederationFeature = new AtomicReference<>("true");
    private static final MutableSettingsProvider federationSettings = new MutableSettingsProvider();
    static {
        federationSettings.put(Federation.FEDERATION_ENABLED.getKey(), "true");
    }

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.clusterWithFederationUnregistered(
        spec -> spec.systemProperty(Federation.REGISTER_PROPERTY, registerFederationFeature::get).settings(federationSettings)
    );

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    @Override
    protected void turnFederationOff() {
        registerFederationFeature.set("false");
        federationSettings.remove(Federation.FEDERATION_ENABLED.getKey());
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
