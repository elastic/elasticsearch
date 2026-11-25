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
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;
import org.junit.ClassRule;

import java.util.concurrent.atomic.AtomicReference;

/**
 * The operator lever of {@link AbstractFederationRestartRestTestCase}: federation is unregistered with
 * {@code -Des.esql.register_federation_feature=false} and the node bounced, then registered again and bounced again.
 *
 * <p>Turning the lever off drops {@link Federation#FEDERATION_ENABLED} and the endpoint allowlist from the node's yml
 * along with it, so the restarted node is the shape an operator actually deploys. An unregistered feature registers no
 * settings, so a node that still carried either key would not start at all, which is what
 * {@link FederationSettingRejectedWhenUnregisteredRestIT} covers. Turning it back on restores both keys.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FederationKillSwitchRestartRestIT extends AbstractFederationRestartRestTestCase {

    private static final AtomicReference<String> registerFederationFeature = new AtomicReference<>("true");
    private static final MutableSettingsProvider federationSettings = new MutableSettingsProvider();
    static {
        putFederationSettings();
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
    protected void setFederation(boolean on) {
        registerFederationFeature.set(Boolean.toString(on));
        if (on) {
            putFederationSettings();
        } else {
            federationSettings.remove(Federation.FEDERATION_ENABLED.getKey());
            federationSettings.remove(S3FixtureUtils.ALLOWED_ENDPOINT_HOSTS_SETTING);
        }
    }

    /** The settings a registered node needs for this flow, none of which an unregistered node accepts. */
    private static void putFederationSettings() {
        federationSettings.put(Federation.FEDERATION_ENABLED.getKey(), "true");
        federationSettings.put(S3FixtureUtils.ALLOWED_ENDPOINT_HOSTS_SETTING, S3FixtureUtils.LOOPBACK_ENDPOINT_HOSTS);
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
