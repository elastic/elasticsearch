/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.Build;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.S3FixtureUtils;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;

/**
 * The user lever of {@link AbstractFederationRestartRestTestCase}: {@code esql.federation.enabled} is set to
 * {@code false} and the node bounced, which is how a user opts out again after opting in, and then set to {@code true}
 * and bounced again.
 *
 * <p>The first phase takes the default rather than pinning the setting, so that a dataset is actually created and read
 * on a default-configured node; {@link FederationBuildDefaultRestIT} only proves the default registers the routes. That
 * needs the default to be on, which it is on every stateful node and in every snapshot build, and the suite reads the
 * node type back to skip itself where it is not (a stateless release build); the round trip is still covered there by
 * {@link FederationKillSwitchRestartRestIT}, which pins the setting. The other two phases are explicit in both directions.
 *
 * <p>The endpoint allowlist is set here rather than left to the shared cluster builder, which also adds it in this
 * repository but is substituted elsewhere with a builder that only defines the node shape.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FederationSettingRestartRestIT extends AbstractFederationRestartRestTestCase {

    /** Carries {@code esql.federation.enabled} only once a phase has set it; the first phase leaves it to the default. */
    private static final MutableSettingsProvider federationEnabled = new MutableSettingsProvider();

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(
        spec -> spec.settings(federationEnabled)
            .setting(S3FixtureUtils.ALLOWED_ENDPOINT_HOSTS_SETTING, S3FixtureUtils.LOOPBACK_ENDPOINT_HOSTS)
    );

    @Before
    public void assumeDefaultIsOn() throws IOException {
        assumeTrue(
            "the first phase takes the default, which is off on a stateless release build",
            Federation.defaultEnabled(FederationNodeSettings.isStateless(client()), Build.current().isSnapshot())
        );
    }

    @Override
    protected ElasticsearchCluster cluster() {
        return cluster;
    }

    @Override
    protected void setFederation(boolean on) {
        federationEnabled.put(Federation.FEDERATION_ENABLED.getKey(), Boolean.toString(on));
    }

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }
}
