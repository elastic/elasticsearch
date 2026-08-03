/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.common.io.Streams;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.is;

/**
 * A node whose operator unregistered federation does not merely ignore {@link Federation#FEDERATION_ENABLED}, it
 * refuses to start with it: an unregistered feature registers none of its settings, so the key is unknown and settings
 * validation rejects it exactly as it rejects a misspelled one.
 *
 * <p>The node boots with the feature registered and the setting on, then only the system property flips and the node is
 * restarted, which is the upgrade-shaped path into the combination: an operator adds the kill switch to a deployment
 * that had opted in. The restart must fail, and the reason must name the setting in the node log rather than leaving an
 * operator to guess.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FederationSettingRejectedWhenUnregisteredRestIT extends ESRestTestCase {

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
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        // The cluster is dead once the restart below fails, so its state cannot be wiped over REST.
        return true;
    }

    public void testNodeDoesNotStartWithTheSettingWhenFeatureIsUnregistered() throws IOException {
        registerFederationFeature.set("false");
        expectThrows(
            Exception.class,
            "the node came up with ["
                + Federation.FEDERATION_ENABLED.getKey()
                + "] configured while the federation feature was unregistered, so the setting is still being registered "
                + "and settings validation accepted configuration for a feature that is not there",
            () -> cluster.restart(false)
        );

        AtomicBoolean found = new AtomicBoolean(false);
        for (int i = 0; i < cluster.getNumNodes(); i++) {
            try (InputStream log = cluster.getNodeLog(i, LogType.SERVER)) {
                Streams.readAllLines(log, line -> {
                    if (line.contains("unknown setting [" + Federation.FEDERATION_ENABLED.getKey() + "]")) {
                        found.set(true);
                    }
                });
            }
        }
        assertThat(
            "the node refused to start, but its log does not name ["
                + Federation.FEDERATION_ENABLED.getKey()
                + "] as an unknown setting, so it failed for some other reason",
            found.get(),
            is(true)
        );
    }
}
