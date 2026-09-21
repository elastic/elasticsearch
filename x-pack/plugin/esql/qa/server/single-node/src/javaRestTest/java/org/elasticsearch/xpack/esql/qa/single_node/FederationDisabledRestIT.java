/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.datasource.DataSourceService;
import org.junit.ClassRule;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * End-to-end REST coverage for a node whose operator unregistered federation
 * ({@code -Des.esql.register_federation_feature=false}), the always-off deployment shape. The property is read once at
 * node startup, so this needs a dedicated cluster with it set on the node JVM (see the {@code @ClassRule}), and that
 * cluster carries no federation setting: an unregistered feature does not accept its own settings, which
 * {@link FederationSettingRejectedWhenUnregisteredRestIT} covers.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FederationDisabledRestIT extends AbstractFederationUnavailableRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.clusterWithFederationUnregistered(
        spec -> spec.systemProperty(Federation.REGISTER_PROPERTY, "false")
    );

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    /**
     * The unregistered feature owns no settings, so the cluster settings API rejects them the same way it rejects a
     * misspelled key. This is the counterpart of the accepted update in {@link FederationNotEnabledRestIT}: merely leaving
     * the feature disabled keeps its settings configurable, unregistering it takes them away.
     */
    public void testFederationSettingsAreRejectedOverRest() throws IOException {
        assertRejected(ExternalSourceSettings.FEDERATED_IDENTITY_ENABLED.getKey(), "true");
        assertRejected(DataSourceService.MAX_DATA_SOURCES_COUNT_SETTING.getKey(), "7");
    }

    private static void assertRejected(String key, String value) throws IOException {
        Request update = new Request("PUT", "/_cluster/settings");
        update.setJsonEntity(Strings.format("""
            {"persistent": {"%s": %s}}""", key, value));
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(update));
        assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(e.getMessage(), containsString("persistent setting [" + key + "], not recognized"));
    }
}
