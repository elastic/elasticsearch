/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xpack.esql.datasources.ExternalSourceSettings;
import org.elasticsearch.xpack.esql.datasources.Federation;
import org.elasticsearch.xpack.esql.datasources.datasource.DataSourceService;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;

/**
 * End-to-end REST coverage for the registered-but-not-enabled deployment shape: the feature is registered, but the
 * setting is off, so it is unavailable and looks exactly like the operator-unregistered case in
 * {@link FederationDisabledRestIT}. The shared cluster builder turns the setting on for the other suites in this
 * project, so this cluster sets it back to {@code false}.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FederationNotEnabledRestIT extends AbstractFederationUnavailableRestTestCase {

    private static final String FEDERATED_IDENTITY = ExternalSourceSettings.FEDERATED_IDENTITY_ENABLED.getKey();
    private static final String MAX_DISCOVERED_FILES = ExternalSourceSettings.MAX_DISCOVERED_FILES.getKey();
    private static final String MAX_DATA_SOURCES = DataSourceService.MAX_DATA_SOURCES_COUNT_SETTING.getKey();

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(
        spec -> spec.setting(Federation.FEDERATION_ENABLED.getKey(), "false")
    );

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    /**
     * Only availability is off here, not registration, so the rest of the federation settings still exist and the cluster
     * settings API accepts them. A deployment can therefore configure the feature before turning it on, or without ever
     * turning it on. This assertion belongs to this suite rather than the shared base because the same request must be
     * rejected on the operator-unregistered node of {@link FederationDisabledRestIT}, where those settings do not exist.
     */
    public void testFederationSettingsAreStillUpdatableOverRest() throws IOException {
        Request update = new Request("PUT", "/_cluster/settings");
        update.setJsonEntity(Strings.format("""
            {"persistent": {"%s": true, "%s": 42, "%s": 7}}""", FEDERATED_IDENTITY, MAX_DISCOVERED_FILES, MAX_DATA_SOURCES));
        Response response = client().performRequest(update);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(200));

        Request get = new Request("GET", "/_cluster/settings");
        get.addParameter("flat_settings", "true");
        @SuppressWarnings("unchecked")
        Map<String, Object> persistent = (Map<String, Object>) entityAsMap(client().performRequest(get)).get("persistent");
        assertThat(persistent, hasEntry(FEDERATED_IDENTITY, "true"));
        assertThat(persistent, hasEntry(MAX_DISCOVERED_FILES, "42"));
        assertThat(persistent, hasEntry(MAX_DATA_SOURCES, "7"));
    }
}
