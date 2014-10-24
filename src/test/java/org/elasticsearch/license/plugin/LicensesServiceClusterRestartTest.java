/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ClusterAdminClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.plugin.action.get.GetLicenseRequestBuilder;
import org.elasticsearch.license.plugin.action.get.GetLicenseResponse;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequestBuilder;
import org.elasticsearch.license.plugin.action.put.PutLicenseResponse;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Ignore;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.SUITE;
import static org.hamcrest.CoreMatchers.equalTo;

@ClusterScope(scope = SUITE, numDataNodes = 0)
public class LicensesServiceClusterRestartTest extends ElasticsearchIntegrationTest {

    static String priKeyPath;
    static String pubKeyPath;

    @Override
    protected Settings transportClientSettings() {
        // Plugin should be loaded on the transport client as well
        return settingsBuilder().build();
    }


    @Test @Ignore
    public void test() throws Exception {
        priKeyPath = Paths.get(LicensesServiceClusterRestartTest.class.getResource("/private.key").toURI()).toAbsolutePath().toString();
        pubKeyPath = Paths.get(LicensesServiceClusterRestartTest.class.getResource("/public.key").toURI()).toAbsolutePath().toString();

        logger.info("--> starting 1 nodes");
        String node1 = internalCluster().startNode(settingsBuilder());

        ensureGreen();
        final List<ESLicense> esLicenses = putLicense(node1);
        final Client startNodeClient = internalCluster().startNodeClient(settingsBuilder().build());
        //TODO: just pass node name instead
        getAndCheckLicense(startNodeClient, esLicenses);

        logger.info("--> cluster state before full cluster restart");
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        logger.info("Cluster state: {}", clusterState);

        logger.info("--> restart all nodes");
        internalCluster().fullRestart();
        ensureYellow();

        logger.info("--> cluster state after full cluster restart");
        clusterState = client().admin().cluster().prepareState().get().getState();
        logger.info("Cluster state: {}", clusterState);

        getAndCheckLicense(internalCluster().startNodeClient(settingsBuilder().build()), esLicenses);
    }
    
    private List<ESLicense> putLicense(String node) throws Exception {

        final ClusterAdminClient cluster = internalCluster().client(node).admin().cluster();

        Map<String, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes featureAttributes =
                new TestUtils.FeatureAttributes("shield", "subscription", "platinum", "foo bar Inc.", "elasticsearch", 2, "2014-12-13", "2015-12-13");
        map.put(TestUtils.SHIELD, featureAttributes);
        String licenseString = TestUtils.generateESLicenses(map);
        String licenseOutput = TestUtils.runLicenseGenerationTool(licenseString, pubKeyPath, priKeyPath);

        PutLicenseRequestBuilder putLicenseRequestBuilder = new PutLicenseRequestBuilder(cluster);
        final List<ESLicense> putLicenses = ESLicenses.fromSource(licenseOutput);
        assertThat(putLicenses.size(), equalTo(1));
        putLicenseRequestBuilder.setLicense(putLicenses);
        ensureGreen();

        final ActionFuture<PutLicenseResponse> putLicenseFuture = putLicenseRequestBuilder.execute();

        final PutLicenseResponse putLicenseResponse = putLicenseFuture.get();
        assertTrue(putLicenseResponse.isAcknowledged());

        return putLicenses;
    }

    private void getAndCheckLicense(Client client, List<ESLicense> license) {
        final ClusterAdminClient cluster = client.admin().cluster();
        final GetLicenseResponse response = new GetLicenseRequestBuilder(cluster).get();
        assertThat(response.licenses().size(), equalTo(1));
        TestUtils.isSame(license, response.licenses());
    }

    private ImmutableSettings.Builder settingsBuilder() {
        return ImmutableSettings.settingsBuilder()
                .put("gateway.type", "local")
                .put("plugin.types", LicensePlugin.class.getName());
    }
}
