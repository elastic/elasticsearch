/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProcessedClusterStateUpdateTask;
import org.elasticsearch.cluster.ack.ClusterStateUpdateResponse;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.core.ESLicenses;
import org.elasticsearch.license.core.LicenseBuilders;
import org.elasticsearch.license.core.LicenseUtils;
import org.elasticsearch.license.manager.ESLicenseManager;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequest;
import org.elasticsearch.license.plugin.core.*;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.SUITE;

@ClusterScope(scope = SUITE, numDataNodes = 10)
public class LicensesServiceTests extends ElasticsearchIntegrationTest {


    private static String pubKeyPath = null;
    private static String priKeyPath = null;

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.settingsBuilder()
                .put("plugins.load_classpath_plugins", false)
                .put("plugin.types", LicensePlugin.class.getName())
                .build();
    }

    @Override
    protected Settings transportClientSettings() {
        // Plugin should be loaded on the transport client as well
        return nodeSettings(0);
    }

    @BeforeClass
    public static void setup() throws IOException, URISyntaxException {
        priKeyPath = Paths.get(LicenseTransportTests.class.getResource("/org.elasticsearch.license.plugin/test_pri.key").toURI()).toAbsolutePath().toString();
        pubKeyPath = Paths.get(LicenseTransportTests.class.getResource("/org.elasticsearch.license.plugin/test_pub.key").toURI()).toAbsolutePath().toString();
    }


    @After
    public void afterTest() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        masterClusterService().submitStateUpdateTask("delete licensing metadata", new ProcessedClusterStateUpdateTask() {
            @Override
            public void clusterStateProcessed(String source, ClusterState oldState, ClusterState newState) {
                latch.countDown();
            }

            @Override
            public ClusterState execute(ClusterState currentState) throws Exception {
                MetaData.Builder mdBuilder = MetaData.builder(currentState.metaData());
                mdBuilder.putCustom(LicensesMetaData.TYPE, null);
                return ClusterState.builder(currentState).metaData(mdBuilder).build();
            }

            @Override
            public void onFailure(String source, @Nullable Throwable t) {
                logger.error("error on metaData cleanup after test", t);
            }
        });
        latch.await();
        clear();
        masterClusterService().remove(licensesService());
        masterClusterService().add(licensesService());

    }

    @Test
    public void testEmptySignedLicenseCheck() {
        LicensesManagerService licensesManagerService = licensesManagerService();
        assertTrue(LicensesStatus.VALID == licensesManagerService.checkLicenses(LicenseBuilders.licensesBuilder().build()));
    }

    @Test
    public void testInvalidSignedLicenseCheck() throws Exception {
        LicensesManagerService licensesManagerService = licensesManagerService();

        Map<String, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes featureAttributes =
                new TestUtils.FeatureAttributes("shield", "subscription", "platinum", "foo bar Inc.", "elasticsearch", 2, "2014-12-13", "2015-12-13");
        map.put(TestUtils.SHIELD, featureAttributes);
        String licenseString = TestUtils.generateESLicenses(map);
        String licenseOutput = TestUtils.runLicenseGenerationTool(licenseString, pubKeyPath, priKeyPath);
        ESLicenses licenses = LicenseUtils.readLicensesFromString(licenseOutput);

        assertTrue(LicensesStatus.VALID == licensesManagerService.checkLicenses(licenses));

        ESLicenses.ESLicense tamperedLicense = LicenseBuilders.licenseBuilder(true)
                .fromLicense(licenses.get(TestUtils.SHIELD))
                .expiryDate(licenses.get(TestUtils.SHIELD).expiryDate() + 5 * 24 * 60 * 60 * 1000l)
                .issuer("elasticsearch")
                .build();

        ESLicenses tamperedLicenses = LicenseBuilders.licensesBuilder().license(tamperedLicense).build();

        assertTrue(LicensesStatus.INVALID == licensesManagerService.checkLicenses(tamperedLicenses));
    }

    @Test
    public void testStoringLicenses() throws Exception {
        Map<String, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes featureAttributes1 =
                new TestUtils.FeatureAttributes("shield", "subscription", "platinum", "foo bar Inc.", "elasticsearch", 2, "2014-12-13", "2015-12-13");
        map.put(TestUtils.SHIELD, featureAttributes1);
        String licenseString = TestUtils.generateESLicenses(map);
        String licenseOutput = TestUtils.runLicenseGenerationTool(licenseString, pubKeyPath, priKeyPath);
        ESLicenses licenses = LicenseUtils.readLicensesFromString(licenseOutput);

        LicensesManagerService licensesManagerService = licensesManagerService();
        ESLicenseManager esLicenseManager = ((LicensesService) licensesManagerService).getEsLicenseManager();
        final CountDownLatch latch1 = new CountDownLatch(1);
        licensesManagerService.registerLicenses(new LicensesService.PutLicenseRequestHolder(new PutLicenseRequest().license(licenses), "test"), new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                if (clusterStateUpdateResponse.isAcknowledged()) {
                    latch1.countDown();
                }
            }

            @Override
            public void onFailure(Throwable e) {

            }
        });

        latch1.await();
        LicensesMetaData metaData = clusterService().state().metaData().custom(LicensesMetaData.TYPE);
        ESLicenses metaDataLicense = esLicenseManager.fromSignatures(metaData.getSignatures());
        TestUtils.isSame(licenses, metaDataLicense);


        TestUtils.FeatureAttributes featureAttributes2 =
                new TestUtils.FeatureAttributes("shield", "subscription", "platinum", "foo bar Inc.", "elasticsearch", 2, "2014-12-13", "2016-12-13");
        map.put(TestUtils.SHIELD, featureAttributes2);
        licenseString = TestUtils.generateESLicenses(map);
        licenseOutput = TestUtils.runLicenseGenerationTool(licenseString, pubKeyPath, priKeyPath);
        ESLicenses licenses2 = LicenseUtils.readLicensesFromString(licenseOutput);
        final CountDownLatch latch2 = new CountDownLatch(1);
        licensesManagerService.registerLicenses(new LicensesService.PutLicenseRequestHolder(new PutLicenseRequest().license(licenses2), "test"), new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                if (clusterStateUpdateResponse.isAcknowledged()) {
                    latch2.countDown();
                }
            }

            @Override
            public void onFailure(Throwable e) {

            }
        });

        latch2.await();
        metaData = clusterService().state().metaData().custom(LicensesMetaData.TYPE);
        metaDataLicense = esLicenseManager.fromSignatures(metaData.getSignatures());
        TestUtils.isSame(licenses2, metaDataLicense);
    }

    @Test
    public void testTrialLicenseGeneration() throws Exception {
        LicensesClientService clientService = licensesClientService();
        final CountDownLatch latch = new CountDownLatch(1);
        clientService.register("shield", new LicensesService.TrialLicenseOptions(10, 100), new LicensesClientService.Listener() {
            @Override
            public void onEnabled() {
                latch.countDown();
            }

            @Override
            public void onDisabled() {
                fail();
            }
        });
        latch.await();
        final LicensesMetaData metaData = clusterService().state().metaData().custom(LicensesMetaData.TYPE);
        assertTrue(metaData.getEncodedTrialLicenses().size() == 1);
    }

    @Test
    public void testMultipleClientRegistration() {
    }

    private class TestLicenseClientListener implements LicensesClientService.Listener {

        AtomicBoolean shouldBeEnabled = new AtomicBoolean(false);
        AtomicBoolean processed = new AtomicBoolean(false);

        private TestLicenseClientListener(boolean shouldBeEnabled) {
            this.shouldBeEnabled.getAndSet(shouldBeEnabled);
        }

        private void reset() {
            processed.set(false);
        }

        @Override
        public void onEnabled() {
            if (this.shouldBeEnabled.get()) {
                processed.set(true);
            } else {
                fail("onEnabled should not have been called");
            }

        }

        @Override
        public void onDisabled() {
            if (!this.shouldBeEnabled.get()) {
                processed.set(true);
            } else {
                fail("onDisabled should not have been called");
            }
        }
    }

    @Test
    public void testClientValidation() throws Exception {
        // start with no trial license
        // feature should be onDisabled
        // then add signed license
        // feature should be onEnabled

        LicensesClientService clientService = licensesClientService();
        LicensesManagerService managerService = licensesManagerService();
        final TestLicenseClientListener testLicenseClientListener = new TestLicenseClientListener(false);
        clientService.register("shield", null, testLicenseClientListener);

        for (String enabledFeature : managerService.enabledFeatures()) {
            assertFalse(enabledFeature.equals("shield"));
        }
        logger.info("pass initial check");

        assertFalse(testLicenseClientListener.processed.get());
        testLicenseClientListener.shouldBeEnabled.set(true);

        Map<String, TestUtils.FeatureAttributes> map = new HashMap<>();
        TestUtils.FeatureAttributes featureAttributes1 =
                new TestUtils.FeatureAttributes("shield", "subscription", "platinum", "foo bar Inc.", "elasticsearch", 2, "2014-12-13", "2015-12-13");
        map.put(TestUtils.SHIELD, featureAttributes1);
        String licenseString = TestUtils.generateESLicenses(map);
        String licenseOutput = TestUtils.runLicenseGenerationTool(licenseString, pubKeyPath, priKeyPath);
        ESLicenses licenses = LicenseUtils.readLicensesFromString(licenseOutput);

        LicensesManagerService licensesManagerService = licensesManagerService();
        final CountDownLatch latch1 = new CountDownLatch(1);
        licensesManagerService.registerLicenses(new LicensesService.PutLicenseRequestHolder(new PutLicenseRequest().license(licenses), "test"), new ActionListener<ClusterStateUpdateResponse>() {
            @Override
            public void onResponse(ClusterStateUpdateResponse clusterStateUpdateResponse) {
                if (clusterStateUpdateResponse.isAcknowledged()) {
                    latch1.countDown();
                }
            }

            @Override
            public void onFailure(Throwable e) {

            }
        });

        latch1.await();

        logger.info("waiting for onEnabled");
        while (!testLicenseClientListener.processed.get()) {
        }

        Set<String> enabledFeatures = licensesManagerService.enabledFeatures();
        assertTrue(enabledFeatures.contains("shield"));

    }

    @Test
    public void testFeatureWithoutLicense() throws Exception {
        LicensesClientService clientService = licensesClientService();
        clientService.register("marvel", null, new LicensesClientService.Listener() {
            @Override
            public void onEnabled() {
                fail();
            }

            @Override
            public void onDisabled() {
            }
        });

        LicensesManagerService managerService = licensesManagerService();
        assertFalse("feature should not be enabled: no licenses registered", managerService.enabledFeatures().contains("marvel"));
    }

    @Test
    @Ignore
    public void testLicenseExpiry() throws Exception {
        //TODO, first figure out how to generate a license with a quick expiry in matter of seconds
    }


    private LicensesManagerService licensesManagerService() {
        final InternalTestCluster clients = internalCluster();
        return clients.getInstance(LicensesManagerService.class, clients.getMasterName());
    }

    private LicensesClientService licensesClientService() {
        final InternalTestCluster clients = internalCluster();
        return clients.getInstance(LicensesClientService.class, clients.getMasterName());
    }

    private LicensesService licensesService() {
        final InternalTestCluster clients = internalCluster();
        return clients.getInstance(LicensesService.class, clients.getMasterName());
    }

    private ClusterService masterClusterService() {
        final InternalTestCluster clients = internalCluster();
        return clients.getInstance(ClusterService.class, clients.getMasterName());
    }

    private void clear() {
        final InternalTestCluster clients = internalCluster();
        LicensesService service = clients.getInstance(LicensesService.class, clients.getMasterName());
        service.clear();
    }


}
