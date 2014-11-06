/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.TestUtils;
import org.elasticsearch.license.core.ESLicense;
import org.elasticsearch.license.manager.ESLicenseManager;
import org.elasticsearch.license.plugin.action.put.PutLicenseRequest;
import org.elasticsearch.license.plugin.core.*;
import org.elasticsearch.test.InternalTestCluster;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.license.plugin.core.LicensesService.LicensesUpdateResponse;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.TEST;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = TEST, numDataNodes = 10)
public class LicensesServiceTests extends AbstractLicensesIntegrationTests {

    private static String node = null;
    private static String[] nodes;

    @Before
    public void beforeTest() throws Exception {
        wipeAllLicenses();

        DiscoveryNodes discoveryNodes = LicensesServiceTests.masterClusterService().state().getNodes();
        Set<String> dataNodeSet = new HashSet<>();
        for (DiscoveryNode discoveryNode : discoveryNodes) {
            if (discoveryNode.dataNode()) {
                dataNodeSet.add(discoveryNode.getName());
            }
        }
        nodes = dataNodeSet.toArray(new String[dataNodeSet.size()]);
        node = nodes[randomIntBetween(0, nodes.length - 1)];
    }

    @Test
    public void testStoreAndGetLicenses() throws Exception {
        LicensesManagerService licensesManagerService = masterLicensesManagerService();
        ESLicense shieldShortLicense = generateSignedLicense("shield", TimeValue.timeValueHours(1));
        ESLicense shieldLongLicense = generateSignedLicense("shield", TimeValue.timeValueHours(2));
        ESLicense marvelShortLicense = generateSignedLicense("marvel", TimeValue.timeValueHours(1));
        ESLicense marvelLongLicense = generateSignedLicense("marvel", TimeValue.timeValueHours(2));

        List<ESLicense> licenses = Arrays.asList(shieldLongLicense, shieldShortLicense, marvelLongLicense, marvelShortLicense);
        Collections.shuffle(licenses);
        putAndCheckSignedLicensesAction(licensesManagerService, licenses, LicensesStatus.VALID);

        final ImmutableSet<String> licenseSignatures = masterLicenseManager().toSignatures(licenses);
        LicensesMetaData licensesMetaData = clusterService().state().metaData().custom(LicensesMetaData.TYPE);

        // all licenses should be stored in the metaData
        assertThat(licenseSignatures, equalTo(licensesMetaData.getSignatures()));

        // only the latest expiry date license for each feature should be returned by getLicenses()
        final List<ESLicense> getLicenses = licensesManagerService.getLicenses();
        TestUtils.isSame(getLicenses, Arrays.asList(shieldLongLicense, marvelLongLicense));
    }

    @Test
    public void testInvalidLicenseStorage() throws Exception {
        LicensesManagerService licensesManagerService = masterLicensesManagerService();
        ESLicense signedLicense = generateSignedLicense("shield", TimeValue.timeValueMinutes(2));

        // modify content of signed license
        ESLicense tamperedLicense = ESLicense.builder()
                .fromLicenseSpec(signedLicense, signedLicense.signature())
                .expiryDate(signedLicense.expiryDate() + 10 * 24 * 60 * 60 * 1000l)
                .verify()
                .build();

        putAndCheckSignedLicensesAction(licensesManagerService, Arrays.asList(tamperedLicense), LicensesStatus.INVALID);

        // ensure that the invalid license never made it to cluster state
        LicensesMetaData licensesMetaData = clusterService().state().metaData().custom(LicensesMetaData.TYPE);
        if (licensesMetaData != null) {
            assertThat(licensesMetaData.getSignatures().size(), equalTo(0));
        }
    }

    @Test
    public void testTrialLicenseEnforcement() throws Exception {
        // register with trial license and assert onEnable and onDisable notification

        final LicensesClientService clientService = licensesClientService();
        final TestTrackingClientListener clientListener = new TestTrackingClientListener();
        List<Action> actions = new ArrayList<>();

        final TimeValue expiryDuration = TimeValue.timeValueSeconds(2);
        String feature1 = "feature1";
        actions.add(registerWithTrialLicense(clientService, clientListener, feature1, expiryDuration));
        actions.add(assertExpiryAction(feature1, "trial", expiryDuration));
        assertClientListenerNotificationCount(clientListener, actions);
    }

    @Test
    public void testMultipleClientSignedLicenseEnforcement() throws Exception {
        // multiple client registration with null trial license and then different expiry signed license

        final LicensesManagerService masterLicensesManagerService = masterLicensesManagerService();
        final LicensesService licensesService = randomLicensesService();
        final TestTrackingClientListener clientListener1 = new TestTrackingClientListener();
        final TestTrackingClientListener clientListener2 = new TestTrackingClientListener();
        List<Action> firstClientActions = new ArrayList<>();
        List<Action> secondClientActions = new ArrayList<>();

        final TimeValue firstExpiryDuration = TimeValue.timeValueSeconds(2);
        String feature1 = "feature1";
        firstClientActions.add(registerWithoutTrialLicense(licensesService, clientListener1, feature1));
        firstClientActions.add(generateAndPutSignedLicenseAction(masterLicensesManagerService, feature1, firstExpiryDuration));
        firstClientActions.add(assertExpiryAction(feature1, "signed", firstExpiryDuration));

        final TimeValue secondExpiryDuration = TimeValue.timeValueSeconds(1);
        String feature2 = "feature2";
        secondClientActions.add(registerWithoutTrialLicense(licensesService, clientListener2, feature2));
        secondClientActions.add(generateAndPutSignedLicenseAction(masterLicensesManagerService, feature2, secondExpiryDuration));
        secondClientActions.add(assertExpiryAction(feature2, "signed", secondExpiryDuration));

        if (randomBoolean()) {
            assertClientListenerNotificationCount(clientListener1, firstClientActions);
            assertClientListenerNotificationCount(clientListener2, secondClientActions);
        } else {
            assertClientListenerNotificationCount(clientListener2, secondClientActions);
            assertClientListenerNotificationCount(clientListener1, firstClientActions);
        }
    }

    @Test
    public void testMultipleClientTrialAndSignedLicenseEnforcement() throws Exception {
        // multiple client registration: one with trial license and another with signed license (different expiry duration)

        final LicensesManagerService masterLicensesManagerService = masterLicensesManagerService();
        final LicensesService licensesService = randomLicensesService();
        final TestTrackingClientListener clientListener1 = new TestTrackingClientListener();
        final TestTrackingClientListener clientListener2 = new TestTrackingClientListener();
        List<Action> firstClientActions = new ArrayList<>();
        List<Action> secondClientActions = new ArrayList<>();

        final TimeValue firstExpiryDuration = TimeValue.timeValueSeconds(2);
        String feature1 = "feature1";
        firstClientActions.add(registerWithoutTrialLicense(licensesService, clientListener1, feature1));
        firstClientActions.add(generateAndPutSignedLicenseAction(masterLicensesManagerService, feature1, firstExpiryDuration));
        firstClientActions.add(assertExpiryAction(feature1, "signed", firstExpiryDuration));

        final TimeValue secondExpiryDuration = TimeValue.timeValueSeconds(1);
        String feature2 = "feature2";
        secondClientActions.add(registerWithTrialLicense(licensesService, clientListener2, feature2, secondExpiryDuration));
        secondClientActions.add(assertExpiryAction(feature2, "trial", secondExpiryDuration));

        if (randomBoolean()) {
            assertClientListenerNotificationCount(clientListener1, firstClientActions);
            assertClientListenerNotificationCount(clientListener2, secondClientActions);
        } else {
            assertClientListenerNotificationCount(clientListener2, secondClientActions);
            assertClientListenerNotificationCount(clientListener1, firstClientActions);
        }
    }

    @Test
    public void testMultipleClientTrialLicenseRegistration() throws Exception {
        // multiple client registration: both with trail license of different expiryDuration

        final LicensesService licensesService = randomLicensesService();
        final TestTrackingClientListener clientListener1 = new TestTrackingClientListener();
        final TestTrackingClientListener clientListener2 = new TestTrackingClientListener();
        List<Action> firstClientActions = new ArrayList<>();
        List<Action> secondClientActions = new ArrayList<>();

        TimeValue firstExpiryDuration = TimeValue.timeValueSeconds(1);
        String feature1 = "feature1";
        firstClientActions.add(registerWithTrialLicense(licensesService, clientListener1, feature1, firstExpiryDuration));
        firstClientActions.add(assertExpiryAction(feature1, "trial", firstExpiryDuration));

        TimeValue secondExpiryDuration = TimeValue.timeValueSeconds(2);
        String feature2 = "feature2";
        secondClientActions.add(registerWithTrialLicense(licensesService, clientListener2, feature2, secondExpiryDuration));
        secondClientActions.add(assertExpiryAction(feature2, "trial", secondExpiryDuration));

        if (randomBoolean()) {
            assertClientListenerNotificationCount(clientListener1, firstClientActions);
            assertClientListenerNotificationCount(clientListener2, secondClientActions);
        } else {
            assertClientListenerNotificationCount(clientListener2, secondClientActions);
            assertClientListenerNotificationCount(clientListener1, firstClientActions);
        }
    }

    @Test
    public void testFeatureWithoutLicense() throws Exception {
        // client registration with no trial license + no signed license
        final LicensesClientService clientService = licensesClientService();
        final TestTrackingClientListener clientListener = new TestTrackingClientListener();
        List<Action> actions = new ArrayList<>();

        actions.add(registerWithoutTrialLicense(clientService, clientListener, "feature1"));
        assertClientListenerNotificationCount(clientListener, actions);
    }

    @Test
    public void testLicenseExpiry() throws Exception {
        final LicensesClientService clientService = licensesClientService();
        final TestTrackingClientListener clientListener = new TestTrackingClientListener();
        List<Action> actions = new ArrayList<>();

        TimeValue expiryDuration = TimeValue.timeValueSeconds(2);
        String feature = "feature1";
        actions.add(registerWithTrialLicense(clientService, clientListener, feature, expiryDuration));
        actions.add(assertExpiryAction(feature, "trial", expiryDuration));

        assertClientListenerNotificationCount(clientListener, actions);
    }

    @Test
    public void testRandomActionSequenceMultipleFeature() throws Exception {
        LicensesService licensesService = randomLicensesService();
        LicensesManagerService masterLicensesManagerService = masterLicensesManagerService();
        Map<TestTrackingClientListener, List<Action>> clientListenersWithActions = new HashMap<>();

        TimeValue expiryDuration = TimeValue.timeValueSeconds(0);
        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            final TestTrackingClientListener clientListener = new TestTrackingClientListener();
            String feature = "feature_" + String.valueOf(i);
            expiryDuration = TimeValue.timeValueMillis(randomIntBetween(1, 3) * 1000l + expiryDuration.millis());
            List<Action> actions = new ArrayList<>();

            if (randomBoolean()) {
                actions.add(registerWithTrialLicense(licensesService, clientListener, feature, expiryDuration));
                actions.add(assertExpiryAction(feature, "trial", expiryDuration));
            } else {
                actions.add(registerWithoutTrialLicense(licensesService, clientListener, feature));
                actions.add(generateAndPutSignedLicenseAction(masterLicensesManagerService, feature, expiryDuration));
                actions.add(assertExpiryAction(feature, "signed", expiryDuration));
            }
            clientListenersWithActions.put(clientListener, actions);
        }

        for (Map.Entry<TestTrackingClientListener, List<Action>> entry : clientListenersWithActions.entrySet()) {
            assertClientListenerNotificationCount(entry.getKey(), entry.getValue());
        }

    }

    private void putAndCheckSignedLicensesAction(final LicensesManagerService masterLicensesManagerService, final List<ESLicense> license, final LicensesStatus expectedStatus) {
        PutLicenseRequest putLicenseRequest = new PutLicenseRequest().licenses(license);
        LicensesService.PutLicenseRequestHolder requestHolder = new LicensesService.PutLicenseRequestHolder(putLicenseRequest, "test");
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicBoolean success = new AtomicBoolean(false);
        masterLicensesManagerService.registerLicenses(requestHolder, new ActionListener<LicensesUpdateResponse>() {
            @Override
            public void onResponse(LicensesUpdateResponse licensesUpdateResponse) {
                if (licensesUpdateResponse.isAcknowledged() && licensesUpdateResponse.status() == expectedStatus) {
                    success.set(true);
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(Throwable e) {
                latch.countDown();
            }
        });
        try {
            latch.await();
        } catch (InterruptedException e) {
            fail(e.getMessage());
        }
        assertThat(success.get(), equalTo(true));
    }

    private Action generateAndPutSignedLicenseAction(final LicensesManagerService masterLicensesManagerService, final String feature, final TimeValue expiryDuration) throws Exception {
        return new Action(new Runnable() {
            @Override
            public void run() {
                ESLicense license;
                try {
                    license = generateSignedLicense(feature, expiryDuration);
                } catch (Exception e) {
                    fail(e.getMessage());
                    return;
                }
                putAndCheckSignedLicensesAction(masterLicensesManagerService, Arrays.asList(license), LicensesStatus.VALID);
            }
        }, 0, 1, "should trigger onEnable for " + feature + " once [signed license]");
    }

    private Action registerWithoutTrialLicense(final LicensesClientService clientService, final LicensesClientService.Listener clientListener, final String feature) {
        return new Action(new Runnable() {
            @Override
            public void run() {
                clientService.register(feature, null, clientListener);
            }
        }, 0, 0, "should not trigger any notification [disabled by default]");
    }

    private Action registerWithTrialLicense(final LicensesClientService clientService, final LicensesClientService.Listener clientListener, final String feature, final TimeValue expiryDuration) {
        return new Action(new Runnable() {
            @Override
            public void run() {
                clientService.register(feature, new LicensesService.TrialLicenseOptions(expiryDuration, 10),
                        clientListener);

                // invoke clusterChanged event to flush out pendingRegistration
                LicensesService licensesService = (LicensesService) clientService;
                ClusterChangedEvent event = new ClusterChangedEvent("", clusterService().state(), clusterService().state());
                licensesService.clusterChanged(event);
            }
        }, 0, 1, "should trigger onEnable for " + feature + " once [trial license]");
    }

    private Action assertExpiryAction(String feature, String licenseType, TimeValue expiryDuration) {
        return new Action(new Runnable() {
            @Override
            public void run() {
            }
        }, 1, 0, TimeValue.timeValueMillis(expiryDuration.getMillis() * 2),
                "should trigger onDisable for " + feature + " once [" + licenseType + " license expiry]");
    }

    private void assertClientListenerNotificationCount(final TestTrackingClientListener clientListener, List<Action> actions) throws Exception {
        final AtomicInteger expectedEnabledCount = new AtomicInteger(0);
        final AtomicInteger expectedDisableCount = new AtomicInteger(0);

        for (final Action action : actions) {
            expectedEnabledCount.addAndGet(action.expectedEnabledCount);
            expectedDisableCount.addAndGet(action.expectedDisabledCount);
        }

        final CountDownLatch enableLatch = new CountDownLatch(expectedEnabledCount.get());
        final CountDownLatch disableLatch = new CountDownLatch(expectedDisableCount.get());
        final AtomicLong cumulativeTimeoutMillis = new AtomicLong(0);
        clientListener.latch(enableLatch, disableLatch);
        for (final Action  action : actions) {
            action.run();
            cumulativeTimeoutMillis.addAndGet(action.timeout.getMillis());
        }

        if (expectedDisableCount.get() > 0) {
            assertThat(getActionMsg(true, enableLatch.getCount(), actions), enableLatch.await((cumulativeTimeoutMillis.get() * 2), TimeUnit.MILLISECONDS), equalTo(true));
        }
        if (expectedDisableCount.get() > 0) {
            assertThat(disableLatch.await((cumulativeTimeoutMillis.get() * 2), TimeUnit.MILLISECONDS), equalTo(true));
        }
    }

    private static String getActionMsg(final boolean enabledCount,final long latchCount,final List<Action> actions) {
            AtomicLong cumulativeCount = new AtomicLong(0);
            for (Action action : actions) {
                cumulativeCount.addAndGet((enabledCount)? action.expectedEnabledCount : action.expectedDisabledCount);
                if (latchCount <= cumulativeCount.get()) {
                    return action.msg;
                }
            }
        return "there should be no errors";
    }

    private class TestTrackingClientListener implements LicensesClientService.Listener {
        CountDownLatch enableLatch;
        CountDownLatch disableLatch;

        public synchronized void latch(CountDownLatch enableLatch, CountDownLatch disableLatch) {
            this.enableLatch = enableLatch;
            this.disableLatch = disableLatch;
        }

        @Override
        public void onEnabled() {
            this.enableLatch.countDown();
        }

        @Override
        public void onDisabled() {
            this.disableLatch.countDown();
        }
    }

    private class Action {
        final int expectedDisabledCount;
        final int expectedEnabledCount;
        final TimeValue timeout;
        final Runnable action;
        final String msg;

        private Action(Runnable action, int expectedEnabledCount, int expectedDisabledCount, String msg) {
            this(action, expectedEnabledCount, expectedDisabledCount, TimeValue.timeValueSeconds(1), msg);
        }

        private Action(Runnable action, int expectedDisabledCount, int expectedEnabledCount, TimeValue timeout, String msg) {
            this.expectedDisabledCount = expectedDisabledCount;
            this.expectedEnabledCount = expectedEnabledCount;
            this.action = action;
            this.timeout = timeout;
            this.msg = msg;
        }

        public void run() {
            action.run();
        }
    }


    private LicensesManagerService masterLicensesManagerService() {
        final InternalTestCluster clients = internalCluster();
        return clients.getInstance(LicensesManagerService.class, clients.getMasterName());
    }

    private ESLicenseManager masterLicenseManager() {
        final InternalTestCluster clients = internalCluster();
        return clients.getInstance(ESLicenseManager.class, clients.getMasterName());
    }

    private LicensesClientService licensesClientService() {
        return internalCluster().getInstance(LicensesClientService.class, node);
    }

    private LicensesService randomLicensesService() {
        String randomNode = randomFrom(nodes);
        return internalCluster().getInstance(LicensesService.class, randomNode);
    }

    private static ClusterService masterClusterService() {
        final InternalTestCluster clients = internalCluster();
        return clients.getInstance(ClusterService.class, clients.getMasterName());
    }
}
