/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.core.LicensesClientService;
import org.elasticsearch.license.plugin.core.LicensesManagerService;
import org.elasticsearch.license.plugin.core.LicensesService;
import org.elasticsearch.license.plugin.core.LicensesStatus;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.license.plugin.TestUtils.generateSignedLicense;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.ClusterScope;
import static org.elasticsearch.test.ElasticsearchIntegrationTest.Scope.TEST;
import static org.hamcrest.Matchers.equalTo;

@ClusterScope(scope = TEST, numDataNodes = 10)
public class LicensesClientServiceTests extends AbstractLicensesServiceTests {

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
    public void testLicenseWithFutureIssueDate() throws Exception {
        final LicensesClientService clientService = licensesClientService();
        final TestTrackingClientListener clientListener = new TestTrackingClientListener();
        String feature = "feature";
        List<Action> actions = new ArrayList<>();
        long now = System.currentTimeMillis();
        long issueDate = dateMath("now+10d/d", now);

        actions.add(registerWithTrialLicense(clientService, clientListener, feature, TimeValue.timeValueSeconds(2)));
        actions.add(generateAndPutSignedLicenseAction(masterLicensesManagerService(), feature, issueDate, TimeValue.timeValueHours(24 * 20)));
        actions.add(assertExpiryAction(feature, "trial", TimeValue.timeValueSeconds(2)));
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

    private Action generateAndPutSignedLicenseAction(final LicensesManagerService masterLicensesManagerService, final String feature, final TimeValue expiryDuration) throws Exception {
        return generateAndPutSignedLicenseAction(masterLicensesManagerService, feature, -1, expiryDuration);
    }
    private Action generateAndPutSignedLicenseAction(final LicensesManagerService masterLicensesManagerService, final String feature, final long issueDate, final TimeValue expiryDuration) throws Exception {
        return new Action(new Runnable() {
            @Override
            public void run() {
                License license;
                try {
                    license = generateSignedLicense(feature, issueDate, expiryDuration);
                } catch (Exception e) {
                    fail(e.getMessage());
                    return;
                }
                registerAndAckSignedLicenses(masterLicensesManagerService, Arrays.asList(license), LicensesStatus.VALID);
            }
        }, 0, (issueDate <= System.currentTimeMillis()) ? 1 : 0, "should trigger onEnable for " + feature + " once [signed license]");
    }

    private Action registerWithoutTrialLicense(final LicensesClientService clientService, final LicensesClientService.Listener clientListener, final String feature) {
        return new Action(new Runnable() {
            @Override
            public void run() {
                clientService.register(feature, null, clientListener);
            }
        }, 0, 0, "should not trigger any notification [disabled by default]");
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
        for (final Action action : actions) {
            action.run();
            cumulativeTimeoutMillis.addAndGet(action.timeout.getMillis());
        }

        if (expectedDisableCount.get() > 0) {
            assertThat(getActionMsg(true, enableLatch.getCount(), actions), enableLatch.await((cumulativeTimeoutMillis.get() * 2), TimeUnit.MILLISECONDS), equalTo(true));
        }
        if (expectedDisableCount.get() > 0) {
            assertThat(getActionMsg(false, disableLatch.getCount(), actions), disableLatch.await((cumulativeTimeoutMillis.get() * 2), TimeUnit.MILLISECONDS), equalTo(true));
        }
    }

    private static String getActionMsg(final boolean enabledCount, final long latchCount, final List<Action> actions) {
        AtomicLong cumulativeCount = new AtomicLong(0);
        for (Action action : actions) {
            cumulativeCount.addAndGet((enabledCount) ? action.expectedEnabledCount : action.expectedDisabledCount);
            if (latchCount <= cumulativeCount.get()) {
                return action.msg;
            }
        }
        return "there should be no errors";
    }
}
