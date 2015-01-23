/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin;

import org.elasticsearch.common.base.Predicate;
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
import static org.hamcrest.Matchers.*;

@ClusterScope(scope = TEST, numDataNodes = 10)
public class LicensesClientServiceTests extends AbstractLicensesServiceTests {

    @Test
    public void testTrialLicenseEnforcement() throws Exception {
        // register with trial license and assert onEnable and onDisable notification

        final LicensesClientService clientService = licensesClientService();
        String feature1 = "feature1";
        final TestTrackingClientListener clientListener = new TestTrackingClientListener(feature1);
        List<Action> actions = new ArrayList<>();

        final TimeValue expiryDuration = TimeValue.timeValueSeconds(2);
        actions.add(registerWithTrialLicense(clientService, clientListener, feature1, expiryDuration));
        actions.add(assertExpiryAction(feature1, "trial", expiryDuration));
        assertClientListenerNotificationCount(clientListener, actions);
    }

    @Test
    public void testLicenseWithFutureIssueDate() throws Exception {
        final LicensesClientService clientService = licensesClientService();
        String feature = "feature";
        final TestTrackingClientListener clientListener = new TestTrackingClientListener(feature);
        List<Action> actions = new ArrayList<>();
        long now = System.currentTimeMillis();
        long issueDate = dateMath("now+10d/d", now);

        actions.add(registerWithoutTrialLicense(clientService, clientListener, feature));
        actions.add(generateAndPutSignedLicenseAction(masterLicensesManagerService(), feature, issueDate, TimeValue.timeValueHours(24 * 20)));
        assertClientListenerNotificationCount(clientListener, actions);
        assertThat(clientListener.enabled.get(), equalTo(false));
    }

    @Test
    public void testPostExpiration() throws Exception {
        int postExpirySeconds = randomIntBetween(5, 10);
        TimeValue postExpiryDuration = TimeValue.timeValueSeconds(postExpirySeconds);
        TimeValue min = TimeValue.timeValueSeconds(postExpirySeconds - randomIntBetween(1, 3));
        TimeValue max = TimeValue.timeValueSeconds(postExpirySeconds + randomIntBetween(1, 10));

        final LicensesService.ExpirationCallback.Post post = new LicensesService.ExpirationCallback.Post(min, max, TimeValue.timeValueMillis(10)) {

            @Override
            public void on(License license, LicensesService.ExpirationStatus status) {
            }
        };
        long now = System.currentTimeMillis();
        assertThat(post.matches(now - postExpiryDuration.millis(), now), equalTo(true));
        assertThat(post.matches(now + postExpiryDuration.getMillis(), now), equalTo(false));
    }

    @Test
    public void testPostExpirationWithNullMax() throws Exception {
        int postExpirySeconds = randomIntBetween(5, 10);
        TimeValue postExpiryDuration = TimeValue.timeValueSeconds(postExpirySeconds);
        TimeValue min = TimeValue.timeValueSeconds(postExpirySeconds - randomIntBetween(1, 3));

        final LicensesService.ExpirationCallback.Post post = new LicensesService.ExpirationCallback.Post(min, null, TimeValue.timeValueMillis(10)) {

            @Override
            public void on(License license, LicensesService.ExpirationStatus status) {
            }
        };
        long now = System.currentTimeMillis();
        assertThat(post.matches(now - postExpiryDuration.millis(), now), equalTo(true));
    }

    @Test
    public void testPreExpirationWithNullMin() throws Exception {
        int expirySeconds = randomIntBetween(5, 10);
        TimeValue expiryDuration = TimeValue.timeValueSeconds(expirySeconds);
        TimeValue max = TimeValue.timeValueSeconds(expirySeconds + randomIntBetween(1, 10));

        final LicensesService.ExpirationCallback.Pre pre = new LicensesService.ExpirationCallback.Pre(null, max, TimeValue.timeValueMillis(10)) {

            @Override
            public void on(License license, LicensesService.ExpirationStatus status) {
            }
        };
        long now = System.currentTimeMillis();
        assertThat(pre.matches(expiryDuration.millis() + now, now), equalTo(true));
    }

    @Test
    public void testPreExpiration() throws Exception {
        int expirySeconds = randomIntBetween(5, 10);
        TimeValue expiryDuration = TimeValue.timeValueSeconds(expirySeconds);
        TimeValue min = TimeValue.timeValueSeconds(expirySeconds - randomIntBetween(0, 3));
        TimeValue max = TimeValue.timeValueSeconds(expirySeconds + randomIntBetween(1, 10));

        final LicensesService.ExpirationCallback.Pre pre = new LicensesService.ExpirationCallback.Pre(min, max, TimeValue.timeValueMillis(10)) {

            @Override
            public void on(License license, LicensesService.ExpirationStatus status) {
            }
        };
        long now = System.currentTimeMillis();
        assertThat(pre.matches(expiryDuration.millis() + now, now), equalTo(true));
        assertThat(pre.matches(now - expiryDuration.getMillis(), now), equalTo(false));
    }

    @Test
    public void testMultipleEventNotification() throws Exception {
        final LicensesManagerService licensesManagerService = masterLicensesManagerService();
        final LicensesClientService clientService = licensesClientService();
        final String feature = "feature";
        TestTrackingClientListener clientListener = new TestTrackingClientListener(feature, true);

        List<LicensesService.ExpirationCallback> callbacks = new ArrayList<>();
        final AtomicInteger triggerCount1 = new AtomicInteger(0);
        callbacks.add(preCallbackLatch(TimeValue.timeValueMillis(500), TimeValue.timeValueSeconds(1), TimeValue.timeValueMillis(100), triggerCount1));
        final AtomicInteger triggerCount2 = new AtomicInteger(0);
        callbacks.add(postCallbackLatch(TimeValue.timeValueMillis(0), null, TimeValue.timeValueMillis(200), triggerCount2));
        final AtomicInteger triggerCount3 = new AtomicInteger(0);
        callbacks.add(preCallbackLatch(TimeValue.timeValueSeconds(1), TimeValue.timeValueSeconds(2), TimeValue.timeValueMillis(500), triggerCount3));

        List<Action> actions = new ArrayList<>();
        actions.add(registerWithEventNotification(clientService, clientListener, feature, TimeValue.timeValueSeconds(3), callbacks));
        actions.add(assertExpiryAction(feature, "trial", TimeValue.timeValueMinutes(1)));
        assertClientListenerNotificationCount(clientListener, actions);
        assertThat(triggerCount3.get(), greaterThanOrEqualTo(2));
        assertThat(triggerCount3.get(), lessThan(4));
        assertThat(triggerCount1.get(), greaterThan(4));
        assertThat(awaitBusy(new Predicate<Object>() {
            @Override
            public boolean apply(Object o) {
                if (triggerCount2.get() > 0) {
                    return true;
                }
                return false;
            }
        }, 2, TimeUnit.SECONDS), equalTo(true));
        int previousTriggerCount = triggerCount2.get();

        // Update license
        generateAndPutSignedLicenseAction(licensesManagerService, feature, TimeValue.timeValueSeconds(10)).run();
        Thread.sleep(500);
        assertThat(previousTriggerCount, lessThanOrEqualTo(triggerCount2.get() + 1));
    }

    @Test
    public void testPreEventNotification() throws Exception {
        final LicensesClientService clientService = licensesClientService();
        final String feature = "feature";
        TestTrackingClientListener clientListener = new TestTrackingClientListener(feature, true);
        AtomicInteger counter = new AtomicInteger(0);
        List<Action> actions = new ArrayList<>();
        actions.add(
                registerWithEventNotification(clientService, clientListener, feature, TimeValue.timeValueSeconds(3),
                        Arrays.asList(
                                preCallbackLatch(TimeValue.timeValueMillis(500), TimeValue.timeValueSeconds(2), TimeValue.timeValueMillis(500), counter)
                        ))
        );
        actions.add(assertExpiryAction(feature, "trial", TimeValue.timeValueSeconds(3)));
        assertClientListenerNotificationCount(clientListener, actions);
        assertThat(counter.get(), greaterThanOrEqualTo(3));
        assertThat(counter.get(), lessThan(5));
    }

    @Test
    public void testPostEventNotification() throws Exception {
        final LicensesClientService clientService = licensesClientService();
        final String feature = "feature";
        TestTrackingClientListener clientListener = new TestTrackingClientListener(feature, true);
        AtomicInteger counter = new AtomicInteger(0);
        List<Action> actions = new ArrayList<>();
        actions.add(
            registerWithEventNotification(clientService, clientListener, feature, TimeValue.timeValueSeconds(1),
                Arrays.asList(
                        postCallbackLatch(TimeValue.timeValueMillis(500), TimeValue.timeValueSeconds(2), TimeValue.timeValueMillis(500), counter)
                ))
        );
        actions.add(assertExpiryAction(feature, "trial", TimeValue.timeValueSeconds(1)));
        assertClientListenerNotificationCount(clientListener, actions);
        Thread.sleep(50 + 2000);
        assertThat(counter.get(), greaterThanOrEqualTo(3));
        assertThat(counter.get(), lessThan(5));
    }

    private LicensesService.ExpirationCallback preCallbackLatch(TimeValue min, TimeValue max, TimeValue frequency, final AtomicInteger triggerCount) {
        return new LicensesService.ExpirationCallback.Pre(min, max, frequency) {
            @Override
            public void on(License license, LicensesService.ExpirationStatus status) {
                triggerCount.incrementAndGet();
            }
        };
    }

    private LicensesService.ExpirationCallback postCallbackLatch(TimeValue min, TimeValue max, TimeValue frequency, final AtomicInteger triggerCount) {
        return new LicensesService.ExpirationCallback.Post(min, max, frequency) {
            @Override
            public void on(License license, LicensesService.ExpirationStatus status) {
                triggerCount.incrementAndGet();
            }
        };
    }


    @Test
    public void testMultipleClientSignedLicenseEnforcement() throws Exception {
        // multiple client registration with null trial license and then different expiry signed license

        final LicensesManagerService masterLicensesManagerService = masterLicensesManagerService();
        final LicensesService licensesService = randomLicensesService();
        String feature1 = "feature1";
        String feature2 = "feature2";
        final TestTrackingClientListener clientListener1 = new TestTrackingClientListener(feature1);
        final TestTrackingClientListener clientListener2 = new TestTrackingClientListener(feature2);
        List<Action> firstClientActions = new ArrayList<>();
        List<Action> secondClientActions = new ArrayList<>();

        final TimeValue firstExpiryDuration = TimeValue.timeValueSeconds(2);
        firstClientActions.add(registerWithoutTrialLicense(licensesService, clientListener1, feature1));
        firstClientActions.add(generateAndPutSignedLicenseAction(masterLicensesManagerService, feature1, firstExpiryDuration));
        firstClientActions.add(assertExpiryAction(feature1, "signed", firstExpiryDuration));

        final TimeValue secondExpiryDuration = TimeValue.timeValueSeconds(1);
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
        String feature1 = "feature1";
        String feature2 = "feature2";
        final TestTrackingClientListener clientListener1 = new TestTrackingClientListener(feature1);
        final TestTrackingClientListener clientListener2 = new TestTrackingClientListener(feature2);
        List<Action> firstClientActions = new ArrayList<>();
        List<Action> secondClientActions = new ArrayList<>();

        final TimeValue firstExpiryDuration = TimeValue.timeValueSeconds(2);
        firstClientActions.add(registerWithoutTrialLicense(licensesService, clientListener1, feature1));
        firstClientActions.add(generateAndPutSignedLicenseAction(masterLicensesManagerService, feature1, firstExpiryDuration));
        firstClientActions.add(assertExpiryAction(feature1, "signed", firstExpiryDuration));

        final TimeValue secondExpiryDuration = TimeValue.timeValueSeconds(1);
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
        String feature1 = "feature1";
        String feature2 = "feature2";
        final TestTrackingClientListener clientListener1 = new TestTrackingClientListener(feature1);
        final TestTrackingClientListener clientListener2 = new TestTrackingClientListener(feature2);
        List<Action> firstClientActions = new ArrayList<>();
        List<Action> secondClientActions = new ArrayList<>();

        TimeValue firstExpiryDuration = TimeValue.timeValueSeconds(1);
        firstClientActions.add(registerWithTrialLicense(licensesService, clientListener1, feature1, firstExpiryDuration));
        firstClientActions.add(assertExpiryAction(feature1, "trial", firstExpiryDuration));

        TimeValue secondExpiryDuration = TimeValue.timeValueSeconds(2);
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
        String feature = "feature1";
        final TestTrackingClientListener clientListener = new TestTrackingClientListener(feature);
        List<Action> actions = new ArrayList<>();

        actions.add(registerWithoutTrialLicense(clientService, clientListener, feature));
        assertClientListenerNotificationCount(clientListener, actions);
    }

    @Test
    public void testLicenseExpiry() throws Exception {
        final LicensesClientService clientService = licensesClientService();
        String feature = "feature1";
        final TestTrackingClientListener clientListener = new TestTrackingClientListener(feature);
        List<Action> actions = new ArrayList<>();

        TimeValue expiryDuration = TimeValue.timeValueSeconds(2);
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
            String feature = "feature_" + String.valueOf(i);
            final TestTrackingClientListener clientListener = new TestTrackingClientListener(feature);
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
                clientService.register(feature, null, Collections.<LicensesService.ExpirationCallback>emptyList(), clientListener);
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
