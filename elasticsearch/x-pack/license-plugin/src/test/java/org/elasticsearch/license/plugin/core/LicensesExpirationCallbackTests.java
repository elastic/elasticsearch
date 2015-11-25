/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core;

import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.license.core.License;
import org.elasticsearch.license.plugin.TestUtils;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.equalTo;

public class LicensesExpirationCallbackTests extends ESSingleNodeTestCase {
    static {
        MetaData.registerPrototype(LicensesMetaData.TYPE, LicensesMetaData.PROTO);
    }

    @Override
    protected boolean resetNodeAfterTest() {
        return true;
    }

    public void testPostExpiration() throws Exception {
        int postExpirySeconds = randomIntBetween(5, 10);
        TimeValue postExpiryDuration = TimeValue.timeValueSeconds(postExpirySeconds);
        TimeValue min = TimeValue.timeValueSeconds(postExpirySeconds - randomIntBetween(1, 3));
        TimeValue max = TimeValue.timeValueSeconds(postExpirySeconds + randomIntBetween(1, 10));

        final LicensesService.ExpirationCallback.Post post = new LicensesService.ExpirationCallback.Post(min, max, TimeValue.timeValueMillis(10)) {
            @Override
            public void on(License license) {
            }
        };
        long now = System.currentTimeMillis();
        assertThat(post.matches(now - postExpiryDuration.millis(), now), equalTo(true));
        assertThat(post.matches(now + postExpiryDuration.getMillis(), now), equalTo(false));
    }

    public void testPostExpirationWithNullMax() throws Exception {
        int postExpirySeconds = randomIntBetween(5, 10);
        TimeValue postExpiryDuration = TimeValue.timeValueSeconds(postExpirySeconds);
        TimeValue min = TimeValue.timeValueSeconds(postExpirySeconds - randomIntBetween(1, 3));

        final LicensesService.ExpirationCallback.Post post = new LicensesService.ExpirationCallback.Post(min, null, TimeValue.timeValueMillis(10)) {
            @Override
            public void on(License license) {
            }
        };
        long now = System.currentTimeMillis();
        assertThat(post.matches(now - postExpiryDuration.millis(), now), equalTo(true));
    }

    public void testPreExpirationWithNullMin() throws Exception {
        int expirySeconds = randomIntBetween(5, 10);
        TimeValue expiryDuration = TimeValue.timeValueSeconds(expirySeconds);
        TimeValue max = TimeValue.timeValueSeconds(expirySeconds + randomIntBetween(1, 10));

        final LicensesService.ExpirationCallback.Pre pre = new LicensesService.ExpirationCallback.Pre(null, max, TimeValue.timeValueMillis(10)) {
            @Override
            public void on(License license) {
            }
        };
        long now = System.currentTimeMillis();
        assertThat(pre.matches(expiryDuration.millis() + now, now), equalTo(true));
    }

    public void testPreExpiration() throws Exception {
        int expirySeconds = randomIntBetween(5, 10);
        TimeValue expiryDuration = TimeValue.timeValueSeconds(expirySeconds);
        TimeValue min = TimeValue.timeValueSeconds(expirySeconds - randomIntBetween(0, 3));
        TimeValue max = TimeValue.timeValueSeconds(expirySeconds + randomIntBetween(1, 10));

        final LicensesService.ExpirationCallback.Pre pre = new LicensesService.ExpirationCallback.Pre(min, max, TimeValue.timeValueMillis(10)) {
            @Override
            public void on(License license) {
            }
        };
        long now = System.currentTimeMillis();
        assertThat(pre.matches(expiryDuration.millis() + now, now), equalTo(true));
        assertThat(pre.matches(now - expiryDuration.getMillis(), now), equalTo(false));
    }

    public void testPreExpirationNotification() throws Exception {
        final LicensesService licensesService = getInstanceFromNode(LicensesService.class);
        licensesService.setTrialLicenseDuration(TimeValue.timeValueSeconds(5));
        AtomicInteger counter = new AtomicInteger(0);
        licensesService.setExpirationCallbacks(Collections.singletonList(
                        preCallbackLatch(TimeValue.timeValueSeconds(1), TimeValue.timeValueSeconds(2), TimeValue.timeValueMillis(400), counter)) // 2000, 1600, 1200
        );
        licensesService.start();
        TestUtils.AssertingLicensee licensee = new TestUtils.AssertingLicensee("testPreExpirationNotification", logger);
        licensesService.register(licensee);
        boolean success = awaitBusy(() -> (counter.get() == 3 || counter.get() == 2));
        assertThat("counter: actual: " + counter.get() + "vs expected: 3", success, equalTo(true));
        licensesService.stop();
    }

    public void testPostExpirationNotification() throws Exception {
        final LicensesService licensesService = getInstanceFromNode(LicensesService.class);
        licensesService.setTrialLicenseDuration(TimeValue.timeValueSeconds(3));
        AtomicInteger counter = new AtomicInteger(0);
        licensesService.setExpirationCallbacks(Collections.singletonList(
                postCallbackLatch(TimeValue.timeValueMillis(700), TimeValue.timeValueSeconds(3), TimeValue.timeValueSeconds(1), counter)) // 700, 1700, 2700
        );
        licensesService.start();
        TestUtils.AssertingLicensee licensee = new TestUtils.AssertingLicensee("testPostExpirationNotification", logger);
        licensesService.register(licensee);
        // callback can be called only twice if the third notification is triggered with a delay
        // causing the trigger time to be out of the post expiry callback window
        boolean success = awaitBusy(() -> (counter.get() == 3 || counter.get() == 2));
        assertThat("counter: actual: " + counter.get() + "vs expected: 3", success, equalTo(true));
        licensesService.stop();
    }

    public void testMultipleExpirationNotification() throws Exception {
        final LicensesService licensesService = getInstanceFromNode(LicensesService.class);
        licensesService.setTrialLicenseDuration(TimeValue.timeValueSeconds(4));
        AtomicInteger postCounter = new AtomicInteger(0);
        AtomicInteger preCounter = new AtomicInteger(0);
        licensesService.setExpirationCallbacks(Arrays.asList(
                        preCallbackLatch(TimeValue.timeValueSeconds(1), TimeValue.timeValueSeconds(2), TimeValue.timeValueMillis(400), preCounter), // 2000, 1600, 1200
                        postCallbackLatch(TimeValue.timeValueMillis(100), TimeValue.timeValueSeconds(2), TimeValue.timeValueMillis(400), postCounter)) // 100, 500, 900, 1300, 1700
        );
        licensesService.start();
        TestUtils.AssertingLicensee licensee = new TestUtils.AssertingLicensee("testMultipleExpirationNotification", logger);
        licensesService.register(licensee);
        // callback can be called one less than expected if the last notification is triggered
        // with a delay, causing the trigger time to be out of the expiry callback window
        boolean success = awaitBusy(() -> ((preCounter.get() == 3 || preCounter.get() == 2)
                        && (postCounter.get() == 5 || postCounter.get() == 4)));
        assertThat("post count: actual: " + postCounter.get() + "vs expected: 5 " +
                "pre count: actual: " + preCounter.get() + " vs expected: 3", success, equalTo(true));
        licensesService.stop();
    }

    private static LicensesService.ExpirationCallback preCallbackLatch(TimeValue min, TimeValue max, TimeValue frequency, final AtomicInteger count) {
        return new LicensesService.ExpirationCallback.Pre(min, max, frequency) {
            @Override
            public void on(License license) {
                count.incrementAndGet();
            }
        };
    }

    private static LicensesService.ExpirationCallback postCallbackLatch(TimeValue min, TimeValue max, TimeValue frequency, final AtomicInteger count) {
        return new LicensesService.ExpirationCallback.Post(min, max, frequency) {
            @Override
            public void on(License license) {
                count.incrementAndGet();
            }
        };
    }
}
