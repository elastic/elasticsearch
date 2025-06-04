/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.autoscaling;

import org.elasticsearch.exception.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.autoscaling.action.DeleteAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingCapacityAction;
import org.elasticsearch.xpack.autoscaling.action.GetAutoscalingPolicyAction;
import org.elasticsearch.xpack.autoscaling.action.PutAutoscalingPolicyAction;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class AutoscalingLicenseCheckerIT extends ESSingleNodeTestCase {

    public static class NonCompliantLicenseLocalStateAutoscaling extends LocalStateCompositeXPackPlugin {

        public NonCompliantLicenseLocalStateAutoscaling(final Settings settings, final Path configPath) {
            super(settings, configPath);
            plugins.add(new Autoscaling(new AutoscalingLicenseChecker(() -> false)));
        }

    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(NonCompliantLicenseLocalStateAutoscaling.class);
    }

    public void testCanNotPutPolicyWithNonCompliantLicense() throws InterruptedException {
        final PutAutoscalingPolicyAction.Request request = new PutAutoscalingPolicyAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            "",
            Collections.emptySortedSet(),
            Collections.emptySortedMap()
        );
        final CountDownLatch latch = new CountDownLatch(1);
        client().execute(PutAutoscalingPolicyAction.INSTANCE, request, new ActionListener<>() {

            @Override
            public void onResponse(final AcknowledgedResponse response) {
                try {
                    fail();
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(final Exception e) {
                try {
                    assertNonCompliantLicense(e);
                } finally {
                    latch.countDown();
                }
            }

        });
        latch.await();
    }

    public void testCanNotGetPolicyWithNonCompliantLicense() throws InterruptedException {
        final GetAutoscalingPolicyAction.Request request = new GetAutoscalingPolicyAction.Request(TEST_REQUEST_TIMEOUT, "");
        final CountDownLatch latch = new CountDownLatch(1);
        client().execute(GetAutoscalingPolicyAction.INSTANCE, request, new ActionListener<>() {

            @Override
            public void onResponse(final GetAutoscalingPolicyAction.Response response) {
                try {
                    fail();
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(final Exception e) {
                try {
                    assertNonCompliantLicense(e);
                } finally {
                    latch.countDown();
                }
            }

        });
        latch.await();
    }

    public void testCanNonGetAutoscalingCapacityDecisionWithNonCompliantLicense() throws InterruptedException {
        final GetAutoscalingCapacityAction.Request request = new GetAutoscalingCapacityAction.Request(TEST_REQUEST_TIMEOUT);
        final CountDownLatch latch = new CountDownLatch(1);
        client().execute(GetAutoscalingCapacityAction.INSTANCE, request, new ActionListener<>() {

            @Override
            public void onResponse(final GetAutoscalingCapacityAction.Response response) {
                try {
                    fail();
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(final Exception e) {
                try {
                    assertNonCompliantLicense(e);
                } finally {
                    latch.countDown();
                }
            }

        });
        latch.await();
    }

    public void testCanDeleteAutoscalingPolicyEvenWithNonCompliantLicense() throws InterruptedException {
        final DeleteAutoscalingPolicyAction.Request request = new DeleteAutoscalingPolicyAction.Request(
            TEST_REQUEST_TIMEOUT,
            TEST_REQUEST_TIMEOUT,
            "*"
        );
        final CountDownLatch latch = new CountDownLatch(1);
        client().execute(DeleteAutoscalingPolicyAction.INSTANCE, request, new ActionListener<>() {

            @Override
            public void onResponse(final AcknowledgedResponse response) {
                latch.countDown();
            }

            @Override
            public void onFailure(final Exception e) {
                try {
                    fail();
                } finally {
                    latch.countDown();
                }
            }

        });
        latch.await();
    }

    private void assertNonCompliantLicense(final Exception e) {
        assertThat(e, instanceOf(ElasticsearchSecurityException.class));
        assertThat(e.getMessage(), equalTo("current license is non-compliant for [autoscaling]"));
    }

}
