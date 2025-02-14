/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.GetEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.AbstractEnrichTestCase;
import org.elasticsearch.xpack.enrich.EnrichPolicyLocks;
import org.junit.After;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.enrich.EnrichPolicyTests.assertEqualPolicies;
import static org.elasticsearch.xpack.enrich.EnrichPolicyTests.randomEnrichPolicy;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class TransportGetEnrichPolicyActionTests extends AbstractEnrichTestCase {

    @After
    public void cleanupPolicies() throws InterruptedException {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<GetEnrichPolicyAction.Response> reference = new AtomicReference<>();
        final TransportGetEnrichPolicyAction transportAction = node().injector().getInstance(TransportGetEnrichPolicyAction.class);
        final var task = createTask();
        ActionTestUtils.execute(transportAction, task, new GetEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT), new ActionListener<>() {
            @Override
            public void onResponse(GetEnrichPolicyAction.Response response) {
                reference.set(response);
                latch.countDown();

            }

            public void onFailure(final Exception e) {
                fail(e);
            }
        });
        latch.await();
        assertNotNull(reference.get());
        GetEnrichPolicyAction.Response response = reference.get();

        for (EnrichPolicy.NamedPolicy policy : response.getPolicies()) {
            try {
                deleteEnrichPolicy(policy.getName(), clusterService);
            } catch (Exception e) {
                // if the enrich policy does not exist, then just keep going
            }
        }

        // fail if the state of this is left locked
        EnrichPolicyLocks enrichPolicyLocks = getInstanceFromNode(EnrichPolicyLocks.class);
        assertThat(enrichPolicyLocks.lockedPolices().size(), equalTo(0));
    }

    public void testListPolicies() throws InterruptedException {
        EnrichPolicy policy = randomEnrichPolicy(XContentType.JSON);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        String name = "my-policy";

        AtomicReference<Exception> error = saveEnrichPolicy(name, policy, clusterService);
        assertThat(error.get(), nullValue());

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<GetEnrichPolicyAction.Response> reference = new AtomicReference<>();
        final TransportGetEnrichPolicyAction transportAction = node().injector().getInstance(TransportGetEnrichPolicyAction.class);
        final var task = createTask();
        ActionTestUtils.execute(transportAction, task, new GetEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT), new ActionListener<>() {
            @Override
            public void onResponse(GetEnrichPolicyAction.Response response) {
                reference.set(response);
                latch.countDown();

            }

            public void onFailure(final Exception e) {
                fail(e);
            }
        });
        latch.await();
        assertNotNull(reference.get());
        GetEnrichPolicyAction.Response response = reference.get();

        assertThat(response.getPolicies().size(), equalTo(1));

        EnrichPolicy.NamedPolicy actualPolicy = response.getPolicies().get(0);
        assertThat(name, equalTo(actualPolicy.getName()));
        assertEqualPolicies(policy, actualPolicy.getPolicy());
    }

    public void testListEmptyPolicies() throws InterruptedException {
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<GetEnrichPolicyAction.Response> reference = new AtomicReference<>();
        final TransportGetEnrichPolicyAction transportAction = node().injector().getInstance(TransportGetEnrichPolicyAction.class);
        final var task = createTask();
        ActionTestUtils.execute(transportAction, task, new GetEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT), new ActionListener<>() {
            @Override
            public void onResponse(GetEnrichPolicyAction.Response response) {
                reference.set(response);
                latch.countDown();

            }

            public void onFailure(final Exception e) {
                fail(e);
            }
        });
        latch.await();
        assertNotNull(reference.get());
        GetEnrichPolicyAction.Response response = reference.get();

        assertThat(response.getPolicies().size(), equalTo(0));
    }

    public void testGetPolicy() throws InterruptedException {
        EnrichPolicy policy = randomEnrichPolicy(XContentType.JSON);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        String name = "my-policy";

        AtomicReference<Exception> error = saveEnrichPolicy(name, policy, clusterService);
        assertThat(error.get(), nullValue());

        // save a second one to verify the count below on GET
        error = saveEnrichPolicy("something-else", randomEnrichPolicy(XContentType.JSON), clusterService);
        assertThat(error.get(), nullValue());

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<GetEnrichPolicyAction.Response> reference = new AtomicReference<>();
        final TransportGetEnrichPolicyAction transportAction = node().injector().getInstance(TransportGetEnrichPolicyAction.class);
        ActionTestUtils.execute(
            transportAction,
            createTask(),
            new GetEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, name),
            new ActionListener<>() {
                @Override
                public void onResponse(GetEnrichPolicyAction.Response response) {
                    reference.set(response);
                    latch.countDown();
                }

                public void onFailure(final Exception e) {
                    fail(e);
                }
            }
        );
        latch.await();
        assertNotNull(reference.get());
        GetEnrichPolicyAction.Response response = reference.get();

        assertThat(response.getPolicies().size(), equalTo(1));

        EnrichPolicy.NamedPolicy actualPolicy = response.getPolicies().get(0);
        assertThat(name, equalTo(actualPolicy.getName()));
        assertEqualPolicies(policy, actualPolicy.getPolicy());
    }

    public void testGetMultiplePolicies() throws InterruptedException {
        EnrichPolicy policy = randomEnrichPolicy(XContentType.JSON);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        String name = "my-policy";
        String anotherName = "my-other-policy";

        AtomicReference<Exception> error = saveEnrichPolicy(name, policy, clusterService);
        assertThat(error.get(), nullValue());

        error = saveEnrichPolicy(anotherName, policy, clusterService);
        assertThat(error.get(), nullValue());

        // save a second one to verify the count below on GET
        EnrichPolicy policy2 = randomEnrichPolicy(XContentType.JSON);
        error = saveEnrichPolicy("something-else", policy2, clusterService);
        assertThat(error.get(), nullValue());

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<GetEnrichPolicyAction.Response> reference = new AtomicReference<>();
        final TransportGetEnrichPolicyAction transportAction = node().injector().getInstance(TransportGetEnrichPolicyAction.class);
        ActionTestUtils.execute(
            transportAction,
            createTask(),
            new GetEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, name, anotherName),
            new ActionListener<>() {
                @Override
                public void onResponse(GetEnrichPolicyAction.Response response) {
                    reference.set(response);
                    latch.countDown();
                }

                public void onFailure(final Exception e) {
                    fail(e);
                }
            }
        );
        latch.await();
        assertNotNull(reference.get());
        GetEnrichPolicyAction.Response response = reference.get();

        assertThat(response.getPolicies().size(), equalTo(2));
    }

    public void testGetPolicyThrowsError() throws InterruptedException {
        EnrichPolicy policy = randomEnrichPolicy(XContentType.JSON);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        String name = "my-policy";

        AtomicReference<Exception> error = saveEnrichPolicy(name, policy, clusterService);
        assertThat(error.get(), nullValue());

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<GetEnrichPolicyAction.Response> reference = new AtomicReference<>();
        final TransportGetEnrichPolicyAction transportAction = node().injector().getInstance(TransportGetEnrichPolicyAction.class);
        ActionTestUtils.execute(
            transportAction,
            createTask(),
            new GetEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, "non-exists"),
            new ActionListener<>() {
                @Override
                public void onResponse(GetEnrichPolicyAction.Response response) {
                    reference.set(response);
                    latch.countDown();
                }

                public void onFailure(final Exception e) {
                    fail(e);
                }
            }
        );
        latch.await();
        assertNotNull(reference.get());
        assertThat(reference.get().getPolicies().size(), equalTo(0));
    }

    private static CancellableTask createTask() {
        return new CancellableTask(randomNonNegativeLong(), "test", GetEnrichPolicyAction.NAME, "", TaskId.EMPTY_TASK_ID, Map.of());
    }
}
