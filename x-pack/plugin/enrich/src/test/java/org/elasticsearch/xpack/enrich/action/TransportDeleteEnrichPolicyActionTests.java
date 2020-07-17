/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.AbstractEnrichTestCase;
import org.elasticsearch.xpack.enrich.EnrichPolicyLocks;
import org.elasticsearch.xpack.enrich.EnrichStore;
import org.junit.After;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.enrich.EnrichPolicyTests.randomEnrichPolicy;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class TransportDeleteEnrichPolicyActionTests extends AbstractEnrichTestCase {

    @After
    public void cleanupPolicy() {
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        String name = "my-policy";

        try {
            deleteEnrichPolicy(name, clusterService);
        } catch (Exception e) {
            // if the enrich policy does not exist, then just keep going
        }

        // fail if the state of this is left locked
        EnrichPolicyLocks enrichPolicyLocks = getInstanceFromNode(EnrichPolicyLocks.class);
        assertFalse(enrichPolicyLocks.captureExecutionState().isAnyPolicyInFlight());
    }

    public void testDeletePolicyDoesNotExistUnlocksPolicy() throws InterruptedException {
        String fakeId = "fake-id";
        createIndex(EnrichPolicy.getBaseName(fakeId) + "-foo1");
        createIndex(EnrichPolicy.getBaseName(fakeId) + "-foo2");

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> reference = new AtomicReference<>();
        final TransportDeleteEnrichPolicyAction transportAction = node().injector().getInstance(TransportDeleteEnrichPolicyAction.class);
        ActionTestUtils.execute(transportAction, null, new DeleteEnrichPolicyAction.Request(fakeId), new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                fail();
            }

            public void onFailure(final Exception e) {
                reference.set(e);
                latch.countDown();
            }
        });
        latch.await();
        assertNotNull(reference.get());
        assertThat(reference.get(), instanceOf(ResourceNotFoundException.class));
        assertThat(reference.get().getMessage(), equalTo("policy [fake-id] not found"));

        // fail if the state of this is left locked
        EnrichPolicyLocks enrichPolicyLocks = getInstanceFromNode(EnrichPolicyLocks.class);
        assertFalse(enrichPolicyLocks.captureExecutionState().isAnyPolicyInFlight());
    }

    public void testDeleteWithoutIndex() throws Exception {
        EnrichPolicy policy = randomEnrichPolicy(XContentType.JSON);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        String name = "my-policy";

        AtomicReference<Exception> error = saveEnrichPolicy(name, policy, clusterService);
        assertThat(error.get(), nullValue());

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<AcknowledgedResponse> reference = new AtomicReference<>();
        final TransportDeleteEnrichPolicyAction transportAction = node().injector().getInstance(TransportDeleteEnrichPolicyAction.class);
        ActionTestUtils.execute(transportAction, null, new DeleteEnrichPolicyAction.Request(name), new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                reference.set(acknowledgedResponse);
                latch.countDown();
            }

            public void onFailure(final Exception e) {
                fail();
            }
        });
        latch.await();
        assertNotNull(reference.get());
        assertTrue(reference.get().isAcknowledged());

        EnrichPolicyLocks enrichPolicyLocks = getInstanceFromNode(EnrichPolicyLocks.class);
        assertFalse(enrichPolicyLocks.captureExecutionState().isAnyPolicyInFlight());

        assertNull(EnrichStore.getPolicy(name, clusterService.state()));
    }

    public void testDeleteIsNotLocked() throws Exception {
        EnrichPolicy policy = randomEnrichPolicy(XContentType.JSON);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        String name = "my-policy";

        AtomicReference<Exception> error = saveEnrichPolicy(name, policy, clusterService);
        assertThat(error.get(), nullValue());

        boolean destructiveRequiresName = randomBoolean();
        if (destructiveRequiresName) {
            Settings settings = Settings.builder()
                .put(DestructiveOperations.REQUIRES_NAME_SETTING.getKey(), destructiveRequiresName)
                .build();
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));
        }

        createIndex(EnrichPolicy.getBaseName(name) + "-foo1");
        createIndex(EnrichPolicy.getBaseName(name) + "-foo2");

        client().admin()
            .indices()
            .prepareGetIndex()
            .setIndices(EnrichPolicy.getBaseName(name) + "-foo1", EnrichPolicy.getBaseName(name) + "-foo2")
            .get();

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<AcknowledgedResponse> reference = new AtomicReference<>();
        final TransportDeleteEnrichPolicyAction transportAction = node().injector().getInstance(TransportDeleteEnrichPolicyAction.class);
        ActionTestUtils.execute(transportAction, null, new DeleteEnrichPolicyAction.Request(name), new ActionListener<>() {
            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                reference.set(acknowledgedResponse);
                latch.countDown();
            }

            public void onFailure(final Exception e) {
                fail();
            }
        });
        latch.await();
        assertNotNull(reference.get());
        assertTrue(reference.get().isAcknowledged());

        expectThrows(
            IndexNotFoundException.class,
            () -> client().admin()
                .indices()
                .prepareGetIndex()
                .setIndices(EnrichPolicy.getBaseName(name) + "-foo1", EnrichPolicy.getBaseName(name) + "-foo2")
                .get()
        );

        if (destructiveRequiresName) {
            Settings settings = Settings.builder().putNull(DestructiveOperations.REQUIRES_NAME_SETTING.getKey()).build();
            assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings));
        }

        EnrichPolicyLocks enrichPolicyLocks = getInstanceFromNode(EnrichPolicyLocks.class);
        assertFalse(enrichPolicyLocks.captureExecutionState().isAnyPolicyInFlight());

        assertNull(EnrichStore.getPolicy(name, clusterService.state()));
    }

    public void testDeleteLocked() throws InterruptedException {
        EnrichPolicy policy = randomEnrichPolicy(XContentType.JSON);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        String name = "my-policy";
        final TransportDeleteEnrichPolicyAction transportAction = node().injector().getInstance(TransportDeleteEnrichPolicyAction.class);
        AtomicReference<Exception> error = saveEnrichPolicy(name, policy, clusterService);
        assertThat(error.get(), nullValue());

        createIndex(EnrichPolicy.getBaseName(name) + "-foo1");
        createIndex(EnrichPolicy.getBaseName(name) + "-foo2");

        EnrichPolicyLocks enrichPolicyLocks = getInstanceFromNode(EnrichPolicyLocks.class);
        assertFalse(enrichPolicyLocks.captureExecutionState().isAnyPolicyInFlight());

        enrichPolicyLocks.lockPolicy(name);
        assertTrue(enrichPolicyLocks.captureExecutionState().isAnyPolicyInFlight());

        {
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<Exception> reference = new AtomicReference<>();
            ActionTestUtils.execute(transportAction, null, new DeleteEnrichPolicyAction.Request(name), new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    fail();
                }

                public void onFailure(final Exception e) {
                    reference.set(e);
                    latch.countDown();
                }
            });
            latch.await();
            assertNotNull(reference.get());
            assertThat(reference.get(), instanceOf(EsRejectedExecutionException.class));
            assertThat(
                reference.get().getMessage(),
                equalTo("Could not obtain lock because policy execution for [my-policy] is already in progress.")
            );
        }
        {
            enrichPolicyLocks.releasePolicy(name);
            assertFalse(enrichPolicyLocks.captureExecutionState().isAnyPolicyInFlight());

            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<AcknowledgedResponse> reference = new AtomicReference<>();

            ActionTestUtils.execute(transportAction, null, new DeleteEnrichPolicyAction.Request(name), new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    reference.set(acknowledgedResponse);
                    latch.countDown();
                }

                public void onFailure(final Exception e) {
                    fail();
                }
            });
            latch.await();
            assertNotNull(reference.get());
            assertTrue(reference.get().isAcknowledged());

            assertFalse(enrichPolicyLocks.captureExecutionState().isAnyPolicyInFlight());

            assertNull(EnrichStore.getPolicy(name, clusterService.state()));
        }
    }
}
