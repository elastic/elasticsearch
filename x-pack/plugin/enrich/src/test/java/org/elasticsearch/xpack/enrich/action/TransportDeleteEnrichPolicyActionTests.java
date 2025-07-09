/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.DestructiveOperations;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.AbstractEnrichTestCase;
import org.elasticsearch.xpack.enrich.EnrichPolicyLocks;
import org.elasticsearch.xpack.enrich.EnrichPolicyLocks.EnrichPolicyLock;
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

    private final ProjectId projectId = Metadata.DEFAULT_PROJECT_ID;

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
        assertThat(enrichPolicyLocks.lockedPolices().size(), equalTo(0));
    }

    public void testDeletePolicyDoesNotExistUnlocksPolicy() throws InterruptedException {
        String fakeId = "fake-id";
        createIndex(EnrichPolicy.getIndexName(fakeId, 1001));
        createIndex(EnrichPolicy.getIndexName(fakeId, 1002));

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<Exception> reference = new AtomicReference<>();
        final TransportDeleteEnrichPolicyAction transportAction = node().injector().getInstance(TransportDeleteEnrichPolicyAction.class);
        ActionTestUtils.execute(
            transportAction,
            null,
            new DeleteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, fakeId),
            new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    fail();
                }

                public void onFailure(final Exception e) {
                    reference.set(e);
                    latch.countDown();
                }
            }
        );
        latch.await();
        assertNotNull(reference.get());
        assertThat(reference.get(), instanceOf(ResourceNotFoundException.class));
        assertThat(reference.get().getMessage(), equalTo("policy [fake-id] not found"));

        // fail if the state of this is left locked
        EnrichPolicyLocks enrichPolicyLocks = getInstanceFromNode(EnrichPolicyLocks.class);
        assertThat(enrichPolicyLocks.lockedPolices().size(), equalTo(0));
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
        ActionTestUtils.execute(
            transportAction,
            null,
            new DeleteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, name),
            new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    reference.set(acknowledgedResponse);
                    latch.countDown();
                }

                public void onFailure(final Exception e) {
                    fail();
                }
            }
        );
        latch.await();
        assertNotNull(reference.get());
        assertTrue(reference.get().isAcknowledged());

        EnrichPolicyLocks enrichPolicyLocks = getInstanceFromNode(EnrichPolicyLocks.class);
        assertThat(enrichPolicyLocks.lockedPolices().size(), equalTo(0));

        assertNull(EnrichStore.getPolicy(name, clusterService.state().metadata().getProject(projectId)));
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
            assertAcked(clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).setPersistentSettings(settings));
        }

        createIndex(EnrichPolicy.getIndexName(name, 1001));
        createIndex(EnrichPolicy.getIndexName(name, 1002));

        indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT)
            .setIndices(EnrichPolicy.getIndexName(name, 1001), EnrichPolicy.getIndexName(name, 1002))
            .get();

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<AcknowledgedResponse> reference = new AtomicReference<>();
        final TransportDeleteEnrichPolicyAction transportAction = node().injector().getInstance(TransportDeleteEnrichPolicyAction.class);
        ActionTestUtils.execute(
            transportAction,
            null,
            new DeleteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, name),
            new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    reference.set(acknowledgedResponse);
                    latch.countDown();
                }

                public void onFailure(final Exception e) {
                    fail();
                }
            }
        );
        latch.await();
        assertNotNull(reference.get());
        assertTrue(reference.get().isAcknowledged());

        expectThrows(
            IndexNotFoundException.class,
            indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT)
                .setIndices(EnrichPolicy.getIndexName(name, 1001), EnrichPolicy.getIndexName(name, 1001))
        );

        if (destructiveRequiresName) {
            Settings settings = Settings.builder().putNull(DestructiveOperations.REQUIRES_NAME_SETTING.getKey()).build();
            assertAcked(clusterAdmin().prepareUpdateSettings(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT).setPersistentSettings(settings));
        }

        EnrichPolicyLocks enrichPolicyLocks = getInstanceFromNode(EnrichPolicyLocks.class);
        assertThat(enrichPolicyLocks.lockedPolices().size(), equalTo(0));

        assertNull(EnrichStore.getPolicy(name, clusterService.state().metadata().getProject(projectId)));
    }

    public void testDeleteLocked() throws InterruptedException {
        EnrichPolicy policy = randomEnrichPolicy(XContentType.JSON);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        String name = "my-policy";
        final TransportDeleteEnrichPolicyAction transportAction = node().injector().getInstance(TransportDeleteEnrichPolicyAction.class);
        AtomicReference<Exception> error = saveEnrichPolicy(name, policy, clusterService);
        assertThat(error.get(), nullValue());

        createIndex(EnrichPolicy.getIndexName(name, 1001));
        createIndex(EnrichPolicy.getIndexName(name, 1002));

        EnrichPolicyLocks enrichPolicyLocks = getInstanceFromNode(EnrichPolicyLocks.class);
        assertThat(enrichPolicyLocks.lockedPolices().size(), equalTo(0));

        EnrichPolicyLock policyLock = enrichPolicyLocks.lockPolicy(name);
        assertThat(enrichPolicyLocks.lockedPolices().size(), equalTo(1));

        {
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<Exception> reference = new AtomicReference<>();
            ActionTestUtils.execute(
                transportAction,
                null,
                new DeleteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, name),
                new ActionListener<>() {
                    @Override
                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                        fail();
                    }

                    public void onFailure(final Exception e) {
                        reference.set(e);
                        latch.countDown();
                    }
                }
            );
            latch.await();
            assertNotNull(reference.get());
            assertThat(reference.get(), instanceOf(EsRejectedExecutionException.class));
            assertThat(
                reference.get().getMessage(),
                equalTo("Could not obtain lock because policy execution for [my-policy] is already in progress.")
            );
        }
        {
            policyLock.close();
            assertThat(enrichPolicyLocks.lockedPolices().size(), equalTo(0));

            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<AcknowledgedResponse> reference = new AtomicReference<>();

            ActionTestUtils.execute(
                transportAction,
                null,
                new DeleteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, name),
                new ActionListener<>() {
                    @Override
                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                        reference.set(acknowledgedResponse);
                        latch.countDown();
                    }

                    public void onFailure(final Exception e) {
                        fail();
                    }
                }
            );
            latch.await();
            assertNotNull(reference.get());
            assertTrue(reference.get().isAcknowledged());

            assertThat(enrichPolicyLocks.lockedPolices().size(), equalTo(0));

            assertNull(EnrichStore.getPolicy(name, clusterService.state().metadata().getProject(projectId)));
        }
    }

    public void testDeletePolicyPrefixes() throws InterruptedException {
        EnrichPolicy policy = randomEnrichPolicy(XContentType.JSON);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);

        String name = "my-policy";
        String otherName = "my-policy-two"; // the first policy is a prefix of this one

        final TransportDeleteEnrichPolicyAction transportAction = node().injector().getInstance(TransportDeleteEnrichPolicyAction.class);
        AtomicReference<Exception> error;
        error = saveEnrichPolicy(name, policy, clusterService);
        assertThat(error.get(), nullValue());
        error = saveEnrichPolicy(otherName, policy, clusterService);
        assertThat(error.get(), nullValue());

        // create an index for the *other* policy
        createIndex(EnrichPolicy.getIndexName(otherName, 1001));

        {
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<AcknowledgedResponse> reference = new AtomicReference<>();

            ActionTestUtils.execute(
                transportAction,
                null,
                new DeleteEnrichPolicyAction.Request(TEST_REQUEST_TIMEOUT, name),
                new ActionListener<>() {
                    @Override
                    public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                        reference.set(acknowledgedResponse);
                        latch.countDown();
                    }

                    public void onFailure(final Exception e) {
                        fail();
                    }
                }
            );
            latch.await();
            assertNotNull(reference.get());
            assertTrue(reference.get().isAcknowledged());

            assertNull(EnrichStore.getPolicy(name, clusterService.state().metadata().getProject(projectId)));

            // deleting name policy should have no effect on the other policy
            assertNotNull(EnrichStore.getPolicy(otherName, clusterService.state().metadata().getProject(projectId)));

            // and the index associated with the other index should be unaffected
            indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT).setIndices(EnrichPolicy.getIndexName(otherName, 1001)).get();
        }
    }
}
