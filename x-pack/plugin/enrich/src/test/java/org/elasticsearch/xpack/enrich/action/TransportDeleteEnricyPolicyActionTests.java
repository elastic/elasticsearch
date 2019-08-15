/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.enrich.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;
import org.elasticsearch.xpack.core.enrich.action.DeleteEnrichPolicyAction;
import org.elasticsearch.xpack.enrich.AbstractEnrichTestCase;
import org.elasticsearch.xpack.enrich.EnrichPolicyLocks;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.enrich.EnrichPolicyTests.randomEnrichPolicy;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.core.IsInstanceOf.instanceOf;

public class TransportDeleteEnricyPolicyActionTests extends AbstractEnrichTestCase {

    public void testDeleteIsNotLocked() throws InterruptedException {
        EnrichPolicy policy = randomEnrichPolicy(XContentType.JSON);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        String name = "my-policy";

        AtomicReference<Exception> error = saveEnrichPolicy(name, policy, clusterService);
        assertThat(error.get(), nullValue());

        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<AcknowledgedResponse> reference = new AtomicReference<>();
        final TransportDeleteEnrichPolicyAction transportAction = node().injector().getInstance(TransportDeleteEnrichPolicyAction.class);
        transportAction.execute(null,
            new DeleteEnrichPolicyAction.Request(name),
            new ActionListener<AcknowledgedResponse>() {
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
    }

    public void testDeleteLocked() throws InterruptedException {
        EnrichPolicy policy = randomEnrichPolicy(XContentType.JSON);
        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        String name = "my-policy";
        final TransportDeleteEnrichPolicyAction transportAction = node().injector().getInstance(TransportDeleteEnrichPolicyAction.class);
        AtomicReference<Exception> error = saveEnrichPolicy(name, policy, clusterService);
        assertThat(error.get(), nullValue());

        EnrichPolicyLocks enrichPolicyLocks = getInstanceFromNode(EnrichPolicyLocks.class);
        assertFalse(enrichPolicyLocks.captureExecutionState().isAnyPolicyInFlight());

        enrichPolicyLocks.lockPolicy(name);
        assertTrue(enrichPolicyLocks.captureExecutionState().isAnyPolicyInFlight());

        {
            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<Exception> reference = new AtomicReference<>();
            transportAction.execute(null,
                new DeleteEnrichPolicyAction.Request(name),
                new ActionListener<AcknowledgedResponse>() {
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
            assertThat(reference.get().getMessage(),
                equalTo("Could not obtain lock because policy execution for [my-policy] is already in progress."));
        }
        {
            enrichPolicyLocks.releasePolicy(name);
            assertFalse(enrichPolicyLocks.captureExecutionState().isAnyPolicyInFlight());

            final CountDownLatch latch = new CountDownLatch(1);
            final AtomicReference<AcknowledgedResponse> reference = new AtomicReference<>();

            transportAction.execute(null,
                new DeleteEnrichPolicyAction.Request(name),
                new ActionListener<AcknowledgedResponse>() {
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
        }
    }
}
