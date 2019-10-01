/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractEnrichTestCase extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(LocalStateEnrich.class);
    }

    protected EnrichPolicy getPolicySync(String name, ClusterState state) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<EnrichPolicy> policy  = new AtomicReference<>();
        EnrichStore.getPolicy(name, state, client(), ActionListener.wrap(
            (p) -> {
                if (p != null) {
                    policy.set(p.getPolicy());
                }
                latch.countDown();
            },
            (exc) -> {
                fail();
            }
        ));
        latch.await();
        return policy.get();
    }

    protected Collection<EnrichPolicy.NamedPolicy> getPoliciesSync(ClusterState state) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Collection<EnrichPolicy.NamedPolicy>> policy  = new AtomicReference<>();
        EnrichStore.getPolicies(state, client(), ActionListener.wrap(
            (p) -> {
                policy.set(p);
                latch.countDown();
            },
            (exc) -> {
                fail();
            }
        ));
        latch.await();
        return policy.get();
    }

    protected AtomicReference<Exception> saveEnrichPolicy(String name, EnrichPolicy policy,
                                                          ClusterService clusterService) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        EnrichStore.putPolicy(name, policy, clusterService, client(), e -> {
            error.set(e);
            latch.countDown();
        });
        latch.await();
        return error;
    }

    protected void deleteEnrichPolicy(String name, ClusterService clusterService) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        EnrichStore.deletePolicy(name, clusterService, client(), e -> {
            error.set(e);
            latch.countDown();
        });
        latch.await();
        if (error.get() != null){
            throw error.get();
        }
    }
}
