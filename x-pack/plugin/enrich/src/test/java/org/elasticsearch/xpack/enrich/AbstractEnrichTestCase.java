/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

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

    protected AtomicReference<Exception> saveEnrichPolicy(String name, EnrichPolicy policy,
                                                          ClusterService clusterService) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        EnrichStore.putPolicy(name, policy, clusterService, e -> {
            error.set(e);
            latch.countDown();
        });
        latch.await();
        return error;
    }

    protected void deleteEnrichPolicy(String name, ClusterService clusterService) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        EnrichStore.deletePolicy(name, clusterService, e -> {
            error.set(e);
            latch.countDown();
        });
        latch.await();
        if (error.get() != null){
            throw error.get();
        }
    }
}
