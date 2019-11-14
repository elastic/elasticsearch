/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.enrich.EnrichPolicy;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractEnrichTestCase extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(LocalStateEnrich.class);
    }

    protected AtomicReference<Exception> saveEnrichPolicy(String name, EnrichPolicy policy, ClusterService clusterService)
        throws InterruptedException {
        if (policy != null) {
            createSourceIndices(policy);
        }
        IndexNameExpressionResolver resolver = new IndexNameExpressionResolver();
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        EnrichStore.putPolicy(name, policy, clusterService, resolver, e -> {
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
        if (error.get() != null) {
            throw error.get();
        }
    }

    protected void createSourceIndices(EnrichPolicy policy) {
        createSourceIndices(client(), policy);
    }

    protected static void createSourceIndices(Client client, EnrichPolicy policy) {
        for (String sourceIndex : policy.getIndices()) {
            CreateIndexRequest createIndexRequest = new CreateIndexRequest(sourceIndex);
            createIndexRequest.mapping("_doc", policy.getMatchField(), "type=keyword");
            try {
                client.admin().indices().create(createIndexRequest).actionGet();
            } catch (ResourceAlreadyExistsException e) {
                // and that is okay
            }
        }
    }
}
