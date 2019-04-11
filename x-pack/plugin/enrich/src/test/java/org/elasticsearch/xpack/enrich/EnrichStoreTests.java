/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.enrich;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.enrich.EnrichPolicy.Type;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class EnrichStoreTests extends ESSingleNodeTestCase {

    public void testCrud() throws Exception {
        EnrichStore enrichStore = new EnrichStore(getInstanceFromNode(ClusterService.class));

        EnrichPolicy policy = new EnrichPolicy(Type.STRING, TimeValue.timeValueHours(1), "source_index", "query_field",
            Arrays.asList("field1", "field2"));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> error = new AtomicReference<>();
        enrichStore.putPolicy("my-policy", policy, e -> {
            error.set(e);
            latch.countDown();
        });
        latch.await();
        assertThat(error.get(), nullValue());

        EnrichPolicy result = enrichStore.getPolicy("my-policy");
        assertThat(result, equalTo(policy));
    }

}
