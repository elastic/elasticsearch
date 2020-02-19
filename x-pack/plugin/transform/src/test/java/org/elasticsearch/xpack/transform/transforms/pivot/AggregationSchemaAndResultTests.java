/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.transform.transforms.pivot.AggregationConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.PivotConfig;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AggregationSchemaAndResultTests extends ESTestCase {

    private Client client;

    @Before
    public void setupClient() {
        if (client != null) {
            client.close();
        }
        client = new MyMockClient(getTestName());
    }

    @After
    public void tearDownClient() {
        client.close();
    }

    private class MyMockClient extends NoOpClient {

        MyMockClient(String testName) {
            super(testName);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(ActionType<Response> action,
                Request request, ActionListener<Response> listener) {

            if (request instanceof FieldCapabilitiesRequest) {
                FieldCapabilitiesRequest fieldCapsRequest = (FieldCapabilitiesRequest) request;

                Map<String, Map<String, FieldCapabilities>> fieldCaps = new HashMap<>();
                for (String field : fieldCapsRequest.fields()) {

                    // expect a field name like "double_field" where type is a prefix
                    String[] nameTypePair = Strings.split(field, "_");
                    String type = nameTypePair != null ? nameTypePair[0] : "long";

                    fieldCaps.put(field, Collections.singletonMap(type,
                            new FieldCapabilities(field, type, true, true, null, null, null, Collections.emptyMap())));
                }

                // FieldCapabilitiesResponse is package private, thats why we use a mock
                FieldCapabilitiesResponse response = mock(FieldCapabilitiesResponse.class);
                when(response.get()).thenReturn(fieldCaps);

                for (String field : fieldCaps.keySet()) {
                    when(response.getField(field)).thenReturn(fieldCaps.get(field));
                }

                listener.onResponse((Response) response);
                return;
            }

            super.doExecute(action, request, listener);
        }
    }

    public void testBasic() throws InterruptedException {

        AggregatorFactories.Builder aggs = new AggregatorFactories.Builder();
        aggs.addAggregator(AggregationBuilders.avg("avg_rating").field("long_stars"));
        aggs.addAggregator(AggregationBuilders.max("max_rating").field("long_stars"));

        AggregationConfig aggregationConfig = new AggregationConfig(Collections.emptyMap(), aggs);

        GroupConfig groupConfig = GroupConfigTests.randomGroupConfig();
        PivotConfig pivotConfig = new PivotConfig(groupConfig, aggregationConfig, null);
        this.<Map<String, String>>assertAsync(
                listener -> SchemaUtil.deduceMappings(client, pivotConfig, new String[] { "source-index" }, listener), response -> {
                    assertEquals(groupConfig.getGroups().size() + 2, response.size());

                });
    }

    private <T> void assertAsync(Consumer<ActionListener<T>> function, Consumer<T> tests) throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);

        LatchedActionListener<T> listener = new LatchedActionListener<>(ActionListener.wrap(r -> {
            assertTrue("listener called more than once", listenerCalled.compareAndSet(false, true));
            tests.accept(r);
        }, e -> {
            fail("got unexpected exception: " + e);

        }), latch);

        function.accept(listener);
        assertTrue("timed out after 20s", latch.await(20, TimeUnit.SECONDS));
    }

}
