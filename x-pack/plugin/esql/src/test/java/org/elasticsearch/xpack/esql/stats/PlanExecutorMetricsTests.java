/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlQueryRequest;
import org.elasticsearch.xpack.esql.analysis.VerificationException;
import org.elasticsearch.xpack.esql.enrich.EnrichPolicyResolver;
import org.elasticsearch.xpack.esql.execution.PlanExecutor;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.type.EsqlDataTypeRegistry;
import org.elasticsearch.xpack.ql.index.IndexResolver;
import org.junit.After;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PlanExecutorMetricsTests extends ESTestCase {

    private ThreadPool threadPool;

    @Before
    public void setUpThreadPool() throws Exception {
        threadPool = new TestThreadPool(PlanExecutorMetricsTests.class.getSimpleName());
    }

    @After
    public void shutdownThreadPool() throws Exception {
        terminate(threadPool);
    }

    public void testFailedMetric() {
        Client client = mock(Client.class);
        IndexResolver idxResolver = new IndexResolver(client, randomAlphaOfLength(10), EsqlDataTypeRegistry.INSTANCE, Set::of);
        var planExecutor = new PlanExecutor(idxResolver, new EnrichPolicyResolver(null, idxResolver, threadPool));
        String[] indices = new String[] { "test" };

        // simulate a valid field_caps response so we can parse and correctly analyze de query
        FieldCapabilitiesResponse fieldCapabilitiesResponse = mock(FieldCapabilitiesResponse.class);
        when(fieldCapabilitiesResponse.getIndices()).thenReturn(indices);
        when(fieldCapabilitiesResponse.get()).thenReturn(fields(indices));
        doAnswer((Answer<Void>) invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<FieldCapabilitiesResponse> listener = (ActionListener<FieldCapabilitiesResponse>) invocation.getArguments()[1];
            listener.onResponse(fieldCapabilitiesResponse);
            return null;
        }).when(client).fieldCaps(any(), any());

        var request = new EsqlQueryRequest();
        // test a failed query: xyz field doesn't exist
        request.query("from test | stats m = max(xyz)");
        planExecutor.esql(request, randomAlphaOfLength(10), EsqlTestUtils.TEST_CFG, new ActionListener<PhysicalPlan>() {
            @Override
            public void onResponse(PhysicalPlan physicalPlan) {
                fail("this shouldn't happen");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, instanceOf(VerificationException.class));
            }
        });

        // check we recorded the failure and that the query actually came
        assertEquals(1, planExecutor.metrics().stats().get("queries._all.failed"));
        assertEquals(1, planExecutor.metrics().stats().get("queries._all.total"));
        assertEquals(0, planExecutor.metrics().stats().get("features.stats"));

        // fix the failing query: foo field does exist
        request.query("from test | stats m = max(foo)");
        planExecutor.esql(request, randomAlphaOfLength(10), EsqlTestUtils.TEST_CFG, new ActionListener<PhysicalPlan>() {
            @Override
            public void onResponse(PhysicalPlan physicalPlan) {}

            @Override
            public void onFailure(Exception e) {
                fail("this shouldn't happen");
            }
        });

        // check the new metrics
        assertEquals(1, planExecutor.metrics().stats().get("queries._all.failed"));
        assertEquals(2, planExecutor.metrics().stats().get("queries._all.total"));
        assertEquals(1, planExecutor.metrics().stats().get("features.stats"));
    }

    private Map<String, Map<String, FieldCapabilities>> fields(String[] indices) {
        FieldCapabilities fooField = new FieldCapabilities("foo", "integer", false, true, true, indices, null, null, emptyMap());
        FieldCapabilities barField = new FieldCapabilities("bar", "long", false, true, true, indices, null, null, emptyMap());
        Map<String, Map<String, FieldCapabilities>> fields = new HashMap<>();
        fields.put(fooField.getName(), singletonMap(fooField.getName(), fooField));
        fields.put(barField.getName(), singletonMap(barField.getName(), barField));
        return fields;
    }
}
