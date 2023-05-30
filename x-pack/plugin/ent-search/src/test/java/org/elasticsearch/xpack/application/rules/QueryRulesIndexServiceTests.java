/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.engine.VersionConflictEngineException;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.application.rules.QueryRule.QueryRuleType;
import static org.elasticsearch.xpack.application.rules.QueryRuleIndexService.QUERY_RULE_CONCRETE_INDEX_NAME;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;

public class QueryRulesIndexServiceTests extends ESSingleNodeTestCase {

    private QueryRuleIndexService queryRuleIndexService;
    private ClusterService clusterService;

    @Before
    public void setup() throws Exception {
        clusterService = getInstanceFromNode(ClusterService.class);
        BigArrays bigArrays = getInstanceFromNode(BigArrays.class);
        this.queryRuleIndexService = new QueryRuleIndexService(client(), clusterService, writableRegistry(), bigArrays);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(TestPlugin.class);
        return plugins;
    }

    public void testEmptyState() throws Exception {
        expectThrows(ResourceNotFoundException.class, () -> awaitGetQueryRule("i-dont-exist"));
        expectThrows(ResourceNotFoundException.class, () -> awaitDeleteQueryRule("i-dont-exist"));

        QueryRuleIndexService.QueryRuleResult listResults = awaitListQueryRules(0, 10);
        assertThat(listResults.totalResults(), equalTo(0L));
    }

    public void testCreateQueryRule() throws Exception {
        final QueryRule myQueryRule = new QueryRule("my_rule", QueryRuleType.PINNED);

        IndexResponse resp = awaitPutQueryRule(myQueryRule, true);
        assertThat(resp.status(), equalTo(RestStatus.CREATED));
        assertThat(resp.getIndex(), equalTo(QUERY_RULE_CONCRETE_INDEX_NAME));

        QueryRule getQueryRule = awaitGetQueryRule(myQueryRule.id());
        assertThat(getQueryRule, equalTo(myQueryRule));

        expectThrows(VersionConflictEngineException.class, () -> awaitPutQueryRule(myQueryRule, true));
    }

    public void testUpdateQueryRule() throws Exception {
        {
            final QueryRule myQueryRule = new QueryRule("my_rule", QueryRuleType.PINNED);
            IndexResponse resp = awaitPutQueryRule(myQueryRule, false);
            assertThat(resp.status(), equalTo(RestStatus.CREATED));
            assertThat(resp.getIndex(), equalTo(QUERY_RULE_CONCRETE_INDEX_NAME));

            QueryRule getQueryRule = awaitGetQueryRule(myQueryRule.id());
            assertThat(getQueryRule, equalTo(myQueryRule));
        }

        // TODO update with new values
        final QueryRule myQueryRule = new QueryRule("my_rule", QueryRuleType.PINNED);
        IndexResponse newResp = awaitPutQueryRule(myQueryRule, false);
        assertThat(newResp.status(), equalTo(RestStatus.OK));
        assertThat(newResp.getIndex(), equalTo(QUERY_RULE_CONCRETE_INDEX_NAME));
        QueryRule getQueryRule = awaitGetQueryRule(myQueryRule.id());
        assertThat(getQueryRule, equalTo(myQueryRule));
    }

    public void testListQueryRules() throws Exception {
        int numRules = 10;
        for (int i = 0; i < numRules; i++) {
            final QueryRule myQueryRule = new QueryRule("my_rule_" + i, QueryRuleType.PINNED);
            IndexResponse resp = awaitPutQueryRule(myQueryRule, false);
            assertThat(resp.status(), equalTo(RestStatus.CREATED));
            assertThat(resp.getIndex(), equalTo(QUERY_RULE_CONCRETE_INDEX_NAME));
        }

        {
            QueryRuleIndexService.QueryRuleResult searchResponse = awaitListQueryRules(0, 10);
            final List<QueryRule> rules = searchResponse.items();
            assertNotNull(rules);
            assertThat(rules.size(), equalTo(10));
            assertThat(searchResponse.totalResults(), equalTo(10L));

            for (int i = 0; i < numRules; i++) {
                QueryRule rule = rules.get(i);
                assertThat(rule.id(), equalTo("my_rule_" + i));
                assertThat(rule.type(), equalTo(QueryRuleType.PINNED));
            }
        }

        {
           QueryRuleIndexService.QueryRuleResult searchResponse = awaitListQueryRules(5, 10);
            final List<QueryRule> rules = searchResponse.items();
            assertNotNull(rules);
            assertThat(rules.size(), equalTo(5));
            assertThat(searchResponse.totalResults(), equalTo(10L));

            for (int i = 0; i < 5; i++) {
                int index = i + 5;
                QueryRule rule = rules.get(i);
                assertThat(rule.id(), equalTo("my_rule_" + index));
                assertThat(rule.type(), equalTo(QueryRuleType.PINNED));
            }
        }
    }

    public void testDeleteQueryRule() throws Exception {
        for (int i = 0; i < 5; i++) {
            final QueryRule myQueryRule = new QueryRule("my_rule", QueryRuleType.PINNED);
            IndexResponse resp = awaitPutQueryRule(myQueryRule, false);
            assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));
            assertThat(resp.getIndex(), equalTo(QUERY_RULE_CONCRETE_INDEX_NAME));

            QueryRule getQueryRule = awaitGetQueryRule(myQueryRule.id());
            assertThat(getQueryRule, equalTo(myQueryRule));
        }

        DeleteResponse resp = awaitDeleteQueryRule("my_rule");
        assertThat(resp.status(), equalTo(RestStatus.OK));
        expectThrows(ResourceNotFoundException.class, () -> awaitGetQueryRule("my_rule"));
    }

    private IndexResponse awaitPutQueryRule(QueryRule queryRule, boolean create) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<IndexResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        queryRuleIndexService.putQueryRule(queryRule, create, new ActionListener<>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                resp.set(indexResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull(resp.get());
        return resp.get();
    }

    private QueryRule awaitGetQueryRule(String name) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<QueryRule> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        queryRuleIndexService.getQueryRule(name, new ActionListener<>() {
            @Override
            public void onResponse(QueryRule rule) {
                resp.set(rule);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull(resp.get());
        return resp.get();
    }

private DeleteResponse awaitDeleteQueryRule(String name) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<DeleteResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        queryRuleIndexService.deleteQueryRule(name, new ActionListener<>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                resp.set(deleteResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull(resp.get());
        return resp.get();
    }

private QueryRuleIndexService.QueryRuleResult awaitListQueryRules(int from, int size)
        throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<QueryRuleIndexService.QueryRuleResult> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        queryRuleIndexService.listQueryRules(from, size, new ActionListener<>() {
            @Override
            public void onResponse(QueryRuleIndexService.QueryRuleResult result) {
                resp.set(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue(latch.await(5, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull(resp.get());
        return resp.get();
    }

    /**
     * Test plugin to register the {@link QueryRuleIndexService} system index descriptor.
     */
    public static class TestPlugin extends Plugin implements SystemIndexPlugin {
        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(QueryRuleIndexService.getSystemIndexDescriptor());
        }

        @Override
        public String getFeatureName() {
            return this.getClass().getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return this.getClass().getCanonicalName();
        }
    }
}
