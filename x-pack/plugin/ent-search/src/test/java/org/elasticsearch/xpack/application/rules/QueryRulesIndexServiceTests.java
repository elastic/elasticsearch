/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.rules;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.exception.ResourceNotFoundException;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.application.EnterpriseSearchModuleTestUtils;
import org.elasticsearch.xpack.application.rules.action.DeleteQueryRuleAction;
import org.elasticsearch.xpack.application.rules.action.PutQueryRuleAction;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.application.rules.QueryRule.QueryRuleType;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType.EXACT;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType.FUZZY;
import static org.elasticsearch.xpack.application.rules.QueryRuleCriteriaType.GTE;
import static org.elasticsearch.xpack.application.rules.QueryRulesIndexService.QUERY_RULES_CONCRETE_INDEX_NAME;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;

public class QueryRulesIndexServiceTests extends ESSingleNodeTestCase {

    private static final int REQUEST_TIMEOUT_SECONDS = 10;

    private QueryRulesIndexService queryRulesIndexService;

    @Before
    public void setup() {
        Set<Setting<?>> settingsSet = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingsSet.addAll(QueryRulesConfig.getSettings());
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, settingsSet);
        this.queryRulesIndexService = new QueryRulesIndexService(client(), clusterSettings);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        List<Class<? extends Plugin>> plugins = new ArrayList<>(super.getPlugins());
        plugins.add(TestPlugin.class);
        return plugins;
    }

    public void testEmptyState() throws Exception {
        expectThrows(ResourceNotFoundException.class, () -> awaitGetQueryRuleset("i-dont-exist"));
        expectThrows(ResourceNotFoundException.class, () -> awaitDeleteQueryRuleset("i-dont-exist"));

        QueryRulesIndexService.QueryRulesetResult listResults = awaitListQueryRulesets(0, 10);
        assertThat(listResults.totalResults(), equalTo(0L));
    }

    public void testUpdateQueryRuleset() throws Exception {
        {
            final QueryRule myQueryRule1 = new QueryRule(
                "my_rule1",
                QueryRuleType.PINNED,
                List.of(new QueryRuleCriteria(EXACT, "query_string", List.of("foo"))),
                Map.of("ids", List.of("id1", "id2")),
                EnterpriseSearchModuleTestUtils.randomQueryRulePriority()
            );
            final QueryRuleset myQueryRuleset = new QueryRuleset("my_ruleset", Collections.singletonList(myQueryRule1));
            DocWriteResponse resp = awaitPutQueryRuleset(myQueryRuleset);
            assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));
            assertThat(resp.getIndex(), equalTo(QUERY_RULES_CONCRETE_INDEX_NAME));

            QueryRuleset getQueryRuleset = awaitGetQueryRuleset(myQueryRuleset.id());
            assertThat(getQueryRuleset, equalTo(myQueryRuleset));
        }

        final QueryRule myQueryRule1 = new QueryRule(
            "my_rule1",
            QueryRuleType.PINNED,
            List.of(new QueryRuleCriteria(EXACT, "query_string", List.of("foo"))),
            Map.of("docs", List.of(Map.of("_index", "my_index1", "_id", "id1"), Map.of("_index", "my_index2", "_id", "id2"))),
            EnterpriseSearchModuleTestUtils.randomQueryRulePriority()
        );
        final QueryRule myQueryRule2 = new QueryRule(
            "my_rule2",
            QueryRuleType.PINNED,
            List.of(new QueryRuleCriteria(EXACT, "query_string", List.of("bar"))),
            Map.of("docs", List.of(Map.of("_index", "my_index1", "_id", "id3"), Map.of("_index", "my_index2", "_id", "id4"))),
            EnterpriseSearchModuleTestUtils.randomQueryRulePriority()
        );
        final QueryRuleset myQueryRuleset = new QueryRuleset("my_ruleset", List.of(myQueryRule1, myQueryRule2));
        DocWriteResponse newResp = awaitPutQueryRuleset(myQueryRuleset);
        assertThat(newResp.status(), equalTo(RestStatus.OK));
        assertThat(newResp.getIndex(), equalTo(QUERY_RULES_CONCRETE_INDEX_NAME));
        QueryRuleset getQueryRuleset = awaitGetQueryRuleset(myQueryRuleset.id());
        assertThat(getQueryRuleset, equalTo(myQueryRuleset));
    }

    public void testUpdateQueryRule() throws Exception {
        // Creating a rule in a nonexistent ruleset creates the ruleset
        final QueryRule myQueryRule1 = new QueryRule(
            "my_rule1",
            QueryRuleType.PINNED,
            List.of(new QueryRuleCriteria(EXACT, "query_string", List.of("foo"))),
            Map.of("docs", List.of(Map.of("_index", "my_index1", "_id", "id1"), Map.of("_index", "my_index2", "_id", "id2"))),
            EnterpriseSearchModuleTestUtils.randomQueryRulePriority()
        );
        final String rulesetId = "my_ruleset";
        PutQueryRuleAction.Response newResp = awaitPutQueryRule(rulesetId, myQueryRule1);
        assertThat(newResp.status(), equalTo(RestStatus.CREATED));

        QueryRuleset getQueryRuleset = awaitGetQueryRuleset(rulesetId);
        assertThat(getQueryRuleset, equalTo(new QueryRuleset("my_ruleset", List.of(myQueryRule1))));
        QueryRule getQueryRule = awaitGetQueryRule(rulesetId, "my_rule1");
        assertThat(getQueryRule, equalTo(myQueryRule1));

        // Updating the same query rule in the ruleset returns OK instead of CREATED
        final QueryRule updatedQueryRule1 = new QueryRule(
            "my_rule1",
            QueryRuleType.PINNED,
            List.of(new QueryRuleCriteria(EXACT, "query_string", List.of("bar"))),
            Map.of("docs", List.of(Map.of("_index", "my_index1", "_id", "id2"), Map.of("_index", "my_index2", "_id", "id1"))),
            EnterpriseSearchModuleTestUtils.randomQueryRulePriority()
        );
        PutQueryRuleAction.Response updateResp = awaitPutQueryRule(rulesetId, updatedQueryRule1);
        assertThat(updateResp.status(), equalTo(RestStatus.OK));

        QueryRuleset getUpdatedQueryRuleset = awaitGetQueryRuleset(rulesetId);
        assertThat(getUpdatedQueryRuleset, equalTo(new QueryRuleset("my_ruleset", List.of(updatedQueryRule1))));
        QueryRule getUpdatedQueryRule = awaitGetQueryRule(rulesetId, "my_rule1");
        assertThat(getUpdatedQueryRule, equalTo(updatedQueryRule1));

        // Creating a new rule in an existing ruleset
        final QueryRule myQueryRule2 = new QueryRule(
            "my_rule2",
            QueryRuleType.PINNED,
            List.of(new QueryRuleCriteria(EXACT, "query_string", List.of("bar"))),
            Map.of("docs", List.of(Map.of("_index", "my_index1", "_id", "id3"), Map.of("_index", "my_index2", "_id", "id4"))),
            EnterpriseSearchModuleTestUtils.randomQueryRulePriority()
        );

        PutQueryRuleAction.Response addResp = awaitPutQueryRule(rulesetId, myQueryRule2);
        assertThat(addResp.status(), equalTo(RestStatus.CREATED));

        QueryRuleset getQueryRuleset2 = awaitGetQueryRuleset(rulesetId);
        assertThat(getQueryRuleset2, equalTo(new QueryRuleset("my_ruleset", List.of(updatedQueryRule1, myQueryRule2))));
        QueryRule getQueryRule2 = awaitGetQueryRule(rulesetId, "my_rule2");
        assertThat(getQueryRule2, equalTo(myQueryRule2));
    }

    public void testListQueryRulesets() throws Exception {
        int numRulesets = 10;
        for (int i = 0; i < numRulesets; i++) {
            final List<QueryRule> rules = List.of(
                new QueryRule(
                    "my_rule_" + i,
                    QueryRuleType.PINNED,
                    List.of(
                        new QueryRuleCriteria(EXACT, "query_string", List.of("foo" + i)),
                        new QueryRuleCriteria(GTE, "query_string", List.of(i))
                    ),
                    Map.of("ids", List.of("id1", "id2")),
                    EnterpriseSearchModuleTestUtils.randomQueryRulePriority()
                ),
                new QueryRule(
                    "my_rule_" + i + "_" + (i + 1),
                    QueryRuleType.PINNED,
                    List.of(
                        new QueryRuleCriteria(FUZZY, "query_string", List.of("bar" + i)),
                        new QueryRuleCriteria(GTE, "user.age", List.of(i))
                    ),
                    Map.of("ids", List.of("id3", "id4")),
                    EnterpriseSearchModuleTestUtils.randomQueryRulePriority()
                )
            );
            final QueryRuleset myQueryRuleset = new QueryRuleset("my_ruleset_" + i, rules);

            DocWriteResponse resp = awaitPutQueryRuleset(myQueryRuleset);
            assertThat(resp.status(), equalTo(RestStatus.CREATED));
            assertThat(resp.getIndex(), equalTo(QUERY_RULES_CONCRETE_INDEX_NAME));
        }

        {
            QueryRulesIndexService.QueryRulesetResult searchResponse = awaitListQueryRulesets(0, 10);
            final List<QueryRulesetListItem> rulesets = searchResponse.rulesets();
            assertNotNull(rulesets);
            assertThat(rulesets.size(), equalTo(10));
            assertThat(searchResponse.totalResults(), equalTo(10L));

            for (int i = 0; i < numRulesets; i++) {
                String rulesetId = rulesets.get(i).rulesetId();
                assertThat(rulesetId, equalTo("my_ruleset_" + i));
            }
        }

        {
            QueryRulesIndexService.QueryRulesetResult searchResponse = awaitListQueryRulesets(5, 10);
            final List<QueryRulesetListItem> rulesets = searchResponse.rulesets();
            assertNotNull(rulesets);
            assertThat(rulesets.size(), equalTo(5));
            assertThat(searchResponse.totalResults(), equalTo(10L));

            for (int i = 0; i < 5; i++) {
                int index = i + 5;
                QueryRulesetListItem ruleset = rulesets.get(i);
                String rulesetId = ruleset.rulesetId();
                assertThat(rulesetId, equalTo("my_ruleset_" + index));
                Map<QueryRuleCriteriaType, Integer> criteriaTypeCountMap = ruleset.criteriaTypeToCountMap();
                assertThat(criteriaTypeCountMap.size(), equalTo(3));
                assertThat(criteriaTypeCountMap.get(EXACT), equalTo(1));
                assertThat(criteriaTypeCountMap.get(FUZZY), equalTo(1));
                assertThat(criteriaTypeCountMap.get(GTE), equalTo(2));
            }
        }
    }

    public void testDeleteQueryRuleset() throws Exception {
        for (int i = 0; i < 5; i++) {
            final QueryRule myQueryRule1 = new QueryRule(
                "my_rule1",
                QueryRuleType.PINNED,
                List.of(new QueryRuleCriteria(EXACT, "query_string", List.of("foo"))),
                Map.of("ids", List.of("id1", "id2")),
                EnterpriseSearchModuleTestUtils.randomQueryRulePriority()
            );
            final QueryRule myQueryRule2 = new QueryRule(
                "my_rule2",
                QueryRuleType.PINNED,
                List.of(new QueryRuleCriteria(EXACT, "query_string", List.of("bar"))),
                Map.of("ids", List.of("id3", "id4")),
                EnterpriseSearchModuleTestUtils.randomQueryRulePriority()
            );
            final QueryRuleset myQueryRuleset = new QueryRuleset("my_ruleset", List.of(myQueryRule1, myQueryRule2));
            DocWriteResponse resp = awaitPutQueryRuleset(myQueryRuleset);
            assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));
            assertThat(resp.getIndex(), equalTo(QUERY_RULES_CONCRETE_INDEX_NAME));

            QueryRuleset getQueryRuleset = awaitGetQueryRuleset(myQueryRuleset.id());
            assertThat(getQueryRuleset, equalTo(myQueryRuleset));
        }

        DeleteResponse resp = awaitDeleteQueryRuleset("my_ruleset");
        assertThat(resp.status(), equalTo(RestStatus.OK));
        expectThrows(ResourceNotFoundException.class, () -> awaitGetQueryRuleset("my_ruleset"));
    }

    public void testDeleteQueryRule() throws Exception {
        for (int i = 0; i < 5; i++) {
            final QueryRule myQueryRule1 = new QueryRule(
                "my_rule1",
                QueryRuleType.PINNED,
                List.of(new QueryRuleCriteria(EXACT, "query_string", List.of("foo"))),
                Map.of("ids", List.of("id1", "id2")),
                randomBoolean() ? randomIntBetween(0, 100) : null
            );
            final QueryRule myQueryRule2 = new QueryRule(
                "my_rule2",
                QueryRuleType.PINNED,
                List.of(new QueryRuleCriteria(EXACT, "query_string", List.of("bar"))),
                Map.of("ids", List.of("id3", "id4")),
                randomBoolean() ? randomIntBetween(0, 100) : null
            );
            final QueryRuleset myQueryRuleset = new QueryRuleset("my_ruleset", List.of(myQueryRule1, myQueryRule2));
            DocWriteResponse resp = awaitPutQueryRuleset(myQueryRuleset);
            assertThat(resp.status(), anyOf(equalTo(RestStatus.CREATED), equalTo(RestStatus.OK)));
            assertThat(resp.getIndex(), equalTo(QUERY_RULES_CONCRETE_INDEX_NAME));

            QueryRule getQueryRule = awaitGetQueryRule("my_ruleset", "my_rule1");
            assertThat(getQueryRule, equalTo(myQueryRule1));

            DeleteQueryRuleAction.Response deleteResp = awaitDeleteQueryRule("my_ruleset", "my_rule1");
            assertThat(deleteResp.isAcknowledged(), equalTo(true));
            expectThrows(ResourceNotFoundException.class, () -> awaitGetQueryRule("my_ruleset", "my_rule1"));

            QueryRule getQueryRule2 = awaitGetQueryRule("my_ruleset", "my_rule2");
            assertThat(getQueryRule2, equalTo(myQueryRule2));
        }

        // Deleting the last rule in the ruleset should delete the ruleset
        DeleteQueryRuleAction.Response deleteResp = awaitDeleteQueryRule("my_ruleset", "my_rule2");
        assertThat(deleteResp.isAcknowledged(), equalTo(true));
        expectThrows(ResourceNotFoundException.class, () -> awaitGetQueryRule("my_ruleset", "my_rule2"));
        expectThrows(ResourceNotFoundException.class, () -> awaitGetQueryRuleset("my_ruleset"));
    }

    private DocWriteResponse awaitPutQueryRuleset(QueryRuleset queryRuleset) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<DocWriteResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        queryRulesIndexService.putQueryRuleset(queryRuleset, new ActionListener<>() {
            @Override
            public void onResponse(DocWriteResponse indexResponse) {
                resp.set(indexResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue("Timeout waiting for put request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from put request", resp.get());
        return resp.get();
    }

    private PutQueryRuleAction.Response awaitPutQueryRule(String queryRulesetId, QueryRule queryRule) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<PutQueryRuleAction.Response> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        queryRulesIndexService.putQueryRule(queryRulesetId, queryRule, new ActionListener<>() {
            @Override
            public void onResponse(PutQueryRuleAction.Response indexResponse) {
                resp.set(indexResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue("Timeout waiting for put request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from put request", resp.get());
        return resp.get();
    }

    private QueryRuleset awaitGetQueryRuleset(String name) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<QueryRuleset> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        queryRulesIndexService.getQueryRuleset(name, new ActionListener<>() {
            @Override
            public void onResponse(QueryRuleset ruleset) {
                resp.set(ruleset);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue("Timeout waiting for get request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from get request", resp.get());
        return resp.get();
    }

    private QueryRule awaitGetQueryRule(String rulesetId, String ruleId) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<QueryRule> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        queryRulesIndexService.getQueryRule(rulesetId, ruleId, new ActionListener<>() {
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
        assertTrue("Timeout waiting for get request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from get request", resp.get());
        return resp.get();
    }

    private DeleteResponse awaitDeleteQueryRuleset(String name) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<DeleteResponse> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        queryRulesIndexService.deleteQueryRuleset(name, new ActionListener<>() {
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
        assertTrue("Timeout waiting for delete request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from delete request", resp.get());
        return resp.get();
    }

    private DeleteQueryRuleAction.Response awaitDeleteQueryRule(String rulesetId, String ruleId) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<DeleteQueryRuleAction.Response> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        queryRulesIndexService.deleteQueryRule(rulesetId, ruleId, new ActionListener<>() {
            @Override
            public void onResponse(DeleteQueryRuleAction.Response deleteResponse) {
                resp.set(deleteResponse);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue("Timeout waiting for delete request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from delete request", resp.get());
        return resp.get();
    }

    private QueryRulesIndexService.QueryRulesetResult awaitListQueryRulesets(int from, int size) throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<QueryRulesIndexService.QueryRulesetResult> resp = new AtomicReference<>(null);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        queryRulesIndexService.listQueryRulesets(from, size, new ActionListener<>() {
            @Override
            public void onResponse(QueryRulesIndexService.QueryRulesetResult result) {
                resp.set(result);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exc.set(e);
                latch.countDown();
            }
        });
        assertTrue("Timeout waiting for list request", latch.await(REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS));
        if (exc.get() != null) {
            throw exc.get();
        }
        assertNotNull("Received null response from list request", resp.get());
        return resp.get();
    }

    /**
     * Test plugin to register the {@link QueryRulesIndexService} system index descriptor.
     */
    public static class TestPlugin extends Plugin implements SystemIndexPlugin {
        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(QueryRulesIndexService.getSystemIndexDescriptor());
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
