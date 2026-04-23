/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.synonyms;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.indices.IndexCreationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.synonyms.SynonymsTestUtils.randomSynonymRule;
import static org.elasticsearch.action.synonyms.SynonymsTestUtils.randomSynonymsSet;
import static org.hamcrest.Matchers.containsString;

public class SynonymsManagementAPIServiceIT extends ESIntegTestCase {

    private SynonymsManagementAPIService synonymsManagementAPIService;
    private int maxSynonymRules;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class, MapperExtrasPlugin.class);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        maxSynonymRules = randomIntBetween(100, 1000);
        synonymsManagementAPIService = new SynonymsManagementAPIService(
            client(),
            clusterService(),
            maxSynonymRules,
            SynonymsManagementAPIService.PIT_BATCH_SIZE,
            SynonymsManagementAPIService.BULK_CHUNK_SIZE
        );
    }

    public void testCreateManySynonyms() throws Exception {
        String synonymSetId = randomIdentifier();
        boolean refresh = randomBoolean();
        int rulesNumber = randomIntBetween(maxSynonymRules / 2, maxSynonymRules);

        PlainActionFuture<SynonymsManagementAPIService.SynonymsReloadResult> putFuture = new PlainActionFuture<>();
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, randomSynonymsSet(rulesNumber, rulesNumber), refresh, putFuture);
        SynonymsManagementAPIService.SynonymsReloadResult putResult = safeGet(putFuture);
        assertEquals(SynonymsManagementAPIService.UpdateSynonymsResultStatus.CREATED, putResult.synonymsOperationResult());
        assertEquals(refresh, putResult.reloadAnalyzersResponse() != null);

        // Also retrieve them
        PlainActionFuture<PagedResult<SynonymRule>> getFuture = new PlainActionFuture<>();
        synonymsManagementAPIService.getSynonymSetRules(synonymSetId, 0, maxSynonymRules, getFuture);
        PagedResult<SynonymRule> getResult = safeGet(getFuture);
        assertEquals(rulesNumber, getResult.totalResults());
        assertEquals(rulesNumber, getResult.pageResults().length);
    }

    public void testGetAllSynonymSetRulesViaPit() throws Exception {
        int pitBatchSize = randomIntBetween(2, 5);
        int rulesNumber = pitBatchSize * randomIntBetween(3, 6);
        int maxRules = randomBoolean() ? rulesNumber : Integer.MAX_VALUE;
        SynonymsManagementAPIService synsApiService = new SynonymsManagementAPIService(
            client(),
            clusterService(),
            maxRules,
            pitBatchSize,
            SynonymsManagementAPIService.BULK_CHUNK_SIZE
        );

        String synonymSetId = randomIdentifier();
        PlainActionFuture<SynonymsManagementAPIService.SynonymsReloadResult> putFuture = new PlainActionFuture<>();
        synsApiService.putSynonymsSet(synonymSetId, randomSynonymsSet(rulesNumber, rulesNumber), false, putFuture);
        assertEquals(SynonymsManagementAPIService.UpdateSynonymsResultStatus.CREATED, safeGet(putFuture).synonymsOperationResult());

        PlainActionFuture<PagedResult<SynonymRule>> getFuture = new PlainActionFuture<>();
        synsApiService.getSynonymSetRules(synonymSetId, getFuture);
        PagedResult<SynonymRule> result = safeGet(getFuture);
        assertEquals(rulesNumber, result.totalResults());
        assertEquals(rulesNumber, result.pageResults().length);
    }

    public void testPitCapEnforcedWhenRulesExceedLimit() throws Exception {
        // This tests the case where there is no write limit or someone
        // has bypassed the write limit.
        int pitBatchSize = randomIntBetween(2, 5);
        int maxRules = pitBatchSize * randomIntBetween(2, 4);
        int rulesNumber = maxRules + randomIntBetween(1, pitBatchSize);
        SynonymsManagementAPIService synsApiService = new SynonymsManagementAPIService(
            client(),
            clusterService(),
            maxRules,
            pitBatchSize,
            SynonymsManagementAPIService.BULK_CHUNK_SIZE
        );

        String synonymSetId = randomIdentifier();
        // Use bulkUpdateSynonymsSet to bypass the write-time limit check so we can store more than maxRules
        PlainActionFuture<Void> putFuture = new PlainActionFuture<>();
        synsApiService.bulkUpdateSynonymsSet(synonymSetId, randomSynonymsSet(rulesNumber, rulesNumber), putFuture);
        safeGet(putFuture);

        try (var mockLog = MockLog.capture(SynonymsManagementAPIService.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "warning",
                    SynonymsManagementAPIService.class.getName(),
                    Level.WARN,
                    "The number of synonym rules in the synonym set [" + synonymSetId + "] exceeds the maximum allowed*"
                )
            );
            PlainActionFuture<PagedResult<SynonymRule>> getFuture = new PlainActionFuture<>();
            synsApiService.getSynonymSetRules(synonymSetId, getFuture);
            PagedResult<SynonymRule> result = safeGet(getFuture);
            assertEquals(rulesNumber, result.totalResults());   // true total from index
            assertEquals(maxRules, result.pageResults().length); // capped at limit
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testCreateTooManySynonymsAtOnce() throws Exception {
        PlainActionFuture<SynonymsManagementAPIService.SynonymsReloadResult> future = new PlainActionFuture<>();
        synonymsManagementAPIService.putSynonymsSet(
            randomIdentifier(),
            randomSynonymsSet(maxSynonymRules + 1, maxSynonymRules * 2),
            randomBoolean(),
            future
        );
        var ex = expectThrows(IllegalArgumentException.class, () -> future.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(ex.getMessage(), containsString("cannot exceed " + maxSynonymRules));
    }

    public void testCreateTooManySynonymsUsingRuleUpdates() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        int rulesToUpdate = randomIntBetween(1, 10);
        int synonymsToCreate = maxSynonymRules - rulesToUpdate;
        String synonymSetId = randomIdentifier();
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, randomSynonymsSet(synonymsToCreate), true, new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                // Create as many rules as should fail
                SynonymRule[] rules = randomSynonymsSet(atLeast(rulesToUpdate + 1));
                CountDownLatch updatedRulesLatch = new CountDownLatch(rulesToUpdate);
                for (int i = 0; i < rulesToUpdate; i++) {
                    synonymsManagementAPIService.putSynonymRule(synonymSetId, rules[i], randomBoolean(), new ActionListener<>() {
                        @Override
                        public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                            updatedRulesLatch.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            fail(e);
                        }
                    });
                }
                try {
                    updatedRulesLatch.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    fail(e);
                }

                // Updating more rules fails
                int rulesToInsert = rules.length - rulesToUpdate;
                CountDownLatch insertRulesLatch = new CountDownLatch(rulesToInsert);
                for (int i = rulesToUpdate; i < rulesToInsert; i++) {
                    synonymsManagementAPIService.putSynonymRule(
                        // Error here
                        synonymSetId,
                        rules[i],
                        randomBoolean(),
                        new ActionListener<>() {
                            @Override
                            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                                fail("Shouldn't have been able to update a rule");
                            }

                            @Override
                            public void onFailure(Exception e) {
                                if (e instanceof IllegalArgumentException == false) {
                                    fail(e);
                                }
                                updatedRulesLatch.countDown();
                            }
                        }
                    );
                }
                try {
                    insertRulesLatch.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    fail(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });

        latch.await(5, TimeUnit.SECONDS);
    }

    public void testUpdateRuleWithMaxSynonyms() throws Exception {
        String synonymSetId = randomIdentifier();
        SynonymRule[] synonymsSet = randomSynonymsSet(maxSynonymRules, maxSynonymRules);

        PlainActionFuture<SynonymsManagementAPIService.SynonymsReloadResult> putFuture = new PlainActionFuture<>();
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, synonymsSet, true, putFuture);
        safeGet(putFuture);

        // Updating an existing rule at max capacity should succeed
        PlainActionFuture<SynonymsManagementAPIService.SynonymsReloadResult> updateFuture = new PlainActionFuture<>();
        synonymsManagementAPIService.putSynonymRule(
            synonymSetId,
            synonymsSet[randomIntBetween(0, maxSynonymRules - 1)],
            randomBoolean(),
            updateFuture
        );
        safeGet(updateFuture);
    }

    public void testCreateRuleWithMaxSynonyms() throws Exception {
        String synonymSetId = randomIdentifier();
        String ruleId = randomIdentifier();
        SynonymRule[] synonymsSet = randomSynonymsSet(maxSynonymRules, maxSynonymRules);

        PlainActionFuture<SynonymsManagementAPIService.SynonymsReloadResult> putFuture = new PlainActionFuture<>();
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, synonymsSet, true, putFuture);
        safeGet(putFuture);

        // Creating a new rule when at max capacity should fail
        PlainActionFuture<SynonymsManagementAPIService.SynonymsReloadResult> createFuture = new PlainActionFuture<>();
        synonymsManagementAPIService.putSynonymRule(synonymSetId, randomSynonymRule(ruleId), randomBoolean(), createFuture);
        var ex = expectThrows(IllegalArgumentException.class, () -> createFuture.actionGet(TEST_REQUEST_TIMEOUT));
        assertThat(ex.getMessage(), containsString("cannot exceed " + maxSynonymRules));
    }

    public void testCreateSynonymsWithYellowSynonymsIndex() throws Exception {

        // Override health method check to simulate a timeout in checking the synonyms index
        synonymsManagementAPIService = new SynonymsManagementAPIService(
            client(),
            clusterService(),
            100_000,
            SynonymsManagementAPIService.PIT_BATCH_SIZE,
            SynonymsManagementAPIService.BULK_CHUNK_SIZE
        ) {
            @Override
            void checkSynonymsIndexHealth(ActionListener<ClusterHealthResponse> listener) {
                ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).build();
                ClusterHealthResponse response = new ClusterHealthResponse(randomIdentifier(), Strings.EMPTY_ARRAY, clusterState);
                response.setTimedOut(true);
                listener.onResponse(response);
            }
        };

        // Create a rule fails
        CountDownLatch putLatch = new CountDownLatch(1);
        String synonymSetId = randomIdentifier();
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, randomSynonymsSet(1, 1), true, new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                fail("Shouldn't have been able to create synonyms with refresh in synonyms index health");
            }

            @Override
            public void onFailure(Exception e) {
                // Expected
                assertTrue(e instanceof IndexCreationException);
                assertTrue(e.getMessage().contains("synonyms index [.synonyms] is not searchable"));
                putLatch.countDown();
            }
        });

        putLatch.await(5, TimeUnit.SECONDS);

        // Update a rule fails
        CountDownLatch updateLatch = new CountDownLatch(1);
        String synonymRuleId = randomIdentifier();
        synonymsManagementAPIService.putSynonymRule(synonymSetId, randomSynonymRule(synonymRuleId), true, new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                fail("Shouldn't have been able to update synonyms with refresh in synonyms index health");
            }

            @Override
            public void onFailure(Exception e) {
                // Expected
                assertTrue(e instanceof IndexCreationException);
                assertTrue(e.getMessage().contains("synonyms index [.synonyms] is not searchable"));
                updateLatch.countDown();
            }
        });

        updateLatch.await(5, TimeUnit.SECONDS);

        // Delete a rule does not fail
        CountDownLatch deleteLatch = new CountDownLatch(1);
        synonymsManagementAPIService.deleteSynonymRule(synonymSetId, synonymRuleId, true, new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                updateLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                // Expected
                fail("Should have been able to delete a synonym rule");
            }
        });

        deleteLatch.await(5, TimeUnit.SECONDS);

        // But, we can still create a synonyms set without refresh
        CountDownLatch putNoRefreshLatch = new CountDownLatch(1);
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, randomSynonymsSet(1, 1), false, new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                // Expected
                putLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });

        putNoRefreshLatch.await(5, TimeUnit.SECONDS);

        // Same for update
        CountDownLatch putRuleNoRefreshLatch = new CountDownLatch(1);
        synonymsManagementAPIService.putSynonymRule(synonymSetId, randomSynonymRule(synonymRuleId), false, new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                // Expected
                putRuleNoRefreshLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });

        putRuleNoRefreshLatch.await(5, TimeUnit.SECONDS);
    }
}
