/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.synonyms;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
import org.elasticsearch.indices.IndexCreationException;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.junit.Before;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.synonyms.SynonymsTestUtils.randomSynonymRule;
import static org.elasticsearch.action.synonyms.SynonymsTestUtils.randomSynonymsSet;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class SynonymsManagementAPIServiceIT extends ESIntegTestCase {

    private SynonymsManagementAPIService synonymsManagementAPIService;
    private int maxSynonymSets;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class, MapperExtrasPlugin.class);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        maxSynonymSets = randomIntBetween(100, 1000);
        synonymsManagementAPIService = new SynonymsManagementAPIService(client(), maxSynonymSets);
    }

    public void testCreateManySynonyms() throws Exception {
        CountDownLatch putLatch = new CountDownLatch(1);
        String synonymSetId = randomIdentifier();
        boolean refresh = randomBoolean();
        int rulesNumber = randomIntBetween(maxSynonymSets / 2, maxSynonymSets);
        synonymsManagementAPIService.putSynonymsSet(
            synonymSetId,
            randomSynonymsSet(rulesNumber, rulesNumber),
            refresh,
            new ActionListener<>() {
                @Override
                public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                    assertEquals(
                        SynonymsManagementAPIService.UpdateSynonymsResultStatus.CREATED,
                        synonymsReloadResult.synonymsOperationResult()
                    );
                    assertEquals(refresh, synonymsReloadResult.reloadAnalyzersResponse() != null);
                    putLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            }
        );

        putLatch.await(5, TimeUnit.SECONDS);

        CountDownLatch getLatch = new CountDownLatch(1);
        // Also retrieve them
        assertBusy(() -> {
            synonymsManagementAPIService.getSynonymSetRules(synonymSetId, 0, maxSynonymSets, new ActionListener<>() {
                @Override
                public void onResponse(PagedResult<SynonymRule> synonymRulePagedResult) {
                    assertEquals(rulesNumber, synonymRulePagedResult.totalResults());
                    assertEquals(rulesNumber, synonymRulePagedResult.pageResults().length);
                    getLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            });
        }, 5, TimeUnit.SECONDS);

        getLatch.await(10, TimeUnit.SECONDS);
    }

    public void testCreateTooManySynonymsAtOnce() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        synonymsManagementAPIService.putSynonymsSet(
            randomIdentifier(),
            randomSynonymsSet(maxSynonymSets + 1, maxSynonymSets * 2),
            randomBoolean(),
            new ActionListener<>() {
                @Override
                public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                    fail("Shouldn't create synonyms that are too large");
                }

                @Override
                public void onFailure(Exception e) {
                    if (e instanceof IllegalArgumentException) {
                        latch.countDown();
                    } else {
                        fail(e);
                    }
                }
            }
        );

        latch.await(5, TimeUnit.SECONDS);
    }

    public void testCreateTooManySynonymsUsingRuleUpdates() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        int rulesToUpdate = randomIntBetween(1, 10);
        int synonymsToCreate = maxSynonymSets - rulesToUpdate;
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

    public void testUpdateRuleWithMaxSynonyms() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        String synonymSetId = randomIdentifier();
        SynonymRule[] synonymsSet = randomSynonymsSet(maxSynonymSets, maxSynonymSets);
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, synonymsSet, true, new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                // Updating a rule fails
                synonymsManagementAPIService.putSynonymRule(
                    synonymSetId,
                    synonymsSet[randomIntBetween(0, maxSynonymSets - 1)],
                    randomBoolean(),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                            latch.countDown();
                        }

                        @Override
                        public void onFailure(Exception e) {
                            fail("Should update a rule that already exists at max capcity");
                        }
                    }
                );
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });

        latch.await(5, TimeUnit.SECONDS);
    }

    public void testCreateRuleWithMaxSynonyms() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(1);
        String synonymSetId = randomIdentifier();
        String ruleId = randomIdentifier();
        SynonymRule[] synonymsSet = randomSynonymsSet(maxSynonymSets, maxSynonymSets);
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, synonymsSet, true, new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                // Updating a rule fails
                synonymsManagementAPIService.putSynonymRule(
                    synonymSetId,
                    randomSynonymRule(ruleId),
                    randomBoolean(),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                            fail("Should not create a new rule that does not exist when at max capacity");
                        }

                        @Override
                        public void onFailure(Exception e) {
                            latch.countDown();
                        }
                    }
                );
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });

        latch.await(5, TimeUnit.SECONDS);
    }

    public void testTooManySynonymsOnIndexTriggersWarning() throws InterruptedException {
        CountDownLatch insertLatch = new CountDownLatch(1);
        String synonymSetId = randomIdentifier();
        synonymsManagementAPIService.bulkUpdateSynonymsSet(
            synonymSetId,
            randomSynonymsSet(atLeast(maxSynonymSets + 1)),
            new ActionListener<>() {
                @Override
                public void onResponse(BulkResponse bulkItemResponses) {
                    insertLatch.countDown();
                }

                @Override
                public void onFailure(Exception e) {
                    fail(e);
                }
            }
        );

        insertLatch.await(5, TimeUnit.SECONDS);
        Logger logger = mock(Logger.class);
        SynonymsManagementAPIService.logger = logger;

        CountDownLatch readLatch = new CountDownLatch(1);
        synonymsManagementAPIService.getSynonymSetRules(synonymSetId, new ActionListener<>() {
            @Override
            public void onResponse(PagedResult<SynonymRule> synonymRulePagedResult) {
                readLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should not have been able to retrieve synonyms");
            }
        });

        readLatch.await(5, TimeUnit.SECONDS);
        verify(logger).warn(anyString(), eq(synonymSetId));
    }

    public void testCreateSynonymsWithYellowSynonymsIndex() throws Exception {

        // Override health method check to simulate a timeout in checking the synonyms index
        synonymsManagementAPIService = new SynonymsManagementAPIService(client()) {
            @Override
            void checkSynonymsIndexHealth(ActionListener<ClusterHealthResponse> listener) {
                ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).build();
                ClusterHealthResponse response = new ClusterHealthResponse(
                    randomIdentifier(),
                    new String[] { SynonymsManagementAPIService.SYNONYMS_INDEX_CONCRETE_NAME },
                    clusterState
                );
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
