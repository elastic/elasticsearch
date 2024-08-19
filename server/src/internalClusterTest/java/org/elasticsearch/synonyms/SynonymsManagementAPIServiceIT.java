/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.synonyms;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.index.mapper.extras.MapperExtrasPlugin;
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
        int rulesNumber = randomIntBetween(maxSynonymSets / 2, maxSynonymSets);
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, randomSynonymsSet(rulesNumber, rulesNumber), new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                assertEquals(
                    SynonymsManagementAPIService.UpdateSynonymsResultStatus.CREATED,
                    synonymsReloadResult.synonymsOperationResult()
                );
                putLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });

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
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, randomSynonymsSet(synonymsToCreate), new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                // Create as many rules as should fail
                SynonymRule[] rules = randomSynonymsSet(atLeast(rulesToUpdate + 1));
                CountDownLatch updatedRulesLatch = new CountDownLatch(rulesToUpdate);
                for (int i = 0; i < rulesToUpdate; i++) {
                    synonymsManagementAPIService.putSynonymRule(synonymSetId, rules[i], new ActionListener<>() {
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
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, synonymsSet, new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                // Updating a rule fails
                synonymsManagementAPIService.putSynonymRule(
                    synonymSetId,
                    synonymsSet[randomIntBetween(0, maxSynonymSets - 1)],
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
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, synonymsSet, new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                // Updating a rule fails
                synonymsManagementAPIService.putSynonymRule(synonymSetId, randomSynonymRule(ruleId), new ActionListener<>() {
                    @Override
                    public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                        fail("Should not create a new rule that does not exist when at max capacity");
                    }

                    @Override
                    public void onFailure(Exception e) {
                        latch.countDown();
                    }
                });
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
}
