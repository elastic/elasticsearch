/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.synonyms;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
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
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class SynonymsManagementAPIServiceIT extends ESIntegTestCase {

    private SynonymsManagementAPIService synonymsManagementAPIService;
    private int maxTokenLimit;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class, MapperExtrasPlugin.class);
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        maxTokenLimit = randomIntBetween(30, 100);
        synonymsManagementAPIService = new SynonymsManagementAPIService(
            client(),
            10_000,
            maxTokenLimit,
            SynonymsManagementAPIService.TokenLimitMode.STRICT,
            100_000,
            SynonymsManagementAPIService.SCROLL_BATCH_SIZE
        );
    }

    /**
     * Creates synonym rules with a known, fixed token count (3 tokens per rule: "a, b, c").
     */
    private static SynonymRule[] fixedTokenRules(int ruleCount) {
        SynonymRule[] rules = new SynonymRule[ruleCount];
        for (int i = 0; i < ruleCount; i++) {
            rules[i] = new SynonymRule("rule_" + i, "a, b, c");
        }
        return rules;
    }

    public void testCreateManySynonyms() throws Exception {
        CountDownLatch putLatch = new CountDownLatch(1);
        String synonymSetId = randomIdentifier();
        boolean refresh = randomBoolean();
        // 3 tokens per rule; stay within the token limit
        int rulesNumber = randomIntBetween(1, maxTokenLimit / 3);
        SynonymRule[] rules = fixedTokenRules(rulesNumber);
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, rules, refresh, new ActionListener<>() {
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
        });

        putLatch.await(5, TimeUnit.SECONDS);

        CountDownLatch getLatch = new CountDownLatch(1);
        assertBusy(() -> {
            synonymsManagementAPIService.getSynonymSetRules(synonymSetId, 0, 10_000, new ActionListener<>() {
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

    public void testGetAllSynonymSetRulesExceedingScrollLimitFails() throws Exception {
        int scrollLimit = randomIntBetween(2, 10);
        synonymsManagementAPIService = new SynonymsManagementAPIService(client(), 10_000, scrollLimit);

        String synonymSetId = randomIdentifier();
        CountDownLatch putLatch = new CountDownLatch(1);
        // Use bulkUpdateSynonymsSet to bypass the write cap and insert more rules than the scroll limit
        synonymsManagementAPIService.bulkUpdateSynonymsSet(
            synonymSetId,
            randomSynonymsSet(scrollLimit + 1, scrollLimit + 1),
            new ActionListener<>() {
                @Override
                public void onResponse(org.elasticsearch.action.bulk.BulkResponse bulkResponse) {
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
        assertBusy(() -> {
            synonymsManagementAPIService.getSynonymSetRules(synonymSetId, new ActionListener<>() {
                @Override
                public void onResponse(PagedResult<SynonymRule> synonymRulePagedResult) {
                    fail("Expected failure due to scroll limit exceeded");
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(e, instanceOf(IllegalArgumentException.class));
                    assertThat(e.getMessage(), containsString(synonymSetId));
                    assertThat(e.getMessage(), containsString(String.valueOf(scrollLimit)));
                    getLatch.countDown();
                }
            });
        }, 5, TimeUnit.SECONDS);

        getLatch.await(10, TimeUnit.SECONDS);
    }

    public void testGetAllSynonymSetRulesViaScroll() throws Exception {
        int scrollBatchSize = randomIntBetween(2, 5);
        int rulesNumber = scrollBatchSize * randomIntBetween(3, 6);
        int tokenLimit = rulesNumber * 3 + 1;
        SynonymsManagementAPIService scrollService = new SynonymsManagementAPIService(
            client(),
            10_000,
            tokenLimit,
            SynonymsManagementAPIService.TokenLimitMode.STRICT,
            rulesNumber + 1,
            scrollBatchSize
        );

        String synonymSetId = randomIdentifier();
        CountDownLatch putLatch = new CountDownLatch(1);
        scrollService.putSynonymsSet(synonymSetId, fixedTokenRules(rulesNumber), false, new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                putLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });
        putLatch.await(5, TimeUnit.SECONDS);

        CountDownLatch getLatch = new CountDownLatch(1);
        assertBusy(() -> {
            scrollService.getSynonymSetRules(synonymSetId, new ActionListener<>() {
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

    public void testCreateTooManyTokensAtOnce() throws InterruptedException {
        // Each rule has 3 tokens; create enough rules to exceed the token limit
        int rulesNeeded = (maxTokenLimit / 3) + 1;
        CountDownLatch latch = new CountDownLatch(1);
        synonymsManagementAPIService.putSynonymsSet(
            randomIdentifier(),
            fixedTokenRules(rulesNeeded),
            randomBoolean(),
            new ActionListener<>() {
                @Override
                public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                    fail("Shouldn't create synonyms that exceed the token limit");
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(e, instanceOf(IllegalArgumentException.class));
                    assertThat(e.getMessage(), containsString("cannot exceed"));
                    latch.countDown();
                }
            }
        );

        latch.await(5, TimeUnit.SECONDS);
    }

    public void testExceedTokenLimitUsingRuleUpdates() throws InterruptedException {
        // Fill a synonym set to just under the token limit, then try adding one more rule
        int rulesAtCapacity = maxTokenLimit / 3;
        String synonymSetId = randomIdentifier();

        CountDownLatch putLatch = new CountDownLatch(1);
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, fixedTokenRules(rulesAtCapacity), true, new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                putLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });
        assertTrue(putLatch.await(5, TimeUnit.SECONDS));

        // Adding one more rule should exceed the token limit
        CountDownLatch failLatch = new CountDownLatch(1);
        synonymsManagementAPIService.putSynonymRule(
            synonymSetId,
            new SynonymRule("extra_rule", "x, y, z"),
            randomBoolean(),
            new ActionListener<>() {
                @Override
                public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                    fail("Adding a rule should have exceeded the token limit");
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(e, instanceOf(IllegalArgumentException.class));
                    assertThat(e.getMessage(), containsString("exceed"));
                    failLatch.countDown();
                }
            }
        );
        assertTrue(failLatch.await(5, TimeUnit.SECONDS));
    }

    public void testUpdateRuleAtTokenCapacity() throws InterruptedException {
        // Fill to exactly the token limit
        int rulesAtCapacity = maxTokenLimit / 3;
        String synonymSetId = randomIdentifier();
        SynonymRule[] rules = fixedTokenRules(rulesAtCapacity);

        CountDownLatch putLatch = new CountDownLatch(1);
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, rules, true, new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                putLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });
        assertTrue(putLatch.await(5, TimeUnit.SECONDS));

        // Updating an existing rule at capacity should succeed
        CountDownLatch updateLatch = new CountDownLatch(1);
        SynonymRule existingRule = rules[randomIntBetween(0, rulesAtCapacity - 1)];
        synonymsManagementAPIService.putSynonymRule(synonymSetId, existingRule, randomBoolean(), new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                updateLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Should update a rule that already exists at token capacity");
            }
        });
        assertTrue(updateLatch.await(5, TimeUnit.SECONDS));
    }

    public void testCreateNewRuleAtTokenCapacityFails() throws InterruptedException {
        // Fill to exactly the token limit
        int rulesAtCapacity = maxTokenLimit / 3;
        String synonymSetId = randomIdentifier();

        CountDownLatch putLatch = new CountDownLatch(1);
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, fixedTokenRules(rulesAtCapacity), true, new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                putLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });
        assertTrue(putLatch.await(5, TimeUnit.SECONDS));

        // Adding a new rule (not updating existing) should fail
        CountDownLatch failLatch = new CountDownLatch(1);
        synonymsManagementAPIService.putSynonymRule(
            synonymSetId,
            new SynonymRule(randomIdentifier(), "x, y, z"),
            randomBoolean(),
            new ActionListener<>() {
                @Override
                public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult synonymsReloadResult) {
                    fail("Should not create a new rule when at token capacity");
                }

                @Override
                public void onFailure(Exception e) {
                    assertThat(e, instanceOf(IllegalArgumentException.class));
                    assertThat(e.getMessage(), containsString("exceed"));
                    failLatch.countDown();
                }
            }
        );
        assertTrue(failLatch.await(5, TimeUnit.SECONDS));
    }

    public void testCreateSynonymsWithYellowSynonymsIndex() throws Exception {

        // Override health method check to simulate a timeout in checking the synonyms index
        synonymsManagementAPIService = new SynonymsManagementAPIService(client()) {
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

    public void testDynamicTokenLimitClusterSetting() throws Exception {
        int lowLimit = 10;
        updateClusterSettings(Settings.builder().put(SynonymsManagementAPIService.MAX_SYNONYMS_SET_TOKENS_SETTING.getKey(), lowLimit));

        synonymsManagementAPIService = new SynonymsManagementAPIService(client(), clusterService().getClusterSettings());

        // 4 rules x 3 tokens each = 12 tokens, exceeding the limit of 10
        SynonymRule[] rules = new SynonymRule[4];
        for (int i = 0; i < rules.length; i++) {
            rules[i] = new SynonymRule("rule_" + i, "a, b, c");
        }

        String synonymSetId = randomIdentifier();

        CountDownLatch failLatch = new CountDownLatch(1);
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, rules, false, new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult result) {
                fail("Should have been rejected due to token limit");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, instanceOf(IllegalArgumentException.class));
                assertThat(e.getMessage(), containsString("cannot exceed"));
                failLatch.countDown();
            }
        });
        assertTrue(failLatch.await(5, TimeUnit.SECONDS));

        // Raise the limit dynamically
        updateClusterSettings(Settings.builder().put(SynonymsManagementAPIService.MAX_SYNONYMS_SET_TOKENS_SETTING.getKey(), 100));

        // Same put should now succeed
        CountDownLatch successLatch = new CountDownLatch(1);
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, rules, false, new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult result) {
                successLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail(e);
            }
        });
        assertTrue(successLatch.await(5, TimeUnit.SECONDS));

        // Reset to default
        updateClusterSettings(Settings.builder().putNull(SynonymsManagementAPIService.MAX_SYNONYMS_SET_TOKENS_SETTING.getKey()));
    }

    public void testLenientModeSilentlyIgnoresExcessTokens() throws Exception {
        int lowLimit = 10;
        updateClusterSettings(
            Settings.builder()
                .put(SynonymsManagementAPIService.MAX_SYNONYMS_SET_TOKENS_SETTING.getKey(), lowLimit)
                .put(
                    SynonymsManagementAPIService.TOKEN_LIMIT_MODE_SETTING.getKey(),
                    SynonymsManagementAPIService.TokenLimitMode.LENIENT.toString()
                )
        );

        synonymsManagementAPIService = new SynonymsManagementAPIService(client(), clusterService().getClusterSettings());

        // 4 rules x 3 tokens each = 12 tokens, exceeding the limit of 10
        SynonymRule[] rules = new SynonymRule[4];
        for (int i = 0; i < rules.length; i++) {
            rules[i] = new SynonymRule("rule_" + i, "a, b, c");
        }

        String synonymSetId = randomIdentifier();

        // In lenient mode, the put should succeed silently (no error, no data written)
        CountDownLatch putLatch = new CountDownLatch(1);
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, rules, false, new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult result) {
                putLatch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                fail("Lenient mode should not fail: " + e.getMessage());
            }
        });
        assertTrue(putLatch.await(5, TimeUnit.SECONDS));

        // Switch to strict mode dynamically — same request should now fail
        updateClusterSettings(
            Settings.builder()
                .put(
                    SynonymsManagementAPIService.TOKEN_LIMIT_MODE_SETTING.getKey(),
                    SynonymsManagementAPIService.TokenLimitMode.STRICT.toString()
                )
        );

        CountDownLatch failLatch = new CountDownLatch(1);
        synonymsManagementAPIService.putSynonymsSet(synonymSetId, rules, false, new ActionListener<>() {
            @Override
            public void onResponse(SynonymsManagementAPIService.SynonymsReloadResult result) {
                fail("Strict mode should reject excess tokens");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, instanceOf(IllegalArgumentException.class));
                assertThat(e.getMessage(), containsString("cannot exceed"));
                failLatch.countDown();
            }
        });
        assertTrue(failLatch.await(5, TimeUnit.SECONDS));

        // Reset to defaults
        updateClusterSettings(
            Settings.builder()
                .putNull(SynonymsManagementAPIService.MAX_SYNONYMS_SET_TOKENS_SETTING.getKey())
                .putNull(SynonymsManagementAPIService.TOKEN_LIMIT_MODE_SETTING.getKey())
        );
    }
}
