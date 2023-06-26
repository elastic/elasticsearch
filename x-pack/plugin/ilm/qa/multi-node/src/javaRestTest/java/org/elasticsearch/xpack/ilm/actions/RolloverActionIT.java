/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm.actions;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.WarningFailureException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.action.admin.indices.RestPutIndexTemplateAction;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.WaitForRolloverReadyStep;
import org.junit.Before;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.TimeSeriesRestDriver.createIndexWithSettings;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createNewSingletonPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.explainIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getOnlyIndexSettings;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getStepKeyForIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.index;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.rolloverMaxOneDocCondition;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.updatePolicy;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;

public class RolloverActionIT extends ESRestTestCase {

    private static final String FAILED_STEP_RETRY_COUNT_FIELD = "failed_step_retry_count";

    private String index;
    private String policy;
    private String alias;

    @Before
    public void refreshIndex() {
        index = "index-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        policy = "policy-" + randomAlphaOfLength(5);
        alias = "alias-" + randomAlphaOfLength(5);
        logger.info("--> running [{}] with index [{}], alias [{}] and policy [{}]", getTestName(), index, alias, policy);
    }

    public void testRolloverAction() throws Exception {
        String originalIndex = index + "-000001";
        String secondIndex = index + "-000002";
        createIndexWithSettings(
            client(),
            originalIndex,
            alias,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
        );

        // create policy
        createNewSingletonPolicy(client(), policy, "hot", new RolloverAction(null, null, null, 1L, null, null, null, null, null, null));
        // update policy on index
        updatePolicy(client(), originalIndex, policy);
        // index document {"foo": "bar"} to trigger rollover
        index(client(), originalIndex, "_id", "foo", "bar");

        assertBusy(() -> {
            assertThat(getStepKeyForIndex(client(), originalIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey()));
            assertTrue(indexExists(secondIndex));
            assertTrue(indexExists(originalIndex));
            assertEquals("true", getOnlyIndexSettings(client(), originalIndex).get(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE));
        }, 30, TimeUnit.SECONDS);
    }

    public void testRolloverActionWithIndexingComplete() throws Exception {
        String originalIndex = index + "-000001";
        String secondIndex = index + "-000002";
        createIndexWithSettings(
            client(),
            originalIndex,
            alias,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
        );

        Request updateSettingsRequest = new Request("PUT", "/" + originalIndex + "/_settings");
        updateSettingsRequest.setJsonEntity(Strings.format("""
            {
              "settings": {
                "%s": true
              }
            }""", LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE));
        client().performRequest(updateSettingsRequest);
        Request updateAliasRequest = new Request("POST", "/_aliases");
        updateAliasRequest.setJsonEntity(Strings.format("""
            {
              "actions": [
                {
                  "add": {
                    "index": "%s",
                    "alias": "%s",
                    "is_write_index": false
                  }
                }
              ]
            }""", originalIndex, alias));
        client().performRequest(updateAliasRequest);

        // create policy
        createNewSingletonPolicy(client(), policy, "hot", new RolloverAction(null, null, null, 1L, null, null, null, null, null, null));
        // update policy on index
        updatePolicy(client(), originalIndex, policy);
        // index document {"foo": "bar"} to trigger rollover
        index(client(), originalIndex, "_id", "foo", "bar");

        assertBusy(() -> {
            assertThat(getStepKeyForIndex(client(), originalIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey()));
            assertTrue(indexExists(originalIndex));
            assertFalse(indexExists(secondIndex)); // careful, *assertFalse* not *assertTrue*
            assertEquals("true", getOnlyIndexSettings(client(), originalIndex).get(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE));
        }, 30, TimeUnit.SECONDS);
    }

    public void testRolloverActionWithMaxPrimaryShardSize() throws Exception {
        String originalIndex = index + "-000001";
        String secondIndex = index + "-000002";
        createIndexWithSettings(
            client(),
            originalIndex,
            alias,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
        );

        index(client(), originalIndex, "_id", "foo", "bar");

        // create policy
        createNewSingletonPolicy(
            client(),
            policy,
            "hot",
            new RolloverAction(null, ByteSizeValue.ofBytes(1), null, null, null, null, null, null, null, null)
        );
        // update policy on index
        updatePolicy(client(), originalIndex, policy);

        assertBusy(() -> {
            assertThat(getStepKeyForIndex(client(), originalIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey()));
            assertTrue(indexExists(secondIndex));
            assertTrue(indexExists(originalIndex));
            assertEquals("true", getOnlyIndexSettings(client(), originalIndex).get(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE));
        }, 30, TimeUnit.SECONDS);
    }

    public void testRolloverActionWithMaxPrimaryDocsSize() throws Exception {
        String originalIndex = index + "-000001";
        String secondIndex = index + "-000002";
        createIndexWithSettings(
            client(),
            originalIndex,
            alias,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
        );

        index(client(), originalIndex, "_id", "foo", "bar");

        // create policy
        createNewSingletonPolicy(client(), policy, "hot", new RolloverAction(null, null, null, null, 1L, null, null, null, null, null));
        // update policy on index
        updatePolicy(client(), originalIndex, policy);

        assertBusy(() -> {
            assertThat(getStepKeyForIndex(client(), originalIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey()));
            assertTrue(indexExists(secondIndex));
            assertTrue(indexExists(originalIndex));
            assertEquals("true", getOnlyIndexSettings(client(), originalIndex).get(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE));
        }, 30, TimeUnit.SECONDS);
    }

    /**
     * There are multiple scenarios where we want to set up an empty index, make sure that it *doesn't* roll over, then change something
     * about the cluster, and verify that now the index does roll over. This is a 'template method' that allows you to provide a runnable
     * which will accomplish that end. Each invocation of this should live in its own top-level `public void test...` method.
     */
    private void templateTestRolloverActionWithEmptyIndex(CheckedRunnable<Exception> allowEmptyIndexToRolloverRunnable) throws Exception {
        String originalIndex = index + "-000001";
        String secondIndex = index + "-000002";

        createIndexWithSettings(
            client(),
            originalIndex,
            alias,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
        );

        // create policy
        createNewSingletonPolicy(
            client(),
            policy,
            "hot",
            new RolloverAction(null, null, TimeValue.timeValueSeconds(1), null, null, null, null, null, null, null)
        );
        // update policy on index
        updatePolicy(client(), originalIndex, policy);

        // maybe set the controlling setting to explicitly true (rather than just true by default)
        if (randomBoolean()) {
            setLifecycleRolloverOnlyIfHasDocumentsSetting(true);
        }

        // because the index is empty, it doesn't roll over
        assertBusy(() -> {
            assertThat(getStepKeyForIndex(client(), originalIndex).name(), is(WaitForRolloverReadyStep.NAME));
            assertFalse(indexExists(secondIndex));
            assertTrue(indexExists(originalIndex));
        }, 30, TimeUnit.SECONDS);

        // run the passed in runnable that will somehow make it so the index rolls over
        allowEmptyIndexToRolloverRunnable.run();

        // now the index rolls over as expected
        assertBusy(() -> {
            assertThat(getStepKeyForIndex(client(), originalIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey()));
            assertTrue(indexExists(secondIndex));
            assertTrue(indexExists(originalIndex));
            assertEquals("true", getOnlyIndexSettings(client(), originalIndex).get(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE));
        }, 30, TimeUnit.SECONDS);

        // reset to null so that the post-test cleanup doesn't fail because it sees a deprecated setting
        setLifecycleRolloverOnlyIfHasDocumentsSetting(null);
    }

    public void testRolloverActionWithEmptyIndexThenADocIsIndexed() throws Exception {
        templateTestRolloverActionWithEmptyIndex(() -> {
            // index document {"foo": "bar"} to trigger rollover
            index(client(), index + "-000001", "_id", "foo", "bar");
        });
    }

    public void testRolloverActionWithEmptyIndexThenThePolicyIsChanged() throws Exception {
        templateTestRolloverActionWithEmptyIndex(() -> {
            // change the policy to permit empty rollovers -- with either min_docs or min_primary_shard_docs set to 0
            createNewSingletonPolicy(
                client(),
                policy,
                "hot",
                randomBoolean()
                    ? new RolloverAction(null, null, TimeValue.timeValueSeconds(1), null, null, null, null, null, 0L, null)
                    : new RolloverAction(null, null, TimeValue.timeValueSeconds(1), null, null, null, null, null, null, 0L)
            );
        });
    }

    public void testRolloverActionWithEmptyIndexThenTheClusterSettingIsChanged() throws Exception {
        templateTestRolloverActionWithEmptyIndex(() -> {
            // change the cluster-wide setting to permit empty rollovers
            setLifecycleRolloverOnlyIfHasDocumentsSetting(false);
        });
    }

    private void setLifecycleRolloverOnlyIfHasDocumentsSetting(@Nullable Boolean value) throws IOException {
        try {
            Settings.Builder settings = Settings.builder();
            if (value != null) {
                settings.put(LifecycleSettings.LIFECYCLE_ROLLOVER_ONLY_IF_HAS_DOCUMENTS, value.booleanValue());
            } else {
                settings.putNull(LifecycleSettings.LIFECYCLE_ROLLOVER_ONLY_IF_HAS_DOCUMENTS);
            }
            updateClusterSettings(settings.build());
            if (value != null) {
                fail("expected WarningFailureException from warnings");
            }
        } catch (WarningFailureException e) {
            // expected, this setting is deprecated, so we can get back a warning
        }
    }

    public void testILMRolloverRetriesOnReadOnlyBlock() throws Exception {
        String firstIndex = index + "-000001";

        createNewSingletonPolicy(
            client(),
            policy,
            "hot",
            new RolloverAction(null, null, TimeValue.timeValueSeconds(1), null, null, null, null, null, 0L, null)
        );

        // create the index as readonly and associate the ILM policy to it
        createIndexWithSettings(
            client(),
            firstIndex,
            alias,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
                .put("index.blocks.read_only", true),
            true
        );

        // wait for ILM to start retrying the step
        assertBusy(
            () -> assertThat((Integer) explainIndex(client(), firstIndex).get(FAILED_STEP_RETRY_COUNT_FIELD), greaterThanOrEqualTo(1))
        );

        // remove the read only block
        Request allowWritesOnIndexSettingUpdate = new Request("PUT", firstIndex + "/_settings");
        allowWritesOnIndexSettingUpdate.setJsonEntity("""
            {  "index": {
                 "blocks.read_only" : "false"\s
              }
            }""");
        client().performRequest(allowWritesOnIndexSettingUpdate);

        // index is not readonly so the ILM should complete successfully
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), firstIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));
    }

    public void testILMRolloverOnManuallyRolledIndex() throws Exception {
        String originalIndex = index + "-000001";
        String secondIndex = index + "-000002";
        String thirdIndex = index + "-000003";

        // Set up a policy with rollover
        createNewSingletonPolicy(client(), policy, "hot", new RolloverAction(null, null, null, 2L, null, null, null, null, null, null));
        Request createIndexTemplate = new Request("PUT", "_template/rolling_indexes");
        createIndexTemplate.setJsonEntity(Strings.format("""
            {
              "index_patterns": ["%s-*"],
              "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 0,
                "index.lifecycle.name": "%s",
                "index.lifecycle.rollover_alias": "%s"
              }
            }""", index, policy, alias));
        createIndexTemplate.setOptions(expectWarnings(RestPutIndexTemplateAction.DEPRECATION_WARNING));
        client().performRequest(createIndexTemplate);

        createIndexWithSettings(
            client(),
            originalIndex,
            alias,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0),
            true
        );

        // Index a document
        index(client(), originalIndex, "1", "foo", "bar");
        Request refreshOriginalIndex = new Request("POST", "/" + originalIndex + "/_refresh");
        client().performRequest(refreshOriginalIndex);

        // Manual rollover
        rolloverMaxOneDocCondition(client(), alias);
        assertBusy(() -> assertTrue(indexExists(secondIndex)));

        // Index another document into the original index so the ILM rollover policy condition is met
        index(client(), originalIndex, "2", "foo", "bar");
        client().performRequest(refreshOriginalIndex);

        // Wait for the rollover policy to execute
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), originalIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));

        // ILM should manage the second index after attempting (and skipping) rolling the original index
        assertBusy(() -> assertTrue((boolean) explainIndex(client(), secondIndex).getOrDefault("managed", true)));

        // index some documents to trigger an ILM rollover
        index(client(), alias, "1", "foo", "bar");
        index(client(), alias, "2", "foo", "bar");
        index(client(), alias, "3", "foo", "bar");
        Request refreshSecondIndex = new Request("POST", "/" + secondIndex + "/_refresh");
        client().performRequest(refreshSecondIndex).getStatusLine();

        // ILM should rollover the second index even though it skipped the first one
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), secondIndex), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));
        assertBusy(() -> assertTrue(indexExists(thirdIndex)));
    }

    public void testRolloverStepRetriesUntilRolledOverIndexIsDeleted() throws Exception {
        String index = this.index + "-000001";
        String rolledIndex = this.index + "-000002";

        createNewSingletonPolicy(
            client(),
            policy,
            "hot",
            new RolloverAction(null, null, TimeValue.timeValueSeconds(1), null, null, null, null, null, null, null)
        );

        // create the rolled index so the rollover of the first index fails
        createIndexWithSettings(
            client(),
            rolledIndex,
            alias,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias),
            false
        );

        createIndexWithSettings(
            client(),
            index,
            alias,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias),
            true
        );

        assertBusy(
            () -> assertThat((Integer) explainIndex(client(), index).get(FAILED_STEP_RETRY_COUNT_FIELD), greaterThanOrEqualTo(1)),
            30,
            TimeUnit.SECONDS
        );

        Request moveToStepRequest = new Request("POST", "_ilm/move/" + index);
        moveToStepRequest.setJsonEntity("""
            {
              "current_step": {
                "phase": "hot",
                "action": "rollover",
                "name": "check-rollover-ready"
              },
              "next_step": {
                "phase": "hot",
                "action": "rollover",
                "name": "attempt-rollover"
              }
            }""");

        // Using {@link #waitUntil} here as ILM moves back and forth between the {@link WaitForRolloverReadyStep} step and
        // {@link org.elasticsearch.xpack.core.ilm.ErrorStep} in order to retry the failing step. As {@link #assertBusy}
        // increases the wait time between calls exponentially, we might miss the window where the policy is on
        // {@link WaitForRolloverReadyStep} and the move to `attempt-rollover` request will not be successful.
        assertTrue(waitUntil(() -> {
            try {
                return client().performRequest(moveToStepRequest).getStatusLine().getStatusCode() == 200;
            } catch (IOException e) {
                return false;
            }
        }, 30, TimeUnit.SECONDS));

        // Similar to above, using {@link #waitUntil} as we want to make sure the `attempt-rollover` step started failing and is being
        // retried (which means ILM moves back and forth between the `attempt-rollover` step and the `error` step)
        assertTrue("ILM did not start retrying the attempt-rollover step", waitUntil(() -> {
            try {
                Map<String, Object> explainIndexResponse = explainIndex(client(), index);
                String failedStep = (String) explainIndexResponse.get("failed_step");
                Integer retryCount = (Integer) explainIndexResponse.get(FAILED_STEP_RETRY_COUNT_FIELD);
                return failedStep != null && failedStep.equals("attempt-rollover") && retryCount != null && retryCount >= 1;
            } catch (IOException e) {
                return false;
            }
        }, 30, TimeUnit.SECONDS));

        deleteIndex(rolledIndex);

        // the rollover step should eventually succeed
        assertBusy(() -> assertThat(indexExists(rolledIndex), is(true)));
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), index), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));
    }

    public void testUpdateRolloverLifecycleDateStepRetriesWhenRolloverInfoIsMissing() throws Exception {
        String index = this.index + "-000001";

        createNewSingletonPolicy(client(), policy, "hot", new RolloverAction(null, null, null, 1L, null, null, null, null, null, null));

        createIndexWithSettings(
            client(),
            index,
            alias,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias),
            true
        );

        assertBusy(() -> assertThat(getStepKeyForIndex(client(), index).name(), is(WaitForRolloverReadyStep.NAME)));

        // moving ILM to the "update-rollover-lifecycle-date" without having gone through the actual rollover step
        // the "update-rollover-lifecycle-date" step will fail as the index has no rollover information
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + index);
        moveToStepRequest.setJsonEntity("""
            {
              "current_step": {
                "phase": "hot",
                "action": "rollover",
                "name": "check-rollover-ready"
              },
              "next_step": {
                "phase": "hot",
                "action": "rollover",
                "name": "update-rollover-lifecycle-date"
              }
            }""");
        client().performRequest(moveToStepRequest);

        assertBusy(
            () -> assertThat((Integer) explainIndex(client(), index).get(FAILED_STEP_RETRY_COUNT_FIELD), greaterThanOrEqualTo(1)),
            30,
            TimeUnit.SECONDS
        );

        index(client(), index, "1", "foo", "bar");
        Request refreshIndex = new Request("POST", "/" + index + "/_refresh");
        client().performRequest(refreshIndex);

        // manual rollover the index so the "update-rollover-lifecycle-date" ILM step can continue and finish successfully as the index
        // will have rollover information now
        rolloverMaxOneDocCondition(client(), alias);
        assertBusy(() -> assertThat(getStepKeyForIndex(client(), index), equalTo(PhaseCompleteStep.finalStep("hot").getKey())));
    }

}
