/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ilm;

import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.rollover.RolloverConditions;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.IlmESRestTestCase;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.Phase;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.ReadOnlyAction;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.junit.Before;

import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.TimeSeriesRestDriver.createFullPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createIndexWithSettings;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createNewSingletonPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getStepKeyForIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.index;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.indexDocument;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.waitAndGetShrinkIndexName;
import static org.elasticsearch.xpack.core.ilm.ShrinkIndexNameSupplier.SHRUNKEN_INDEX_PREFIX;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.equalTo;

public class TimeseriesMoveToStepIT extends IlmESRestTestCase {
    private static final Logger logger = LogManager.getLogger(TimeseriesMoveToStepIT.class);

    private String policy;
    private String index;
    private String alias;

    @Before
    public void refreshIndex() {
        index = "index-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        policy = "policy-" + randomAlphaOfLength(5);
        alias = "alias-" + randomAlphaOfLength(5);
        logger.info("--> running [{}] with index [{}], alias [{}] and policy [{}]", getTestName(), index, alias, policy);
    }

    public void testMoveToAllocateStep() throws Exception {
        String originalIndex = index + "-000001";
        // create policy
        createFullPolicy(client(), policy, TimeValue.timeValueHours(10));
        createIndexWithSettings(
            client(),
            originalIndex,
            alias,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.include._name", "test-cluster-0")
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, "alias")
        );

        // move to a step
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + originalIndex);
        assertBusy(() -> assertTrue(getStepKeyForIndex(client(), originalIndex).equals(new StepKey("new", "complete", "complete"))));
        moveToStepRequest.setJsonEntity("""
            {
              "current_step": {
                "phase": "new",
                "action": "complete",
                "name": "complete"
              },
              "next_step": {
                "phase": "cold",
                "action": "allocate",
                "name": "allocate"
              }
            }""");
        client().performRequest(moveToStepRequest);
        assertBusy(() -> assertFalse(indexExists(originalIndex)));
    }

    public void testMoveToRolloverStep() throws Exception {
        String originalIndex = index + "-000001";
        String shrunkenOriginalIndex = SHRUNKEN_INDEX_PREFIX + originalIndex;
        String secondIndex = index + "-000002";

        createFullPolicy(client(), policy, TimeValue.timeValueHours(10));
        createIndexWithSettings(
            client(),
            originalIndex,
            alias,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put("index.routing.allocation.include._name", "test-cluster-0")
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
        );

        // move to a step
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + originalIndex);
        // index document to trigger rollover
        index(client(), originalIndex, "_id", "foo", "bar");
        logger.info(getStepKeyForIndex(client(), originalIndex));
        moveToStepRequest.setJsonEntity("""
            {
              "current_step": {
                "phase": "new",
                "action": "complete",
                "name": "complete"
              },
              "next_step": {
                "phase": "hot",
                "action": "rollover",
                "name": "attempt-rollover"
              }
            }""");
        client().performRequest(moveToStepRequest);

        /*
         * These asserts are in the order that they should be satisfied in, in
         * order to maximize the time for all operations to complete.
         * An "out of order" assert here may result in this test occasionally
         * timing out and failing inappropriately.
         */
        // asserts that rollover was called
        assertBusy(() -> assertTrue(indexExists(secondIndex)));
        // asserts that shrink deleted the original index
        assertBusy(() -> assertFalse(indexExists(originalIndex)), 30, TimeUnit.SECONDS);
        // asserts that the delete phase completed for the managed shrunken index
        assertBusy(() -> assertFalse(indexExists(shrunkenOriginalIndex)));
    }

    public void testMoveToInjectedStep() throws Exception {
        createNewSingletonPolicy(client(), policy, "warm", new ShrinkAction(1, null, false), TimeValue.timeValueHours(12));

        createIndexWithSettings(
            client(),
            index,
            alias,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
        );

        assertBusy(() -> assertThat(getStepKeyForIndex(client(), index), equalTo(new StepKey("new", "complete", "complete"))));

        // Move to a step from the injected unfollow action
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + index);
        moveToStepRequest.setJsonEntity("""
            {
              "current_step": {
                "phase": "new",
                "action": "complete",
                "name": "complete"
              },
              "next_step": {
                "phase": "warm",
                "action": "unfollow",
                "name": "wait-for-indexing-complete"
              }
            }""");
        // If we get an OK on this request we have successfully moved to the injected step
        assertOK(client().performRequest(moveToStepRequest));

        // Make sure we actually move on to and execute the shrink action
        String shrunkenIndex = waitAndGetShrinkIndexName(client(), index);
        assertBusy(() -> {
            assertTrue(indexExists(shrunkenIndex));
            assertTrue(aliasExists(shrunkenIndex, index));
            assertThat(getStepKeyForIndex(client(), shrunkenIndex), equalTo(PhaseCompleteStep.finalStep("warm").getKey()));
        }, 30, TimeUnit.SECONDS);
    }

    public void testMoveToStepRereadsPolicy() throws Exception {
        createNewSingletonPolicy(
            client(),
            policy,
            "hot",
            new RolloverAction(null, null, TimeValue.timeValueHours(1), null, null, null, null, null, null, null),
            TimeValue.ZERO
        );

        createIndexWithSettings(
            client(),
            "test-1",
            alias,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias),
            true
        );

        assertBusy(
            () -> assertThat(getStepKeyForIndex(client(), "test-1"), equalTo(new StepKey("hot", "rollover", "check-rollover-ready"))),
            30,
            TimeUnit.SECONDS
        );

        createNewSingletonPolicy(
            client(),
            policy,
            "hot",
            new RolloverAction(null, null, null, 1L, null, null, null, null, null, null),
            TimeValue.ZERO
        );

        // Move to the same step, which should re-read the policy
        Request moveToStepRequest = new Request("POST", "_ilm/move/test-1");
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
                "name": "check-rollover-ready"
              }
            }""");
        // busy asserting here as ILM moves the index from the `check-rollover-ready` step into the `error` step and back into the
        // `check-rollover-ready` when retrying. the `_ilm/move` api might fail when the as the `current_step` of the index might be
        // the `error` step at execution time.
        assertBusy(() -> client().performRequest(moveToStepRequest), 30, TimeUnit.SECONDS);

        indexDocument(client(), "test-1", true);

        // Make sure we actually rolled over
        assertBusy(() -> { indexExists("test-000002"); });
    }

    /**
     * Test that an async action does not execute when the Move To Step API is used while ILM is stopped.
     * Unfortunately, this test doesn't prove that the async action never executes, as it's hard to prove that an asynchronous process
     * never happens - waiting for a certain period would only increase our confidence but not actually prove it, and it would increase the
     * runtime of the test significantly. We also assert that the remainder of the policy executes after ILM is started again to ensure that
     * the index is not stuck in the async action step.
     */
    public void testAsyncActionDoesNotExecuteAfterILMStop() throws Exception {
        String originalIndex = index + "-000001";
        // Create a simply policy with the most important aspect being the readonly action, which contains the ReadOnlyStep AsyncActionStep.
        var actions = Map.of(
            "rollover",
            new RolloverAction(RolloverConditions.newBuilder().addMaxIndexAgeCondition(TimeValue.timeValueHours(1)).build()),
            "readonly",
            new ReadOnlyAction()
        );
        Phase phase = new Phase("hot", TimeValue.ZERO, actions);
        createPolicy(client(), policy, phase, null, null, null, null);

        createIndexWithSettings(
            client(),
            originalIndex,
            alias,
            Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, policy).put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias)
        );

        // Wait for ILM to do everything it can for this index
        assertBusy(() -> assertEquals(new StepKey("hot", "rollover", "check-rollover-ready"), getStepKeyForIndex(client(), originalIndex)));

        // Stop ILM
        client().performRequest(new Request("POST", "/_ilm/stop"));

        // Move ILM to the readonly step, which is an async action step.
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + originalIndex);
        moveToStepRequest.setJsonEntity("""
            {
              "current_step": {
                "phase": "hot",
                "action": "rollover",
                "name": "check-rollover-ready"
              },
              "next_step": {
                "phase": "hot",
                "action": "readonly",
                "name": "readonly"
              }
            }""");
        client().performRequest(moveToStepRequest);

        // Since ILM is stopped, the async action should not execute and the index should remain in the readonly step.
        // This is the tricky part of the test, as we can't really verify that the async action will never happen.
        assertEquals(new StepKey("hot", "readonly", "readonly"), getStepKeyForIndex(client(), originalIndex));

        // Restart ILM
        client().performRequest(new Request("POST", "/_ilm/start"));

        // Make sure we actually complete the remainder of the policy after ILM is started again.
        assertBusy(() -> assertEquals(new StepKey("hot", "complete", "complete"), getStepKeyForIndex(client(), originalIndex)));
    }

    public void testMoveToStepWithInvalidNextStep() throws Exception {
        createNewSingletonPolicy(client(), policy, "delete", DeleteAction.WITH_SNAPSHOT_DELETE, TimeValue.timeValueDays(100));
        createIndexWithSettings(
            client(),
            index,
            alias,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
        );

        // move to a step
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + index);
        moveToStepRequest.setJsonEntity("""
            {
              "current_step": {
                "phase": "new",
                "action": "complete",
                "name": "complete"
              },
              "next_step": {
                "phase": "hot",
                "action": "rollover",
                "name": "attempt-rollover"
              }
            }""");
        assertBusy(() -> {
            ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(moveToStepRequest));

            String responseEntityAsString = EntityUtils.toString(exception.getResponse().getEntity());
            String expectedErrorMessage = "step [{\\\"phase\\\":\\\"hot\\\",\\\"action\\\":\\\"rollover\\\",\\\"name\\\":"
                + "\\\"attempt-rollover\\\"}] for index ["
                + index
                + "] with policy ["
                + policy
                + "] does not exist";

            assertThat(responseEntityAsString, containsStringIgnoringCase(expectedErrorMessage));
        });
    }

    public void testMoveToStepWithoutStepName() throws Exception {
        createNewSingletonPolicy(client(), policy, "warm", new ForceMergeAction(1, null), TimeValue.timeValueHours(1));
        createIndexWithSettings(
            client(),
            index,
            alias,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
        );

        // move to a step
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + index);
        moveToStepRequest.setJsonEntity("""
            {
              "current_step": {
                "phase": "new",
                "action": "complete",
                "name": "complete"
              },
              "next_step": {
                "phase": "warm",
                "action": "forcemerge"
              }
            }""");

        assertOK(client().performRequest(moveToStepRequest));

        // Make sure we actually move on to and execute the forcemerge action
        assertBusy(
            () -> { assertThat(getStepKeyForIndex(client(), index), equalTo(PhaseCompleteStep.finalStep("warm").getKey())); },
            30,
            TimeUnit.SECONDS
        );
    }

    public void testMoveToStepWithoutAction() throws Exception {
        createNewSingletonPolicy(client(), policy, "warm", new ForceMergeAction(1, null), TimeValue.timeValueHours(1));
        createIndexWithSettings(
            client(),
            index,
            alias,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
        );

        // move to a step
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + index);
        moveToStepRequest.setJsonEntity("""
            {
              "current_step": {
                "phase": "new",
                "action": "complete",
                "name": "complete"
              },
              "next_step": {
                "phase": "warm"
              }
            }""");

        assertOK(client().performRequest(moveToStepRequest));

        // Make sure we actually move on to and execute the forcemerge action
        assertBusy(
            () -> { assertThat(getStepKeyForIndex(client(), index), equalTo(PhaseCompleteStep.finalStep("warm").getKey())); },
            30,
            TimeUnit.SECONDS
        );
    }

    public void testInvalidToMoveToStepWithoutActionButWithName() throws Exception {
        createNewSingletonPolicy(client(), policy, "warm", new ForceMergeAction(1, null), TimeValue.timeValueHours(1));
        createIndexWithSettings(
            client(),
            index,
            alias,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
        );

        // move to a step with an invalid request
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + index);
        moveToStepRequest.setJsonEntity("""
            {
              "current_step": {
                "phase": "new",
                "action": "complete",
                "name": "complete"
              },
              "next_step": {
                "phase": "warm",
                "name": "forcemerge"
              }
            }""");

        assertBusy(() -> {
            ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(moveToStepRequest));
            String responseEntityAsString = EntityUtils.toString(exception.getResponse().getEntity());
            String expectedErrorMessage = "phase; phase and action; or phase, action, and step must be provided, "
                + "but a step name was specified without a corresponding action";
            assertThat(responseEntityAsString, containsStringIgnoringCase(expectedErrorMessage));
        });
    }

    public void testResolveToNonexistentStep() throws Exception {
        createNewSingletonPolicy(client(), policy, "warm", new ForceMergeAction(1, null), TimeValue.timeValueHours(1));
        createIndexWithSettings(
            client(),
            index,
            alias,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
        );

        // move to a step with an invalid request
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + index);
        moveToStepRequest.setJsonEntity("""
            {
              "current_step": {
                "phase": "new",
                "action": "complete",
                "name": "complete"
              },
              "next_step": {
                "phase": "warm",
                "action": "shrink"
              }
            }""");

        assertBusy(() -> {
            ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(moveToStepRequest));
            String responseEntityAsString = EntityUtils.toString(exception.getResponse().getEntity());
            String expectedErrorMessage = """
                unable to determine concrete step key from target next step key: \
                {\\"phase\\":\\"warm\\",\\"action\\":\\"shrink\\"}""";
            assertThat(responseEntityAsString, containsStringIgnoringCase(expectedErrorMessage));
        });
    }
}
