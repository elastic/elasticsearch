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
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ilm.DeleteAction;
import org.elasticsearch.xpack.core.ilm.ForceMergeAction;
import org.elasticsearch.xpack.core.ilm.LifecycleSettings;
import org.elasticsearch.xpack.core.ilm.PhaseCompleteStep;
import org.elasticsearch.xpack.core.ilm.RolloverAction;
import org.elasticsearch.xpack.core.ilm.ShrinkAction;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.junit.Before;

import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.TimeSeriesRestDriver.createFullPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createIndexWithSettings;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.createNewSingletonPolicy;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.getStepKeyForIndex;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.index;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.indexDocument;
import static org.elasticsearch.xpack.TimeSeriesRestDriver.waitAndGetShrinkIndexName;
import static org.elasticsearch.xpack.core.ilm.ShrinkIndexNameSupplier.SHRUNKEN_INDEX_PREFIX;
import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.hamcrest.Matchers.equalTo;

public class TimeseriesMoveToStepIT extends ESRestTestCase {
    private static final Logger logger = LogManager.getLogger(TimeseriesMoveToStepIT.class);

    private String policy;
    private String index;
    private String alias;

    @Before
    public void refreshIndex() {
        index = "index-" + randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        policy = "policy-" + randomAlphaOfLength(5);
        alias = "alias-" + randomAlphaOfLength(5);
    }

    public void testMoveToAllocateStep() throws Exception {
        String originalIndex = index + "-000001";
        // create policy
        createFullPolicy(client(), policy, TimeValue.timeValueHours(10));
        createIndexWithSettings(client(), originalIndex, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.routing.allocation.include._name", "javaRestTest-0")
            .put(LifecycleSettings.LIFECYCLE_NAME, policy)
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, "alias"));

        // move to a step
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + originalIndex);
        assertBusy(() -> assertTrue(getStepKeyForIndex(client(), originalIndex).equals(new StepKey("new", "complete", "complete"))));
        moveToStepRequest.setJsonEntity("{\n" +
            "  \"current_step\": {\n" +
            "    \"phase\": \"new\",\n" +
            "    \"action\": \"complete\",\n" +
            "    \"name\": \"complete\"\n" +
            "  },\n" +
            "  \"next_step\": {\n" +
            "    \"phase\": \"cold\",\n" +
            "    \"action\": \"allocate\",\n" +
            "    \"name\": \"allocate\"\n" +
            "  }\n" +
            "}");
        client().performRequest(moveToStepRequest);
        assertBusy(() -> assertFalse(indexExists(originalIndex)));
    }

    public void testMoveToRolloverStep() throws Exception {
        String originalIndex = index + "-000001";
        String shrunkenOriginalIndex = SHRUNKEN_INDEX_PREFIX + originalIndex;
        String secondIndex = index + "-000002";

        createFullPolicy(client(), policy, TimeValue.timeValueHours(10));
        createIndexWithSettings(client(), originalIndex, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 4)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.routing.allocation.include._name", "javaRestTest-0")
            .put(LifecycleSettings.LIFECYCLE_NAME, policy)
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias));

        // move to a step
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + originalIndex);
        // index document to trigger rollover
        index(client(), originalIndex, "_id", "foo", "bar");
        logger.info(getStepKeyForIndex(client(), originalIndex));
        moveToStepRequest.setJsonEntity("{\n" +
            "  \"current_step\": {\n" +
            "    \"phase\": \"new\",\n" +
            "    \"action\": \"complete\",\n" +
            "    \"name\": \"complete\"\n" +
            "  },\n" +
            "  \"next_step\": {\n" +
            "    \"phase\": \"hot\",\n" +
            "    \"action\": \"rollover\",\n" +
            "    \"name\": \"attempt-rollover\"\n" +
            "  }\n" +
            "}");
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
        createNewSingletonPolicy(client(), policy, "warm", new ShrinkAction(1, null), TimeValue.timeValueHours(12));

        createIndexWithSettings(client(), index, alias, Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 3)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(LifecycleSettings.LIFECYCLE_NAME, policy)
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias));

        assertBusy(() -> assertThat(getStepKeyForIndex(client(), index), equalTo(new StepKey("new", "complete", "complete"))));

        // Move to a step from the injected unfollow action
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + index);
        moveToStepRequest.setJsonEntity("{\n" +
            "  \"current_step\": { \n" +
            "    \"phase\": \"new\",\n" +
            "    \"action\": \"complete\",\n" +
            "    \"name\": \"complete\"\n" +
            "  },\n" +
            "  \"next_step\": { \n" +
            "    \"phase\": \"warm\",\n" +
            "    \"action\": \"unfollow\",\n" +
            "    \"name\": \"wait-for-indexing-complete\"\n" +
            "  }\n" +
            "}");
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
        createNewSingletonPolicy(client(), policy, "hot", new RolloverAction(null, null, TimeValue.timeValueHours(1), null), TimeValue.ZERO);

        createIndexWithSettings(client(), "test-1", alias, Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(LifecycleSettings.LIFECYCLE_NAME, policy)
                .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, alias),
            true);

        assertBusy(() -> assertThat(getStepKeyForIndex(client(), "test-1"),
            equalTo(new StepKey("hot", "rollover", "check-rollover-ready"))), 30, TimeUnit.SECONDS);

        createNewSingletonPolicy(client(), policy, "hot", new RolloverAction(null, null, null, 1L), TimeValue.ZERO);

        // Move to the same step, which should re-read the policy
        Request moveToStepRequest = new Request("POST", "_ilm/move/test-1");
        moveToStepRequest.setJsonEntity("{\n" +
            "  \"current_step\": { \n" +
            "    \"phase\": \"hot\",\n" +
            "    \"action\": \"rollover\",\n" +
            "    \"name\": \"check-rollover-ready\"\n" +
            "  },\n" +
            "  \"next_step\": { \n" +
            "    \"phase\": \"hot\",\n" +
            "    \"action\": \"rollover\",\n" +
            "    \"name\": \"check-rollover-ready\"\n" +
            "  }\n" +
            "}");
        // busy asserting here as ILM moves the index from the `check-rollover-ready` step into the `error` step and back into the
        // `check-rollover-ready` when retrying. the `_ilm/move` api might fail when the as the `current_step` of the index might be
        //  the `error` step at execution time.
        assertBusy(() -> client().performRequest(moveToStepRequest), 30, TimeUnit.SECONDS);

        indexDocument(client(), "test-1", true);

        // Make sure we actually rolled over
        assertBusy(() -> {
            indexExists("test-000002");
        });
    }

    public void testMoveToStepWithInvalidNextStep() throws Exception {
        createNewSingletonPolicy(client(), policy, "delete", new DeleteAction(), TimeValue.timeValueDays(100));
        createIndexWithSettings(client(), index, alias, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(LifecycleSettings.LIFECYCLE_NAME, policy));

        // move to a step
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + index);
        moveToStepRequest.setJsonEntity("{\n" +
            "  \"current_step\": {\n" +
            "    \"phase\": \"new\",\n" +
            "    \"action\": \"complete\",\n" +
            "    \"name\": \"complete\"\n" +
            "  },\n" +
            "  \"next_step\": {\n" +
            "    \"phase\": \"hot\",\n" +
            "    \"action\": \"rollover\",\n" +
            "    \"name\": \"attempt-rollover\"\n" +
            "  }\n" +
            "}");
        assertBusy(() -> {
            ResponseException exception =
                expectThrows(ResponseException.class, () -> client().performRequest(moveToStepRequest));

            String responseEntityAsString = EntityUtils.toString(exception.getResponse().getEntity());
            String expectedErrorMessage = "step [{\\\"phase\\\":\\\"hot\\\",\\\"action\\\":\\\"rollover\\\",\\\"name\\\":" +
                "\\\"attempt-rollover\\\"}] for index [" + index + "] with policy [" + policy + "] does not exist";

            assertThat(responseEntityAsString, containsStringIgnoringCase(expectedErrorMessage));
        });
    }

    public void testMoveToStepWithoutStepName() throws Exception {
        createNewSingletonPolicy(client(), policy, "warm", new ForceMergeAction(1, null), TimeValue.timeValueHours(1));
        createIndexWithSettings(client(), index, alias, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(LifecycleSettings.LIFECYCLE_NAME, policy));

        // move to a step
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + index);
        moveToStepRequest.setJsonEntity("{\n" +
            "  \"current_step\": {\n" +
            "    \"phase\": \"new\",\n" +
            "    \"action\": \"complete\",\n" +
            "    \"name\": \"complete\"\n" +
            "  },\n" +
            "  \"next_step\": {\n" +
            "    \"phase\": \"warm\",\n" +
            "    \"action\": \"forcemerge\"\n" +
            "  }\n" +
            "}");

        assertOK(client().performRequest(moveToStepRequest));

        // Make sure we actually move on to and execute the forcemerge action
        assertBusy(() -> {
            assertThat(getStepKeyForIndex(client(), index), equalTo(PhaseCompleteStep.finalStep("warm").getKey()));
        }, 30, TimeUnit.SECONDS);
    }

    public void testMoveToStepWithoutAction() throws Exception {
        createNewSingletonPolicy(client(), policy, "warm", new ForceMergeAction(1, null), TimeValue.timeValueHours(1));
        createIndexWithSettings(client(), index, alias, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(LifecycleSettings.LIFECYCLE_NAME, policy));

        // move to a step
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + index);
        moveToStepRequest.setJsonEntity("{\n" +
            "  \"current_step\": {\n" +
            "    \"phase\": \"new\",\n" +
            "    \"action\": \"complete\",\n" +
       "    \"name\": \"complete\"\n" +
            "  },\n" +
            "  \"next_step\": {\n" +
            "    \"phase\": \"warm\"\n" +
            "  }\n" +
            "}");

        assertOK(client().performRequest(moveToStepRequest));

        // Make sure we actually move on to and execute the forcemerge action
        assertBusy(() -> {
            assertThat(getStepKeyForIndex(client(), index), equalTo(PhaseCompleteStep.finalStep("warm").getKey()));
        }, 30, TimeUnit.SECONDS);
    }

    public void testInvalidToMoveToStepWithoutActionButWithName() throws Exception {
        createNewSingletonPolicy(client(), policy, "warm", new ForceMergeAction(1, null), TimeValue.timeValueHours(1));
        createIndexWithSettings(client(), index, alias, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(LifecycleSettings.LIFECYCLE_NAME, policy));

        // move to a step with an invalid request
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + index);
        moveToStepRequest.setJsonEntity("{\n" +
            "  \"current_step\": {\n" +
            "    \"phase\": \"new\",\n" +
            "    \"action\": \"complete\",\n" +
            "    \"name\": \"complete\"\n" +
            "  },\n" +
            "  \"next_step\": {\n" +
            "    \"phase\": \"warm\",\n" +
            "    \"name\": \"forcemerge\"\n" +
            "  }\n" +
            "}");

        assertBusy(() -> {
            ResponseException exception =
                expectThrows(ResponseException.class, () -> client().performRequest(moveToStepRequest));
            String responseEntityAsString = EntityUtils.toString(exception.getResponse().getEntity());
            String expectedErrorMessage = "phase; phase and action; or phase, action, and step must be provided, " +
                "but a step name was specified without a corresponding action";
            assertThat(responseEntityAsString, containsStringIgnoringCase(expectedErrorMessage));
        });
    }

    public void testResolveToNonexistentStep() throws Exception {
        createNewSingletonPolicy(client(), policy, "warm", new ForceMergeAction(1, null), TimeValue.timeValueHours(1));
        createIndexWithSettings(client(), index, alias, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(LifecycleSettings.LIFECYCLE_NAME, policy));

        // move to a step with an invalid request
        Request moveToStepRequest = new Request("POST", "_ilm/move/" + index);
        moveToStepRequest.setJsonEntity("{\n" +
            "  \"current_step\": {\n" +
            "    \"phase\": \"new\",\n" +
            "    \"action\": \"complete\",\n" +
            "    \"name\": \"complete\"\n" +
            "  },\n" +
            "  \"next_step\": {\n" +
            "    \"phase\": \"warm\",\n" +
            "    \"action\": \"shrink\"\n" +
            "  }\n" +
            "}");

        assertBusy(() -> {
            ResponseException exception =
                expectThrows(ResponseException.class, () -> client().performRequest(moveToStepRequest));
            String responseEntityAsString = EntityUtils.toString(exception.getResponse().getEntity());
            String expectedErrorMessage = "unable to determine concrete step key from target next step key: " +
                "{\\\"phase\\\":\\\"warm\\\",\\\"action\\\":\\\"shrink\\\"}";
            assertThat(responseEntityAsString, containsStringIgnoringCase(expectedErrorMessage));
        });
    }
}
