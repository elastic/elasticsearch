/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.Strings.format;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class TransformRobustnessIT extends TransformRestTestCase {

    private static final String DANGLING_TASK_ERROR_MESSAGE =
        "Found task for transform [simple_continuous_pivot], but no configuration for it. "
            + "To delete this transform use DELETE with force=true.";

    public void testTaskRemovalAfterInternalIndexGotDeleted() throws Exception {
        String indexName = "continuous_reviews";
        createReviewsIndex(indexName);
        String transformId = "simple_continuous_pivot";
        String transformIndex = "pivot_reviews_continuous";
        final Request createTransformRequest = new Request("PUT", TransformField.REST_BASE_PATH_TRANSFORMS + transformId);
        String config = createConfig(indexName, transformIndex);
        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));
        assertThat(getTransforms(null), hasSize(1));
        // there shouldn't be a task yet
        assertThat(getTransformTasks(), is(empty()));
        startAndWaitForContinuousTransform(transformId, transformIndex, null);
        assertTrue(indexExists(transformIndex));

        // a task exists
        assertThat(getTransformTasks(), hasSize(1));
        // get and check some users
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_0", 3.776978417);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_5", 3.72);
        assertNotNull(getTransformState(transformId));

        assertThat(getTransforms(null), hasSize(1));

        // delete the transform index
        beEvilAndDeleteTheTransformIndex();
        // transform is gone
        assertThat(getTransforms(List.of(Map.of("type", "dangling_task", "reason", DANGLING_TASK_ERROR_MESSAGE))), is(empty()));
        // but the task is still there
        assertThat(getTransformTasks(), hasSize(1));

        Request stopTransformRequest = new Request("POST", TransformField.REST_BASE_PATH_TRANSFORMS + transformId + "/_stop");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(stopTransformRequest));

        assertThat(e.getResponse().getStatusLine().getStatusCode(), is(equalTo(409)));
        assertThat(
            e.getMessage(),
            containsString("Detected transforms with no config [" + transformId + "]. Use force to stop/delete them.")
        );
        stopTransformRequest.addParameter(TransformField.FORCE.getPreferredName(), Boolean.toString(true));

        // make sync, to avoid in-flux state, see gh#51347
        stopTransformRequest.addParameter(TransformField.WAIT_FOR_COMPLETION.getPreferredName(), Boolean.toString(true));

        Map<String, Object> stopTransformResponse = entityAsMap(client().performRequest(stopTransformRequest));
        assertThat(stopTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));

        // the task is gone
        assertThat(getTransformTasks(), is(empty()));

        // delete the transform because the task might have written a state doc, cleanup fails if the index isn't empty
        deleteTransform(transformId);
    }

    public void testBatchTransformLifecycltInALoop() throws IOException {
        createReviewsIndex();

        String transformId = "test_batch_lifecycle_in_a_loop";
        String destIndex = transformId + "-dest";
        for (int i = 0; i < 100; ++i) {
            try {
                // Create the batch transform
                createPivotReviewsTransform(transformId, destIndex, null);
                assertThat(getTransformTasks(), is(empty()));
                assertThat(getTransformTasksFromClusterState(transformId), is(empty()));

                // Wait until the transform finishes
                startAndWaitForTransform(transformId, destIndex);

                // After the transform finishes, there should be no transform task left
                assertThat(getTransformTasks(), is(empty()));
                assertThat(getTransformTasksFromClusterState(transformId), is(empty()));

                // Delete the transform
                deleteTransform(transformId);
            } catch (AssertionError | Exception e) {
                throw new AssertionError(format("Failure at iteration %d: %s", i, e.getMessage()), e);
            }
        }
    }

    public void testInterruptedBatchTransformLifecycltInALoop() throws IOException {
        createReviewsIndex();

        String transformId = "test_interrupted_batch_lifecycle_in_a_loop";
        String destIndex = transformId + "-dest";
        for (int i = 0; i < 100; ++i) {
            long sleepAfterStartMillis = randomLongBetween(0, 1_000);
            boolean force = randomBoolean();
            try {
                // Create the batch transform.
                createPivotReviewsTransform(transformId, destIndex, null);
                assertThat(getTransformTasks(), is(empty()));
                assertThat(getTransformTasksFromClusterState(transformId), is(empty()));

                startTransform(transformId);
                // There is 1 transform task after start.
                assertThat(getTransformTasks(), hasSize(1));
                assertThat(getTransformTasksFromClusterState(transformId), hasSize(1));

                Thread.sleep(sleepAfterStartMillis);

                // Stop the transform with force set randomly.
                stopTransform(transformId, force);
                // After the transform is stopped, there should be no transform task left.
                if (force) {
                    // If the "force" has been used, then the persistent task is removed from the cluster state but the local task can still
                    // be seen by the PersistentTasksNodeService. We need to wait until PersistentTasksNodeService reconciles the state.
                    assertBusy(() -> assertThat(getTransformTasks(), is(empty())));
                } else {
                    // If the "force" hasn't been used then we can expect the local task to be already gone.
                    assertThat(getTransformTasks(), is(empty()));
                }
                assertThat(getTransformTasksFromClusterState(transformId), is(empty()));

                // Delete the transform.
                deleteTransform(transformId);
            } catch (AssertionError | Exception e) {
                throw new AssertionError(
                    format("Failure at iteration %d (sleepAfterStart=%sms,force=%s): %s", i, sleepAfterStartMillis, force, e.getMessage()),
                    e
                );
            }
        }
    }

    public void testContinuousTransformLifecycleInALoop() throws Exception {
        createReviewsIndex();

        String transformId = "test_cont_lifecycle_in_a_loop";
        String destIndex = transformId + "-dest";
        for (int i = 0; i < 100; ++i) {
            long sleepAfterStartMillis = randomLongBetween(0, 5_000);
            boolean force = randomBoolean();
            try {
                // Create the continuous transform.
                createContinuousPivotReviewsTransform(transformId, destIndex, null);
                assertThat(getTransformTasks(), is(empty()));
                assertThat(getTransformTasksFromClusterState(transformId), is(empty()));

                startTransform(transformId);
                // There is 1 transform task after start.
                assertThat(getTransformTasks(), hasSize(1));
                assertThat(getTransformTasksFromClusterState(transformId), hasSize(1));

                Thread.sleep(sleepAfterStartMillis);
                // There should still be 1 transform task as the transform is continuous.
                assertThat(getTransformTasks(), hasSize(1));
                assertThat(getTransformTasksFromClusterState(transformId), hasSize(1));

                // Stop the transform with force set randomly.
                stopTransform(transformId, force);
                if (force) {
                    // If the "force" has been used, then the persistent task is removed from the cluster state but the local task can still
                    // be seen by the PersistentTasksNodeService. We need to wait until PersistentTasksNodeService reconciles the state.
                    assertBusy(() -> assertThat(getTransformTasks(), is(empty())));
                } else {
                    // If the "force" hasn't been used then we can expect the local task to be already gone.
                    assertThat(getTransformTasks(), is(empty()));
                }
                assertThat(getTransformTasksFromClusterState(transformId), is(empty()));

                // Delete the transform.
                deleteTransform(transformId);
            } catch (AssertionError | Exception e) {
                throw new AssertionError(
                    format("Failure at iteration %d (sleepAfterStart=%sms,force=%s): %s", i, sleepAfterStartMillis, force, e.getMessage()),
                    e
                );
            }
        }
    }

    public void testCancellingTransformTask() throws Exception {
        createReviewsIndex();
        String transformId = "cancelling_transform_task";
        String transformIndex = transformId + "-dest";

        // Create the transform.
        Request createTransformRequest = new Request("PUT", TransformField.REST_BASE_PATH_TRANSFORMS + transformId);
        createTransformRequest.setJsonEntity(createConfig(REVIEWS_INDEX_NAME, transformIndex));
        assertAcknowledged(client().performRequest(createTransformRequest));
        assertThat(getTransforms(null), hasSize(1));

        // Verify that there are no transform tasks yet.
        assertThat(getTransformTasks(), is(empty()));

        // Start the transform.
        startTransform(transformId);

        // Verify that the transform task already exists.
        List<String> tasks = getTransformTasks();
        assertThat(tasks, hasSize(1));

        // Sleep for a few seconds so that we cover the task being cancelled at various stages.
        Thread.sleep(randomLongBetween(0, 5_000));

        // Cancel the transform task.
        Request cancelTaskRequest = new Request("POST", "/_tasks/" + tasks.get(0) + "/_cancel");
        assertOK(client().performRequest(cancelTaskRequest));

        // Wait until the transform is stopped.
        assertBusy(() -> {
            Map<?, ?> transformStatsAsMap = getTransformStateAndStats(transformId);
            assertEquals("Stats were: " + transformStatsAsMap, "stopped", XContentMapValues.extractValue("state", transformStatsAsMap));
        }, 30, TimeUnit.SECONDS);

        // Verify that the transform is still present.
        assertThat(getTransforms(null), hasSize(1));

        // Verify that there is no transform task left.
        assertThat(getTransformTasks(), is(empty()));
    }

    private void beEvilAndDeleteTheTransformIndex() throws IOException {
        final Request deleteRequest = new Request("DELETE", TransformInternalIndexConstants.LATEST_INDEX_NAME);
        deleteRequest.setOptions(
            expectWarnings(
                "this request accesses system indices: ["
                    + TransformInternalIndexConstants.LATEST_INDEX_NAME
                    + "], but in a future major version, direct access to system indices will "
                    + "be prevented by default"
            )
        );
        adminClient().performRequest(deleteRequest);
    }

    private static String createConfig(String sourceIndex, String destIndex) {
        return format("""
            {
              "source": {
                "index": "%s"
              },
              "dest": {
                "index": "%s"
              },
              "frequency": "1s",
              "sync": {
                "time": {
                  "field": "timestamp",
                  "delay": "1s"
                }
              },
              "pivot": {
                "group_by": {
                  "reviewer": {
                    "terms": {
                      "field": "user_id"
                    }
                  }
                },
                "aggregations": {
                  "avg_rating": {
                    "avg": {
                      "field": "stars"
                    }
                  }
                }
              }
            }""", sourceIndex, destIndex);
    }
}
