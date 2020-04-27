/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.integration;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.xpack.core.transform.TransformField;
import org.elasticsearch.xpack.core.transform.transforms.persistence.TransformInternalIndexConstants;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class TransformRobustnessIT extends TransformRestTestCase {

    public void testTaskRemovalAfterInternalIndexGotDeleted() throws Exception {
        String indexName = "continuous_reviews";
        createReviewsIndex(indexName);
        String transformId = "simple_continuous_pivot";
        String transformIndex = "pivot_reviews_continuous";
        final Request createTransformRequest = new Request("PUT", TransformField.REST_BASE_PATH_TRANSFORMS + transformId);
        String config = "{"
            + " \"source\": {\"index\":\""
            + indexName
            + "\"},"
            + " \"dest\": {\"index\":\""
            + transformIndex
            + "\"},"
            + " \"frequency\": \"1s\","
            + " \"sync\": {\"time\": {\"field\": \"timestamp\", \"delay\": \"1s\"}},"
            + " \"pivot\": {"
            + "   \"group_by\": {"
            + "     \"reviewer\": {"
            + "       \"terms\": {"
            + "         \"field\": \"user_id\""
            + " } } },"
            + "   \"aggregations\": {"
            + "     \"avg_rating\": {"
            + "       \"avg\": {"
            + "         \"field\": \"stars\""
            + " } } } }"
            + "}";
        createTransformRequest.setJsonEntity(config);
        Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
        assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));
        assertEquals(1, getTransforms().size());
        // there shouldn't be a task yet
        assertEquals(0, getNumberOfTransformTasks());
        startAndWaitForContinuousTransform(transformId, transformIndex, null);
        assertTrue(indexExists(transformIndex));

        // a task exists
        assertEquals(1, getNumberOfTransformTasks());
        // get and check some users
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_0", 3.776978417);
        assertOnePivotValue(transformIndex + "/_search?q=reviewer:user_5", 3.72);
        assertNotNull(getTransformState(transformId));

        assertEquals(1, getTransforms().size());

        // delete the transform index
        beEvilAndDeleteTheTransformIndex();
        // transform is gone
        assertEquals(0, getTransforms().size());
        // but the task is still there
        assertEquals(1, getNumberOfTransformTasks());

        Request stopTransformRequest = new Request("POST", TransformField.REST_BASE_PATH_TRANSFORMS + transformId + "/_stop");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(stopTransformRequest));

        assertEquals(409, e.getResponse().getStatusLine().getStatusCode());
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
        assertEquals(0, getNumberOfTransformTasks());

        // delete the transform because the task might have written a state doc, cleanup fails if the index isn't empty
        deleteTransform(transformId);
    }

    @SuppressWarnings("unchecked")
    private int getNumberOfTransformTasks() throws IOException {
        final Request tasksRequest = new Request("GET", "/_tasks");
        tasksRequest.addParameter("actions", TransformField.TASK_NAME + "*");
        Map<String, Object> tasksResponse = entityAsMap(client().performRequest(tasksRequest));

        Map<String, Object> nodes = (Map<String, Object>) tasksResponse.get("nodes");
        if (nodes == null) {
            return 0;
        }

        int foundTasks = 0;
        for (Entry<String, Object> node : nodes.entrySet()) {
            Map<String, Object> nodeInfo = (Map<String, Object>) node.getValue();
            Map<String, Object> tasks = (Map<String, Object>) nodeInfo.get("tasks");
            foundTasks += tasks != null ? tasks.size() : 0;
        }

        return foundTasks;
    }

    private void beEvilAndDeleteTheTransformIndex() throws IOException {
        adminClient().performRequest(new Request("DELETE", TransformInternalIndexConstants.LATEST_INDEX_NAME));
    }
}
