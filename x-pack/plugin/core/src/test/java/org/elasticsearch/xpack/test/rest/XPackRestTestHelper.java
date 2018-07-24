/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.test.rest;


import org.apache.http.HttpStatus;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksAction;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.ml.MlMetaIndex;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.notifications.AuditorField;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.assertEquals;

public final class XPackRestTestHelper {

    private XPackRestTestHelper() {
    }

    /**
     * Waits for the Machine Learning templates to be created
     * and check the version is up to date
     */
    public static void waitForMlTemplates(RestClient client) throws InterruptedException {
        AtomicReference<Version> masterNodeVersion = new AtomicReference<>();
        ESTestCase.awaitBusy(() -> {
            String response;
            try {
                Request request = new Request("GET", "/_cat/nodes");
                request.addParameter("h", "master,version");
                response = EntityUtils.toString(client.performRequest(request).getEntity());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            for (String line : response.split("\n")) {
                if (line.startsWith("*")) {
                    masterNodeVersion.set(Version.fromString(line.substring(2).trim()));
                    return true;
                }
            }
            return false;
        });

        final List<String> templateNames = Arrays.asList(AuditorField.NOTIFICATIONS_INDEX, MlMetaIndex.INDEX_NAME,
                AnomalyDetectorsIndex.jobStateIndexName(), AnomalyDetectorsIndex.jobResultsIndexPrefix());
        for (String template : templateNames) {
            ESTestCase.awaitBusy(() -> {
                Map<?, ?> response;
                try {
                    String string = EntityUtils.toString(client.performRequest(new Request("GET", "/_template/" + template)).getEntity());
                    response = XContentHelper.convertToMap(JsonXContent.jsonXContent, string, false);
                } catch (ResponseException e) {
                    if (e.getResponse().getStatusLine().getStatusCode() == 404) {
                        return false;
                    }
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                Map<?, ?> templateDefinition = (Map<?, ?>) response.get(template);
                return Version.fromId((Integer) templateDefinition.get("version")).equals(masterNodeVersion.get());
            });
        }
    }

    /**
     * Waits for pending tasks to complete
     */
    public static void waitForPendingTasks(RestClient adminClient) throws Exception {
        ESTestCase.assertBusy(() -> {
            try {
                Request request = new Request("GET", "/_cat/tasks");
                request.addParameter("detailed", "true");
                Response response = adminClient.performRequest(request);
                // Check to see if there are tasks still active. We exclude the
                // list tasks
                // actions tasks form this otherwise we will always fail
                if (response.getStatusLine().getStatusCode() == HttpStatus.SC_OK) {
                    try (BufferedReader responseReader = new BufferedReader(
                            new InputStreamReader(response.getEntity().getContent(), StandardCharsets.UTF_8))) {
                        int activeTasks = 0;
                        String line;
                        StringBuilder tasksListString = new StringBuilder();
                        while ((line = responseReader.readLine()) != null) {
                            if (line.startsWith(ListTasksAction.NAME) == false) {
                                activeTasks++;
                                tasksListString.append(line);
                                tasksListString.append('\n');
                            }
                        }
                        assertEquals(activeTasks + " active tasks found:\n" + tasksListString, 0, activeTasks);
                    }
                }
            } catch (IOException e) {
                throw new AssertionError("Error getting active tasks list", e);
            }
        });
    }
}
