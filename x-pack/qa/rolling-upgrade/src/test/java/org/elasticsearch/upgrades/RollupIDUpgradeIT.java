/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;


public class RollupIDUpgradeIT extends AbstractUpgradeTestCase {
    private static final Version UPGRADE_FROM_VERSION =
        Version.fromString(System.getProperty("tests.upgrade_from_version"));

    /**
     * This test verifies that as a cluster is upgraded incrementally, new documents eventually switch
     * over to the "new" form of ID (128 bit Murmur3 ids).
     *
     * Rollup IDs are essentially the hashed concatenation of keys returned by the composite aggregation,
     * so the field values that are being indexed (timestamp, value, etc) directly affect the
     * ID that is generated.
     *
     * We don't know which node will get the Rollup task to start, so we don't know when it will migrate.
     * The first doc is guaranteed to be the "old" style since all nodes are un-upgraded.  The second
     * and third phase will have a mixed cluster, and the rollup task may or may not migrate.  In those
     * phases we have two options (old and new) for the document added in the phase.
     *
     * The last phase is guaranteed to be new as it's a fully upgraded cluster.
     */
    public void testIDsUpgradeCorrectly() throws Exception {
        assumeTrue("Rollup became available in 6.3", UPGRADE_FROM_VERSION.onOrAfter(Version.V_6_3_0));
        assumeTrue("Rollup ID change happened in 6.4", UPGRADE_FROM_VERSION.before(Version.V_6_4_0));
        switch (CLUSTER_TYPE) {
            case OLD:
                break;
            case MIXED:
                Request waitForYellow = new Request("GET", "/_cluster/health");
                waitForYellow.addParameter("wait_for_nodes", "3");
                waitForYellow.addParameter("wait_for_status", "yellow");
                client().performRequest(waitForYellow);
                break;
            case UPGRADED:
                Request waitForGreen = new Request("GET", "/_cluster/health/target,rollup");
                waitForGreen.addParameter("wait_for_nodes", "3");
                waitForGreen.addParameter("wait_for_status", "green");
                // wait for long enough that we give delayed unassigned shards to stop being delayed
                waitForGreen.addParameter("timeout", "70s");
                waitForGreen.addParameter("level", "shards");
                client().performRequest(waitForGreen);
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }

        OffsetDateTime timestamp = Instant.parse("2018-01-01T00:00:01.000Z").atOffset(ZoneOffset.UTC);

        if (CLUSTER_TYPE == ClusterType.OLD) {
            String recoverQuickly = "{\"settings\": {\"index.unassigned.node_left.delayed_timeout\": \"100ms\"}}";

            Request createTargetIndex = new Request("PUT", "/target");
            createTargetIndex.setJsonEntity(recoverQuickly);
            client().performRequest(createTargetIndex);

            final Request indexRequest = new Request("POST", "/target/_doc/1");
            indexRequest.setJsonEntity("{\"timestamp\":\"" + timestamp.toString() + "\",\"value\":123}");
            client().performRequest(indexRequest);

            // create the rollup job
            final Request createRollupJobRequest = new Request("PUT", "/_rollup/job/rollup-id-test");
            createRollupJobRequest.setJsonEntity("{"
                + "\"index_pattern\":\"target\","
                + "\"rollup_index\":\"rollup\","
                + "\"cron\":\"*/1 * * * * ?\","
                + "\"page_size\":100,"
                + "\"groups\":{"
                + "    \"date_histogram\":{"
                + "        \"field\":\"timestamp\","
                + "        \"interval\":\"5m\""
                + "      },"
                +       "\"histogram\":{"
                + "        \"fields\": [\"value\"],"
                + "        \"interval\":1"
                + "      },"
                +       "\"terms\":{"
                + "        \"fields\": [\"value\"]"
                + "      }"
                + "},"
                + "\"metrics\":["
                + "    {\"field\":\"value\",\"metrics\":[\"min\",\"max\",\"sum\"]}"
                + "]"
                + "}");

            Map<String, Object> createRollupJobResponse = entityAsMap(client().performRequest(createRollupJobRequest));
            assertThat(createRollupJobResponse.get("acknowledged"), equalTo(Boolean.TRUE));

            Request updateSettings = new Request("PUT", "/rollup/_settings");
            updateSettings.setJsonEntity(recoverQuickly);
            client().performRequest(updateSettings);

            // start the rollup job
            final Request startRollupJobRequest = new Request("POST", "_rollup/job/rollup-id-test/_start");
            Map<String, Object> startRollupJobResponse = entityAsMap(client().performRequest(startRollupJobRequest));
            assertThat(startRollupJobResponse.get("started"), equalTo(Boolean.TRUE));

            assertRollUpJob("rollup-id-test");
            List<String> ids = getSearchResults(1);
            assertThat(ids, containsInAnyOrder("3310683722"));

        }

        if (CLUSTER_TYPE == ClusterType.MIXED && Booleans.parseBoolean(System.getProperty("tests.first_round"))) {
            final Request indexRequest = new Request("POST", "/target/_doc/2");
            indexRequest.setJsonEntity("{\"timestamp\":\"" + timestamp.plusDays(1).toString() + "\",\"value\":345}");
            client().performRequest(indexRequest);

            assertRollUpJob("rollup-id-test");
            client().performRequest(new Request("POST", "rollup/_refresh"));

            List<String> ids = getSearchResults(2);
            // first doc, guaranteed old style
            ids.remove("3310683722");

            // next doc may be either style
            ids.removeAll(Arrays.asList("621059582", "rollup-id-test$ehY4NAyVSy8xxUDZrNXXIA"));
            assertThat(ids.toString(),ids.size(), equalTo(0));
        }

        if (CLUSTER_TYPE == ClusterType.MIXED && Booleans.parseBoolean(System.getProperty("tests.first_round")) == false) {
            final Request indexRequest = new Request("POST", "/target/_doc/3");
            indexRequest.setJsonEntity("{\"timestamp\":\"" + timestamp.plusDays(2).toString() + "\",\"value\":456}");
            client().performRequest(indexRequest);

            assertRollUpJob("rollup-id-test");
            client().performRequest(new Request("POST", "rollup/_refresh"));

            List<String> ids = getSearchResults(3);

            // first doc, guaranteed old style
            ids.remove("3310683722");

            // next two docs may be either style
            ids.removeAll(Arrays.asList("621059582", "4288019978",
                "rollup-id-test$ehY4NAyVSy8xxUDZrNXXIA", "rollup-id-test$60RGDSb92YI5LH4_Fnq_1g"));
            assertThat(ids.toString(), ids.size(), equalTo(0));

        }

        if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            final Request indexRequest = new Request("POST", "/target/_doc/4");
            indexRequest.setJsonEntity("{\"timestamp\":\"" + timestamp.plusDays(3).toString() + "\",\"value\":567}");
            client().performRequest(indexRequest);

            assertRollUpJob("rollup-id-test");
            client().performRequest(new Request("POST", "rollup/_refresh"));

            List<String> ids = getSearchResults(4);
            // first doc, guaranteed old style
            ids.remove("3310683722");

            // next two docs may be either style
            ids.removeAll(Arrays.asList("621059582", "4288019978",
                "rollup-id-test$ehY4NAyVSy8xxUDZrNXXIA", "rollup-id-test$60RGDSb92YI5LH4_Fnq_1g"));

            // Last is guaranteed to be new
            ids.remove("rollup-id-test$LAKZftDeQwsUtdPixrkkzQ");
            assertThat(ids.toString(), ids.size(), equalTo(0));
        }

    }

    private List<String> getSearchResults(int expectedCount) throws Exception {
        final List<String> collectedIDs = new ArrayList<>();
        assertBusy(() -> {
            collectedIDs.clear();
            client().performRequest(new Request("POST", "rollup/_refresh"));
            final Request searchRequest = new Request("GET", "rollup/_search");
            searchRequest.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
            try {
                Map<String, Object> searchResponse = entityAsMap(client().performRequest(searchRequest));
                assertNotNull(ObjectPath.eval("hits.total", searchResponse));
                assertThat(ObjectPath.eval("hits.total", searchResponse), equalTo(expectedCount));

                for (int i = 0; i < expectedCount; i++) {
                    String id = ObjectPath.eval("hits.hits." + i + "._id", searchResponse);
                    collectedIDs.add(id);
                    Map<String, Object> doc = ObjectPath.eval("hits.hits." + i + "._source", searchResponse);
                    assertNotNull(doc);
                    if (id.startsWith("rollup-id-test")) {
                        assertThat(doc.get("_rollup.version"), equalTo(2));
                    } else {
                        assertThat(doc.get("_rollup.version"), equalTo(1));
                    }
                }
            } catch (IOException e) {
                fail();
            }
        });
        return collectedIDs;
    }

    @SuppressWarnings("unchecked")
    private void assertRollUpJob(final String rollupJob) throws Exception {
        final Matcher<?> expectedStates = anyOf(equalTo("indexing"), equalTo("started"));
        waitForRollUpJob(rollupJob, expectedStates);

        // check that the rollup job is started using the RollUp API
        final Request getRollupJobRequest = new Request("GET", "_rollup/job/" + rollupJob);
        Map<String, Object> getRollupJobResponse = entityAsMap(client().performRequest(getRollupJobRequest));
        Map<String, Object> job = getJob(getRollupJobResponse, rollupJob);
        if (job != null) {
            assertThat(ObjectPath.eval("status.job_state", job), expectedStates);
        }

        // check that the rollup job is started using the Tasks API
        final Request taskRequest = new Request("GET", "_tasks");
        taskRequest.addParameter("detailed", "true");
        taskRequest.addParameter("actions", "xpack/rollup/*");
        Map<String, Object> taskResponse = entityAsMap(client().performRequest(taskRequest));
        Map<String, Object> taskResponseNodes = (Map<String, Object>) taskResponse.get("nodes");
        Map<String, Object> taskResponseNode = (Map<String, Object>) taskResponseNodes.values().iterator().next();
        Map<String, Object> taskResponseTasks = (Map<String, Object>) taskResponseNode.get("tasks");
        Map<String, Object> taskResponseStatus = (Map<String, Object>) taskResponseTasks.values().iterator().next();
        assertThat(ObjectPath.eval("status.job_state", taskResponseStatus), expectedStates);

        // check that the rollup job is started using the Cluster State API
        final Request clusterStateRequest = new Request("GET", "_cluster/state/metadata");
        Map<String, Object> clusterStateResponse = entityAsMap(client().performRequest(clusterStateRequest));
        List<Map<String, Object>> rollupJobTasks = ObjectPath.eval("metadata.persistent_tasks.tasks", clusterStateResponse);

        boolean hasRollupTask = false;
        for (Map<String, Object> task : rollupJobTasks) {
            if (ObjectPath.eval("id", task).equals(rollupJob)) {
                hasRollupTask = true;
                break;
            }
        }
        if (hasRollupTask == false) {
            fail("Expected persistent task for [" + rollupJob + "] but none found.");
        }

    }

    private void waitForRollUpJob(final String rollupJob, final Matcher<?> expectedStates) throws Exception {
        assertBusy(() -> {
            final Request getRollupJobRequest = new Request("GET", "_rollup/job/" + rollupJob);
            Response getRollupJobResponse = client().performRequest(getRollupJobRequest);
            assertThat(getRollupJobResponse.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));

            Map<String, Object> job = getJob(getRollupJobResponse, rollupJob);
            if (job != null) {
                assertThat(ObjectPath.eval("status.job_state", job), expectedStates);
            }
        }, 30L, TimeUnit.SECONDS);
    }

    private static Map<String, Object> getJob(Response response, String targetJobId) throws IOException {
        return getJob(ESRestTestCase.entityAsMap(response), targetJobId);
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> getJob(Map<String, Object> jobsMap, String targetJobId) throws IOException {

        List<Map<String, Object>> jobs =
            (List<Map<String, Object>>) XContentMapValues.extractValue("jobs", jobsMap);

        if (jobs == null) {
            return null;
        }

        for (Map<String, Object> job : jobs) {
            String jobId = (String) ((Map<String, Object>) job.get("config")).get("id");
            if (jobId.equals(targetJobId)) {
                return job;
            }
        }
        return null;
    }
}
