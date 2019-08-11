/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.restart;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.ObjectPath;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.test.StreamsUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.upgrades.AbstractFullClusterRestartTestCase;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class FullClusterRestartIT extends AbstractFullClusterRestartTestCase {

    public static final String INDEX_ACTION_TYPES_DEPRECATION_MESSAGE =
        "[types removal] Specifying types in a watcher index action is deprecated.";

    public static final int UPGRADE_FIELD_EXPECTED_INDEX_FORMAT_VERSION = 6;
    public static final int SECURITY_EXPECTED_INDEX_FORMAT_VERSION = 6;

    @Override
    protected Settings restClientSettings() {
        String token = "Basic " + Base64.getEncoder().encodeToString("test_user:x-pack-test-password".getBytes(StandardCharsets.UTF_8));
        return Settings.builder()
                .put(ThreadContext.PREFIX + ".Authorization", token)
                // we increase the timeout here to 90 seconds to handle long waits for a green
                // cluster health. the waits for green need to be longer than a minute to
                // account for delayed shards
                .put(ESRestTestCase.CLIENT_SOCKET_TIMEOUT, "90s")
                .build();
    }

    /**
     * Tests that a single document survives. Super basic smoke test.
     */
    public void testSingleDoc() throws IOException {
        String docLocation = "/testsingledoc/_doc/1";
        String doc = "{\"test\": \"test\"}";

        if (isRunningAgainstOldCluster()) {
            Request createDoc = new Request("PUT", docLocation);
            createDoc.addParameter("refresh", "true");
            createDoc.setJsonEntity(doc);
            client().performRequest(createDoc);
        }

        Request getRequest = new Request("GET", docLocation);
        assertThat(toStr(client().performRequest(getRequest)), containsString(doc));
    }

    @SuppressWarnings("unchecked")
    public void testSecurityNativeRealm() throws Exception {
        if (isRunningAgainstOldCluster()) {
            createUser(true);
            createRole(true);
        } else {
            waitForYellow(".security");
            Response settingsResponse = client().performRequest(new Request("GET", "/.security/_settings/index.format"));
            Map<String, Object> settingsResponseMap = entityAsMap(settingsResponse);
            logger.info("settings response map {}", settingsResponseMap);
            final String concreteSecurityIndex;
            if (settingsResponseMap.isEmpty()) {
                fail("The security index does not have the expected setting [index.format]");
            } else {
                concreteSecurityIndex = settingsResponseMap.keySet().iterator().next();
                Map<String, Object> indexSettingsMap =
                        (Map<String, Object>) settingsResponseMap.get(concreteSecurityIndex);
                Map<String, Object> settingsMap = (Map<String, Object>) indexSettingsMap.get("settings");
                logger.info("settings map {}", settingsMap);
                if (settingsMap.containsKey("index")) {
                    @SuppressWarnings("unchecked")
                    int format = Integer.parseInt(String.valueOf(((Map<String, Object>)settingsMap.get("index")).get("format")));
                    assertEquals("The security index needs to be upgraded", SECURITY_EXPECTED_INDEX_FORMAT_VERSION, format);
                }
            }

            // create additional user and role
            createUser(false);
            createRole(false);
        }

        assertUserInfo(isRunningAgainstOldCluster());
        assertRoleInfo(isRunningAgainstOldCluster());
    }

    @SuppressWarnings("unchecked")
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/40178")
    public void testWatcher() throws Exception {
        if (isRunningAgainstOldCluster()) {
            logger.info("Adding a watch on old cluster {}", getOldClusterVersion());
            Request createBwcWatch = new Request("PUT", getWatcherEndpoint() + "/watch/bwc_watch");
            Request createBwcThrottlePeriod = new Request("PUT", getWatcherEndpoint() + "/watch/bwc_throttle_period");
            if (getOldClusterVersion().onOrAfter(Version.V_7_0_0)) {
                createBwcWatch.setOptions(expectWarnings(INDEX_ACTION_TYPES_DEPRECATION_MESSAGE));
                createBwcThrottlePeriod.setOptions(expectWarnings(INDEX_ACTION_TYPES_DEPRECATION_MESSAGE));
            }
            createBwcWatch.setJsonEntity(loadWatch("simple-watch.json"));
            client().performRequest(createBwcWatch);

            logger.info("Adding a watch with \"fun\" throttle periods on old cluster");
            createBwcThrottlePeriod.setJsonEntity(loadWatch("throttle-period-watch.json"));
            client().performRequest(createBwcThrottlePeriod);

            logger.info("Adding a watch with \"fun\" read timeout on old cluster");
            Request createFunnyTimeout = new Request("PUT", getWatcherEndpoint() + "/watch/bwc_funny_timeout");
            createFunnyTimeout.setJsonEntity(loadWatch("funny-timeout-watch.json"));
            client().performRequest(createFunnyTimeout);

            logger.info("Waiting for watch results index to fill up...");
            waitForYellow(".watches,bwc_watch_index,.watcher-history*");
            waitForHits("bwc_watch_index", 2);
            waitForHits(".watcher-history*", 2);
            logger.info("Done creating watcher-related indices");
        } else {
            logger.info("testing against {}", getOldClusterVersion());
            waitForYellow(".watches,bwc_watch_index,.watcher-history*");

            logger.info("checking that the Watches index is the correct version");

            Response settingsResponse = client().performRequest(new Request("GET", "/.watches/_settings/index.format"));
            Map<String, Object> settingsResponseMap = entityAsMap(settingsResponse);
            logger.info("settings response map {}", settingsResponseMap);
            final String concreteWatchesIndex;
            if (settingsResponseMap.isEmpty()) {
                fail("The security index does not have the expected setting [index.format]");
            } else {
                concreteWatchesIndex = settingsResponseMap.keySet().iterator().next();
                Map<String, Object> indexSettingsMap = (Map<String, Object>) settingsResponseMap.get(concreteWatchesIndex);
                Map<String, Object> settingsMap = (Map<String, Object>) indexSettingsMap.get("settings");
                logger.info("settings map {}", settingsMap);
                if (settingsMap.containsKey("index")) {
                    int format = Integer.parseInt(String.valueOf(((Map<String, Object>)settingsMap.get("index")).get("format")));
                    assertEquals("The watches index needs to be upgraded", UPGRADE_FIELD_EXPECTED_INDEX_FORMAT_VERSION, format);
                }
            }

            // Wait for watcher to actually start....
            Map<String, Object> startWatchResponse = entityAsMap(client().performRequest(new Request("POST", "_watcher/_start")));
            assertThat(startWatchResponse.get("acknowledged"), equalTo(Boolean.TRUE));
            assertBusy(() -> {
                Map<String, Object> statsWatchResponse = entityAsMap(client().performRequest(new Request("GET", "_watcher/stats")));
                @SuppressWarnings("unchecked")
                List<Object> states = ((List<Object>) statsWatchResponse.get("stats"))
                    .stream().map(o -> ((Map<String, Object>) o).get("watcher_state")).collect(Collectors.toList());
                assertThat(states, everyItem(is("started")));
            });

            try {
                assertOldTemplatesAreDeleted();
                assertWatchIndexContentsWork();
                assertBasicWatchInteractions();
            } finally {
                /* Shut down watcher after every test because watcher can be a bit finicky about shutting down when the node shuts
                 * down. This makes super sure it shuts down *and* causes the test to fail in a sensible spot if it doesn't shut down.
                 */
                Map<String, Object> stopWatchResponse = entityAsMap(client().performRequest(new Request("POST", "_watcher/_stop")));
                assertThat(stopWatchResponse.get("acknowledged"), equalTo(Boolean.TRUE));
                assertBusy(() -> {
                    Map<String, Object> statsStoppedWatchResponse = entityAsMap(client().performRequest(
                        new Request("GET", "_watcher/stats")));
                    @SuppressWarnings("unchecked")
                    List<Object> states = ((List<Object>) statsStoppedWatchResponse.get("stats"))
                        .stream().map(o -> ((Map<String, Object>) o).get("watcher_state")).collect(Collectors.toList());
                    assertThat(states, everyItem(is("stopped")));
                });
            }
        }
    }

    /**
     * Tests that a RollUp job created on a old cluster is correctly restarted after the upgrade.
     */
    public void testRollupAfterRestart() throws Exception {
        if (isRunningAgainstOldCluster()) {
            final int numDocs = 59;
            final int year = randomIntBetween(1970, 2018);

            // index documents for the rollup job
            final StringBuilder bulk = new StringBuilder();
            for (int i = 0; i < numDocs; i++) {
                if (getOldClusterVersion().onOrAfter(Version.V_7_0_0)) {
                    bulk.append("{\"index\":{\"_index\":\"rollup-docs\"}}\n");
                } else {
                    bulk.append("{\"index\":{\"_index\":\"rollup-docs\",\"_type\":\"doc\"}}\n");
                }
                String date = String.format(Locale.ROOT, "%04d-01-01T00:%02d:00Z", year, i);
                bulk.append("{\"timestamp\":\"").append(date).append("\",\"value\":").append(i).append("}\n");
            }
            bulk.append("\r\n");

            final Request bulkRequest = new Request("POST", "/_bulk");
            bulkRequest.setJsonEntity(bulk.toString());
            client().performRequest(bulkRequest);

            // create the rollup job
            final Request createRollupJobRequest = new Request("PUT", getRollupEndpoint() + "/job/rollup-job-test");

            String intervalType;
            if (getOldClusterVersion().onOrAfter(Version.V_7_2_0)) {
                intervalType = "fixed_interval";
            } else {
                intervalType = "interval";
            }

            createRollupJobRequest.setJsonEntity("{"
                    + "\"index_pattern\":\"rollup-*\","
                    + "\"rollup_index\":\"results-rollup\","
                    + "\"cron\":\"*/30 * * * * ?\","
                    + "\"page_size\":100,"
                    + "\"groups\":{"
                    + "    \"date_histogram\":{"
                    + "        \"field\":\"timestamp\","
                    + "        \"" + intervalType + "\":\"5m\""
                    + "      }"
                    + "},"
                    + "\"metrics\":["
                    + "    {\"field\":\"value\",\"metrics\":[\"min\",\"max\",\"sum\"]}"
                    + "]"
                    + "}");

            Map<String, Object> createRollupJobResponse = entityAsMap(client().performRequest(createRollupJobRequest));
            assertThat(createRollupJobResponse.get("acknowledged"), equalTo(Boolean.TRUE));

            // start the rollup job
            final Request startRollupJobRequest = new Request("POST", getRollupEndpoint() + "/job/rollup-job-test/_start");
            Map<String, Object> startRollupJobResponse = entityAsMap(client().performRequest(startRollupJobRequest));
            assertThat(startRollupJobResponse.get("started"), equalTo(Boolean.TRUE));

            assertRollUpJob("rollup-job-test");

        } else {

            final Request clusterHealthRequest = new Request("GET", "/_cluster/health");
            clusterHealthRequest.addParameter("wait_for_status", "yellow");
            clusterHealthRequest.addParameter("wait_for_no_relocating_shards", "true");
            clusterHealthRequest.addParameter("wait_for_no_initializing_shards", "true");
            Map<String, Object> clusterHealthResponse = entityAsMap(client().performRequest(clusterHealthRequest));
            assertThat(clusterHealthResponse.get("timed_out"), equalTo(Boolean.FALSE));

            assertRollUpJob("rollup-job-test");
        }
    }

    private String loadWatch(String watch) throws IOException {
        return StreamsUtils.copyToStringFromClasspath("/org/elasticsearch/xpack/restart/" + watch);
    }

    private void assertOldTemplatesAreDeleted() throws IOException {
        Map<String, Object> templates = entityAsMap(client().performRequest(new Request("GET", "/_template")));
        assertThat(templates.keySet(), not(hasItems(is("watches"), startsWith("watch-history"), is("triggered_watches"))));
    }

    @SuppressWarnings("unchecked")
    private void assertWatchIndexContentsWork() throws Exception {
        // Fetch a basic watch
        Request getRequest = new Request("GET", "_watcher/watch/bwc_watch");
        getRequest.setOptions(
            expectWarnings(
                INDEX_ACTION_TYPES_DEPRECATION_MESSAGE
            )
        );

        Map<String, Object> bwcWatch = entityAsMap(client().performRequest(getRequest));

        logger.error("-----> {}", bwcWatch);

        assertThat(bwcWatch.get("found"), equalTo(true));
        Map<String, Object> source = (Map<String, Object>) bwcWatch.get("watch");
        assertEquals(1000, source.get("throttle_period_in_millis"));
        int timeout = (int) timeValueSeconds(100).millis();
        assertThat(ObjectPath.eval("input.search.timeout_in_millis", source), equalTo(timeout));
        assertThat(ObjectPath.eval("actions.index_payload.transform.search.timeout_in_millis", source), equalTo(timeout));
        assertThat(ObjectPath.eval("actions.index_payload.index.index", source), equalTo("bwc_watch_index"));
        assertThat(ObjectPath.eval("actions.index_payload.index.timeout_in_millis", source), equalTo(timeout));

        // Fetch a watch with "fun" throttle periods
        getRequest = new Request("GET", "_watcher/watch/bwc_throttle_period");
        getRequest.setOptions(
            expectWarnings(
                INDEX_ACTION_TYPES_DEPRECATION_MESSAGE
            )
        );

        bwcWatch = entityAsMap(client().performRequest(getRequest));
        assertThat(bwcWatch.get("found"), equalTo(true));
        source = (Map<String, Object>) bwcWatch.get("watch");
        assertEquals(timeout, source.get("throttle_period_in_millis"));
        assertThat(ObjectPath.eval("actions.index_payload.throttle_period_in_millis", source), equalTo(timeout));

        /*
         * Fetch a watch with a funny timeout to verify loading fractional time
         * values.
         */
        bwcWatch = entityAsMap(client().performRequest(new Request("GET", "_watcher/watch/bwc_funny_timeout")));
        assertThat(bwcWatch.get("found"), equalTo(true));
        source = (Map<String, Object>) bwcWatch.get("watch");


        Map<String, Object> attachments = ObjectPath.eval("actions.work.email.attachments", source);
        Map<String, Object> attachment = (Map<String, Object>) attachments.get("test_report.pdf");
        Map<String, Object>  request =  ObjectPath.eval("http.request", attachment);
        assertEquals(timeout, request.get("read_timeout_millis"));
        assertEquals("https", request.get("scheme"));
        assertEquals("example.com", request.get("host"));
        assertEquals("{{ctx.metadata.report_url}}", request.get("path"));
        assertEquals(8443, request.get("port"));
        Map<String, String> basic = ObjectPath.eval("auth.basic", request);
        assertThat(basic, hasEntry("username", "Aladdin"));
        // password doesn't come back because it is hidden
        assertThat(basic, hasEntry(is("password"), anyOf(startsWith("::es_encrypted::"), is("::es_redacted::"))));
        Request searchRequest = new Request("GET", ".watcher-history*/_search");
        if (isRunningAgainstOldCluster() == false) {
            searchRequest.addParameter(RestSearchAction.TOTAL_HITS_AS_INT_PARAM, "true");
        }
        Map<String, Object> history = entityAsMap(client().performRequest(searchRequest));
        Map<String, Object> hits = (Map<String, Object>) history.get("hits");
        assertThat((int) (hits.get("total")), greaterThanOrEqualTo(2));
    }

    private void assertBasicWatchInteractions() throws Exception {

        String watch = "{\"trigger\":{\"schedule\":{\"interval\":\"1s\"}},\"input\":{\"none\":{}}," +
            "\"condition\":{\"always\":{}}," +
            "\"actions\":{\"awesome\":{\"logging\":{\"level\":\"info\",\"text\":\"test\"}}}}";
        Request createWatchRequest = new Request("PUT", "_watcher/watch/new_watch");
        createWatchRequest.setJsonEntity(watch);
        Map<String, Object> createWatch = entityAsMap(client().performRequest(createWatchRequest));

        logger.info("create watch {}", createWatch);

        assertThat(createWatch.get("created"), equalTo(true));
        assertThat(createWatch.get("_version"), equalTo(1));

        Map<String, Object> updateWatch = entityAsMap(client().performRequest(createWatchRequest));
        assertThat(updateWatch.get("created"), equalTo(false));
        assertThat(updateWatch.get("_version"), equalTo(2));

        Map<String, Object> get = entityAsMap(client().performRequest(new Request("GET", "_watcher/watch/new_watch")));
        assertThat(get.get("found"), equalTo(true));
        @SuppressWarnings("unchecked") Map<?, ?> source = (Map<String, Object>) get.get("watch");
        Map<String, Object>  logging = ObjectPath.eval("actions.awesome.logging", source);
        assertEquals("info", logging.get("level"));
        assertEquals("test", logging.get("text"));
    }

    private void waitForYellow(String indexName) throws IOException {
        Request request = new Request("GET", "/_cluster/health/" + indexName);
        request.addParameter("wait_for_status", "yellow");
        request.addParameter("timeout", "30s");
        request.addParameter("wait_for_no_relocating_shards", "true");
        request.addParameter("wait_for_no_initializing_shards", "true");
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        assertThat(response.get("timed_out"), equalTo(Boolean.FALSE));
    }

    @SuppressWarnings("unchecked")
    private void waitForHits(String indexName, int expectedHits) throws Exception {
        Request request = new Request("GET", "/" + indexName + "/_search");
        request.addParameter("size", "0");
        assertBusy(() -> {
            try {
                Map<String, Object> response = entityAsMap(client().performRequest(request));
                Map<String, Object> hits = (Map<String, Object>) response.get("hits");
                logger.info("Hits are: {}", hits);
                int total;
                if (getOldClusterVersion().onOrAfter(Version.V_7_0_0) || isRunningAgainstOldCluster() == false) {
                    total = (int) ((Map<String, Object>) hits.get("total")).get("value");
                } else {
                    total = (int) hits.get("total");
                }
                assertThat(total, greaterThanOrEqualTo(expectedHits));
            } catch (IOException ioe) {
                if (ioe instanceof ResponseException) {
                    Response response = ((ResponseException) ioe).getResponse();
                    if (RestStatus.fromCode(response.getStatusLine().getStatusCode()) == RestStatus.SERVICE_UNAVAILABLE) {
                        fail("shards are not yet active");
                    }
                }
                throw ioe;
            }
        }, 30, TimeUnit.SECONDS);
    }

    static String toStr(Response response) throws IOException {
        return EntityUtils.toString(response.getEntity());
    }

    private void createUser(final boolean oldCluster) throws Exception {
        final String id = oldCluster ? "preupgrade_user" : "postupgrade_user";
        Request request = new Request("PUT", getSecurityEndpoint() + "/user/" + id);
        request.setJsonEntity(
            "{\n" +
            "   \"password\" : \"j@rV1s\",\n" +
            "   \"roles\" : [ \"admin\", \"other_role1\" ],\n" +
            "   \"full_name\" : \"" + randomAlphaOfLength(5) + "\",\n" +
            "   \"email\" : \"" + id + "@example.com\",\n" +
            "   \"enabled\": true\n" +
            "}");
        client().performRequest(request);
    }

    private void createRole(final boolean oldCluster) throws Exception {
        final String id = oldCluster ? "preupgrade_role" : "postupgrade_role";
        Request request = new Request("PUT", getSecurityEndpoint() + "/role/" + id);
        request.setJsonEntity(
            "{\n" +
            "  \"run_as\": [ \"abc\" ],\n" +
            "  \"cluster\": [ \"monitor\" ],\n" +
            "  \"indices\": [\n" +
            "    {\n" +
            "      \"names\": [ \"events-*\" ],\n" +
            "      \"privileges\": [ \"read\" ],\n" +
            "      \"field_security\" : {\n" +
            "        \"grant\" : [ \"category\", \"@timestamp\", \"message\" ]\n" +
            "      },\n" +
            "      \"query\": \"{\\\"match\\\": {\\\"category\\\": \\\"click\\\"}}\"\n" +
            "    }\n" +
            "  ]\n" +
            "}");
        client().performRequest(request);
    }

    private void assertUserInfo(final boolean oldCluster) throws Exception {
        final String user = oldCluster ? "preupgrade_user" : "postupgrade_user";
        Request request = new Request("GET", getSecurityEndpoint() + "/user/" + user);;
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        @SuppressWarnings("unchecked") Map<String, Object> userInfo = (Map<String, Object>) response.get(user);
        assertEquals(user + "@example.com", userInfo.get("email"));
        assertNotNull(userInfo.get("full_name"));
        assertNotNull(userInfo.get("roles"));
    }

    private String getSecurityEndpoint() {
        String securityEndpoint;
        if (getOldClusterVersion().onOrAfter(Version.V_7_0_0) || isRunningAgainstOldCluster() == false) {
            securityEndpoint = "/_security";
        } else {
            securityEndpoint = "/_xpack/security";
        }
        return securityEndpoint;
    }

    private String getSQLEndpoint() {
        String securityEndpoint;
        if (getOldClusterVersion().onOrAfter(Version.V_7_0_0) || isRunningAgainstOldCluster() == false) {
            securityEndpoint = "/_sql";
        } else {
            securityEndpoint = "/_xpack/sql";
        }
        return securityEndpoint;
    }

    private String getRollupEndpoint() {
        String securityEndpoint;
        if (getOldClusterVersion().onOrAfter(Version.V_7_0_0) || isRunningAgainstOldCluster() == false) {
            securityEndpoint = "/_rollup";
        } else {
            securityEndpoint = "/_xpack/rollup";
        }
        return securityEndpoint;
    }

    private String getWatcherEndpoint() {
        String securityEndpoint;
        if (getOldClusterVersion().onOrAfter(Version.V_7_0_0) || isRunningAgainstOldCluster() == false) {
            securityEndpoint = "/_watcher";
        } else {
            securityEndpoint = "/_xpack/watcher";
        }
        return securityEndpoint;
    }

    private void assertRoleInfo(final boolean oldCluster) throws Exception {
        final String role = oldCluster ? "preupgrade_role" : "postupgrade_role";
        @SuppressWarnings("unchecked") Map<String, Object> response = (Map<String, Object>) entityAsMap(
            client().performRequest(new Request("GET", getSecurityEndpoint() + "/role/" + role))
        ).get(role);
        assertNotNull(response.get("run_as"));
        assertNotNull(response.get("cluster"));
        assertNotNull(response.get("indices"));
    }

    @SuppressWarnings("unchecked")
    private void assertRollUpJob(final String rollupJob) throws Exception {
        final Matcher<?> expectedStates = anyOf(equalTo("indexing"), equalTo("started"));
        waitForRollUpJob(rollupJob, expectedStates);

        // check that the rollup job is started using the RollUp API
        final Request getRollupJobRequest = new Request("GET", getRollupEndpoint() + "/job/" + rollupJob);
        Map<String, Object> getRollupJobResponse = entityAsMap(client().performRequest(getRollupJobRequest));
        Map<String, Object> job = getJob(getRollupJobResponse, rollupJob);
        assertNotNull(job);
        assertThat(ObjectPath.eval("status.job_state", job), expectedStates);

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
                final String jobStateField = "task.xpack/rollup/job.state.job_state";
                assertThat("Expected field [" + jobStateField + "] to be started or indexing in " + task.get("id"),
                    ObjectPath.eval(jobStateField, task), expectedStates);
                break;
            }
        }
        if (hasRollupTask == false) {
            fail("Expected persistent task for [" + rollupJob + "] but none found.");
        }
    }

    private void waitForRollUpJob(final String rollupJob, final Matcher<?> expectedStates) throws Exception {
        assertBusy(() -> {
            final Request getRollupJobRequest = new Request("GET", getRollupEndpoint() + "/job/" + rollupJob);

            Response getRollupJobResponse = client().performRequest(getRollupJobRequest);
            assertThat(getRollupJobResponse.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));

            Map<String, Object> job = getJob(getRollupJobResponse, rollupJob);
            assertNotNull(job);
            assertThat(ObjectPath.eval("status.job_state", job), expectedStates);
        }, 30L, TimeUnit.SECONDS);
    }

    private Map<String, Object> getJob(Response response, String targetJobId) throws IOException {
        return getJob(ESRestTestCase.entityAsMap(response), targetJobId);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getJob(Map<String, Object> jobsMap, String targetJobId) throws IOException {

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
