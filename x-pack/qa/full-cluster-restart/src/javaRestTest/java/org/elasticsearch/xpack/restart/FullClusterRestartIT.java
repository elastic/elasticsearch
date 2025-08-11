/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.restart;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.document.RestGetAction;
import org.elasticsearch.rest.action.document.RestIndexAction;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.test.StreamsUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus;
import org.elasticsearch.xcontent.DeprecationHandler;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleStats;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.rest.action.search.RestSearchAction.TOTAL_HITS_AS_INT_PARAM;
import static org.elasticsearch.upgrades.FullClusterRestartIT.assertNumHits;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class FullClusterRestartIT extends AbstractXpackFullClusterRestartTestCase {

    public static final String INDEX_ACTION_TYPES_DEPRECATION_MESSAGE =
        "[types removal] Specifying types in a watcher index action is deprecated.";

    public static final String SEARCH_INPUT_TYPES_DEPRECATION_MESSAGE =
        "[types removal] Specifying types in a watcher search request is deprecated.";

    public static final int UPGRADE_FIELD_EXPECTED_INDEX_FORMAT_VERSION = 6;
    public static final int SECURITY_EXPECTED_INDEX_FORMAT_VERSION = 6;

    private String type;

    @Before
    public void setType() {
        type = getOldClusterVersion().before(Version.V_6_7_0) ? "doc" : "_doc";
    }

    public FullClusterRestartIT(@Name("cluster") FullClusterRestartUpgradeStatus upgradeStatus) {
        super(upgradeStatus);
    }

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
        String docLocation = "/testsingledoc/" + type + "/1";
        String doc = "{\"test\": \"test\"}";

        if (isRunningAgainstOldCluster()) {
            Request createDoc = new Request("PUT", docLocation);
            createDoc.addParameter("refresh", "true");
            createDoc.setJsonEntity(doc);
            client().performRequest(createDoc);
        }

        Request getRequest = new Request("GET", docLocation);
        if (getOldClusterVersion().before(Version.V_6_7_0)) {
            getRequest.setOptions(expectWarnings(RestGetAction.TYPES_DEPRECATION_MESSAGE));
        }
        assertThat(toStr(client().performRequest(getRequest)), containsString(doc));
    }

    public void testSecurityNativeRealm() throws Exception {
        if (isRunningAgainstOldCluster()) {
            createUser(true);
            createRole(true);
        } else {
            waitForYellow(".security");
            final Request getSettingsRequest = new Request("GET", "/.security/_settings/index.format");
            getSettingsRequest.setOptions(systemIndexWarningHandlerOptions());
            Response settingsResponse = client().performRequest(getSettingsRequest);
            Map<String, Object> settingsResponseMap = entityAsMap(settingsResponse);
            logger.info("settings response map {}", settingsResponseMap);
            final String concreteSecurityIndex;
            if (settingsResponseMap.isEmpty()) {
                fail("The security index does not have the expected setting [index.format]");
            } else {
                concreteSecurityIndex = settingsResponseMap.keySet().iterator().next();
                Map<?, ?> indexSettingsMap = (Map<?, ?>) settingsResponseMap.get(concreteSecurityIndex);
                Map<?, ?> settingsMap = (Map<?, ?>) indexSettingsMap.get("settings");
                logger.info("settings map {}", settingsMap);
                if (settingsMap.containsKey("index")) {
                    int format = Integer.parseInt(String.valueOf(((Map<?, ?>) settingsMap.get("index")).get("format")));
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

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/48381")
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
            try {
                waitForYellow(".watches,bwc_watch_index,.watcher-history*");
            } catch (ResponseException e) {
                String rsp = toStr(client().performRequest(new Request("GET", "/_cluster/state")));
                logger.info("cluster_state_response=\n{}", rsp);
                throw e;
            }
            waitForHits("bwc_watch_index", 2);
            waitForHits(".watcher-history*", 2);
            logger.info("Done creating watcher-related indices");
        } else {
            logger.info("testing against {}", getOldClusterVersion());
            try {
                waitForYellow(".watches,bwc_watch_index,.watcher-history*");
            } catch (ResponseException e) {
                String rsp = toStr(client().performRequest(new Request("GET", "/_cluster/state")));
                logger.info("cluster_state_response=\n{}", rsp);
                throw e;
            }

            logger.info("checking that the Watches index is the correct version");

            // Verify .watches index format:
            Map<?, ?> getClusterStateResponse = entityAsMap(
                client().performRequest(new Request("GET", "/_cluster/state/metadata/.watches"))
            );
            Map<?, ?> indices = ObjectPath.eval("metadata.indices", getClusterStateResponse);
            Map<?, ?> dotWatchesIndex = (Map<?, ?>) indices.get(".watches"); // ObjectPath.eval(...) doesn't handle keys containing .
            int indexFormat = Integer.parseInt(ObjectPath.eval("settings.index.format", dotWatchesIndex));
            assertEquals("The watches index needs to be upgraded", UPGRADE_FIELD_EXPECTED_INDEX_FORMAT_VERSION, indexFormat);

            // Wait for watcher to actually start....
            startWatcher();

            try {
                assertOldTemplatesAreDeleted();
                assertWatchIndexContentsWork();
                assertBasicWatchInteractions();
            } finally {
                /* Shut down watcher after every test because watcher can be a bit finicky about shutting down when the node shuts
                 * down. This makes super sure it shuts down *and* causes the test to fail in a sensible spot if it doesn't shut down.
                 */
                stopWatcher();
            }
        }
    }

    @SuppressWarnings("unchecked")
    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/84700")
    public void testWatcherWithApiKey() throws Exception {
        assumeTrue("API key is available since 6.7.0", getOldClusterVersion().onOrAfter(Version.V_6_7_0));
        if (isRunningAgainstOldCluster()) {
            final Request createApiKeyRequest = new Request("PUT", "/_security/api_key");
            createApiKeyRequest.setJsonEntity(
                "{\"name\":\"key-1\",\"role_descriptors\":"
                    + "{\"r\":{\"cluster\":[\"all\"],\"indices\":[{\"names\":[\"*\"],\"privileges\":[\"all\"]}]}}}"
            );
            final Response response = client().performRequest(createApiKeyRequest);
            final Map<String, Object> createApiKeyResponse = entityAsMap(response);

            Request createWatchWithApiKeyRequest = new Request("PUT", getWatcherEndpoint() + "/watch/watch_with_api_key");
            createWatchWithApiKeyRequest.setJsonEntity(loadWatch("logging-watch.json"));
            final byte[] keyBytes = (createApiKeyResponse.get("id") + ":" + createApiKeyResponse.get("api_key")).getBytes(
                StandardCharsets.UTF_8
            );
            final String authHeader = "ApiKey " + Base64.getEncoder().encodeToString(keyBytes);
            createWatchWithApiKeyRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authHeader));
            client().performRequest(createWatchWithApiKeyRequest);

            assertBusy(() -> {
                final Request getRequest = new Request("GET", getWatcherEndpoint() + "/watch/watch_with_api_key");
                final Map<String, Object> getWatchStatusResponse = entityAsMap(client().performRequest(getRequest));
                final Map<String, Object> status = (Map<String, Object>) getWatchStatusResponse.get("status");
                assertEquals("executed", status.get("execution_state"));
            }, 30, TimeUnit.SECONDS);

        } else {
            logger.info("testing against {}", getOldClusterVersion());
            try {
                waitForYellow(".watches,.watcher-history*");
            } catch (ResponseException e) {
                String rsp = toStr(client().performRequest(new Request("GET", "/_cluster/state")));
                logger.info("cluster_state_response=\n{}", rsp);
                throw e;
            }

            // Wait for watcher to actually start....
            startWatcher();

            try {
                final Request getWatchStatusRequest = new Request("GET", "_watcher/watch/watch_with_api_key");
                if (getOldClusterVersion().before(Version.V_7_0_0)) {
                    getWatchStatusRequest.setOptions(expectWarnings(SEARCH_INPUT_TYPES_DEPRECATION_MESSAGE));
                }
                final Map<String, Object> getWatchStatusResponse = entityAsMap(client().performRequest(getWatchStatusRequest));
                final Map<String, Object> status = (Map<String, Object>) getWatchStatusResponse.get("status");
                final int version = (int) status.get("version");

                final AtomicBoolean versionIncreased = new AtomicBoolean();
                final AtomicBoolean executed = new AtomicBoolean();
                assertBusy(() -> {
                    final Map<String, Object> newGetWatchStatusResponse = entityAsMap(client().performRequest(getWatchStatusRequest));
                    final Map<String, Object> newStatus = (Map<String, Object>) newGetWatchStatusResponse.get("status");
                    if (false == versionIncreased.get() && version < (int) newStatus.get("version")) {
                        versionIncreased.set(true);
                    }
                    if (false == executed.get() && "executed".equals(newStatus.get("execution_state"))) {
                        executed.set(true);
                    }
                    assertThat(
                        "version increased: [" + versionIncreased.get() + "], executed: [" + executed.get() + "]",
                        versionIncreased.get() && executed.get(),
                        is(true)
                    );
                }, 30, TimeUnit.SECONDS);
            } finally {
                stopWatcher();
            }
        }
    }

    public void testServiceAccountApiKey() throws IOException {
        assumeTrue("no service accounts in versions before " + Version.V_7_13_0, getOldClusterVersion().onOrAfter(Version.V_7_13_0));
        if (isRunningAgainstOldCluster()) {
            final Request createServiceTokenRequest = new Request("POST", "/_security/service/elastic/fleet-server/credential/token");
            final Response createServiceTokenResponse = client().performRequest(createServiceTokenRequest);
            assertOK(createServiceTokenResponse);
            @SuppressWarnings("unchecked")
            final String serviceToken = ((Map<String, String>) responseAsMap(createServiceTokenResponse).get("token")).get("value");
            final Request createApiKeyRequest = new Request("PUT", "/_security/api_key");
            createApiKeyRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "Bearer " + serviceToken));
            createApiKeyRequest.setJsonEntity("{\"name\":\"key-1\"}");
            final Response createApiKeyResponse = client().performRequest(createApiKeyRequest);
            final Map<String, Object> createApiKeyResponseMap = entityAsMap(createApiKeyResponse);
            final String authHeader = "ApiKey "
                + Base64.getEncoder()
                    .encodeToString(
                        (createApiKeyResponseMap.get("id") + ":" + createApiKeyResponseMap.get("api_key")).getBytes(StandardCharsets.UTF_8)
                    );

            final Request indexRequest = new Request("PUT", "/api_keys/_doc/key-1");
            indexRequest.setJsonEntity("{\"auth_header\":\"" + authHeader + "\"}");
            assertOK(client().performRequest(indexRequest));
        } else {
            final Request getRequest = new Request("GET", "/api_keys/_doc/key-1");
            final Response getResponse = client().performRequest(getRequest);
            assertOK(getResponse);
            final Map<String, Object> getResponseMap = responseAsMap(getResponse);
            @SuppressWarnings("unchecked")
            final String authHeader = ((Map<String, String>) getResponseMap.get("_source")).get("auth_header");

            final Request mainRequest = new Request("GET", "/");
            mainRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authHeader));
            assertOK(client().performRequest(mainRequest));

            final Request getUserRequest = new Request("GET", "/_security/user");
            getUserRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authHeader));
            final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(getUserRequest));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(e.getMessage(), containsString("is unauthorized"));
        }
    }

    /**
     * Tests that a RollUp job created on a old cluster is correctly restarted after the upgrade.
     */
    public void testRollupAfterRestart() throws Exception {
        assumeTrue("Rollup can be tested with 6.3.0 and onwards", getOldClusterVersion().onOrAfter(Version.V_6_3_0));
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

            createRollupJobRequest.setJsonEntity(
                "{"
                    + "\"index_pattern\":\"rollup-*\","
                    + "\"rollup_index\":\"results-rollup\","
                    + "\"cron\":\"*/30 * * * * ?\","
                    + "\"page_size\":100,"
                    + "\"groups\":{"
                    + "    \"date_histogram\":{"
                    + "        \"field\":\"timestamp\","
                    + "        \""
                    + intervalType
                    + "\":\"5m\""
                    + "      }"
                    + "},"
                    + "\"metrics\":["
                    + "    {\"field\":\"value\",\"metrics\":[\"min\",\"max\",\"sum\"]}"
                    + "]"
                    + "}"
            );

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
            if (getOldClusterVersion().onOrAfter(Version.V_6_2_0)) {
                clusterHealthRequest.addParameter("wait_for_no_initializing_shards", "true");
            }
            Map<String, Object> clusterHealthResponse = entityAsMap(client().performRequest(clusterHealthRequest));
            assertThat(clusterHealthResponse.get("timed_out"), equalTo(Boolean.FALSE));

            assertRollUpJob("rollup-job-test");
        }
    }

    public void testSlmStats() throws IOException {
        SnapshotLifecyclePolicy slmPolicy = new SnapshotLifecyclePolicy(
            "test-policy",
            "test-policy",
            "* * * 31 FEB ? *",
            "test-repo",
            Collections.singletonMap("indices", Collections.singletonList("*")),
            null
        );
        if (isRunningAgainstOldCluster() && getOldClusterVersion().onOrAfter(Version.V_7_4_0)) {
            Request createRepoRequest = new Request("PUT", "_snapshot/test-repo");
            String repoCreateJson = "{" + " \"type\": \"fs\"," + " \"settings\": {" + "   \"location\": \"test-repo\"" + "  }" + "}";
            createRepoRequest.setJsonEntity(repoCreateJson);
            Request createSlmPolicyRequest = new Request("PUT", "_slm/policy/test-policy");
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                String createSlmPolicyJson = Strings.toString(slmPolicy.toXContent(builder, null));
                createSlmPolicyRequest.setJsonEntity(createSlmPolicyJson);
            }

            client().performRequest(createRepoRequest);
            client().performRequest(createSlmPolicyRequest);
        }

        if (isRunningAgainstOldCluster() == false || getOldClusterVersion().onOrAfter(Version.V_7_5_0)) {
            Response response = client().performRequest(new Request("GET", "_slm/stats"));
            XContentType xContentType = XContentType.fromMediaTypeOrFormat(response.getEntity().getContentType().getValue());
            try (
                XContentParser parser = xContentType.xContent()
                    .createParser(
                        NamedXContentRegistry.EMPTY,
                        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        response.getEntity().getContent()
                    )
            ) {
                assertEquals(new SnapshotLifecycleStats(), SnapshotLifecycleStats.parse(parser));
            }
        }

    }

    public void testRollupIDSchemeAfterRestart() throws Exception {
        assumeTrue("Rollup can be tested with 6.3.0 and onwards", getOldClusterVersion().onOrAfter(Version.V_6_3_0));
        assumeTrue("Rollup ID scheme changed in 6.4", getOldClusterVersion().before(Version.V_6_4_0));
        if (isRunningAgainstOldCluster()) {

            final Request indexRequest = new Request("POST", "/id-test-rollup/" + type + "/1");
            indexRequest.setJsonEntity("{\"timestamp\":\"2018-01-01T00:00:01\",\"value\":123}");
            client().performRequest(indexRequest);

            // create the rollup job
            final Request createRollupJobRequest = new Request("PUT", getRollupEndpoint() + "/job/rollup-id-test");
            createRollupJobRequest.setJsonEntity(
                "{"
                    + "\"index_pattern\":\"id-test-rollup\","
                    + "\"rollup_index\":\"id-test-results-rollup\","
                    + "\"cron\":\"*/1 * * * * ?\","
                    + "\"page_size\":100,"
                    + "\"groups\":{"
                    + "    \"date_histogram\":{"
                    + "        \"field\":\"timestamp\","
                    + "        \"interval\":\"5m\""
                    + "      },"
                    + "\"histogram\":{"
                    + "        \"fields\": [\"value\"],"
                    + "        \"interval\":1"
                    + "      },"
                    + "\"terms\":{"
                    + "        \"fields\": [\"value\"]"
                    + "      }"
                    + "},"
                    + "\"metrics\":["
                    + "    {\"field\":\"value\",\"metrics\":[\"min\",\"max\",\"sum\"]}"
                    + "]"
                    + "}"
            );

            Map<String, Object> createRollupJobResponse = entityAsMap(client().performRequest(createRollupJobRequest));
            assertThat(createRollupJobResponse.get("acknowledged"), equalTo(Boolean.TRUE));

            // start the rollup job
            final Request startRollupJobRequest = new Request("POST", getRollupEndpoint() + "/job/rollup-id-test/_start");
            Map<String, Object> startRollupJobResponse = entityAsMap(client().performRequest(startRollupJobRequest));
            assertThat(startRollupJobResponse.get("started"), equalTo(Boolean.TRUE));

            assertRollUpJob("rollup-id-test");

            assertBusy(() -> {
                client().performRequest(new Request("POST", "id-test-results-rollup/_refresh"));
                final Request searchRequest = new Request("GET", "id-test-results-rollup/_search");
                if (isRunningAgainstOldCluster() == false) {
                    searchRequest.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
                }
                try {
                    Map<String, Object> searchResponse = entityAsMap(client().performRequest(searchRequest));
                    assertNotNull(ObjectPath.eval("hits.total", searchResponse));
                    assertThat(ObjectPath.eval("hits.total", searchResponse), equalTo(1));
                    assertThat(ObjectPath.eval("hits.hits.0._id", searchResponse), equalTo("3310683722"));
                } catch (IOException e) {
                    fail();
                }
            });

            // After we've confirmed the doc, wait until we move back to STARTED so that we know the
            // state was saved at the end
            waitForRollUpJob("rollup-id-test", equalTo("started"));

        } else {

            final Request indexRequest = new Request("POST", "/id-test-rollup/" + type + "/2");
            indexRequest.setJsonEntity("{\"timestamp\":\"2018-01-02T00:00:01\",\"value\":345}");
            if (getOldClusterVersion().before(Version.V_6_7_0)) {
                indexRequest.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
            }
            client().performRequest(indexRequest);

            assertRollUpJob("rollup-id-test");

            // stop the rollup job to force a state save, which will upgrade the ID
            final Request stopRollupJobRequest = new Request("POST", "/_rollup/job/rollup-id-test/_stop");
            Map<String, Object> stopRollupJobResponse = entityAsMap(client().performRequest(stopRollupJobRequest));
            assertThat(stopRollupJobResponse.get("stopped"), equalTo(Boolean.TRUE));

            waitForRollUpJob("rollup-id-test", equalTo("stopped"));

            // start the rollup job again
            final Request startRollupJobRequest = new Request("POST", "/_rollup/job/rollup-id-test/_start");
            Map<String, Object> startRollupJobResponse = entityAsMap(client().performRequest(startRollupJobRequest));
            assertThat(startRollupJobResponse.get("started"), equalTo(Boolean.TRUE));

            waitForRollUpJob("rollup-id-test", anyOf(equalTo("indexing"), equalTo("started")));

            assertBusy(() -> {
                client().performRequest(new Request("POST", "id-test-results-rollup/_refresh"));
                final Request searchRequest = new Request("GET", "id-test-results-rollup/_search");
                if (isRunningAgainstOldCluster() == false) {
                    searchRequest.addParameter(TOTAL_HITS_AS_INT_PARAM, "true");
                }
                try {
                    Map<String, Object> searchResponse = entityAsMap(client().performRequest(searchRequest));
                    assertNotNull(ObjectPath.eval("hits.total", searchResponse));
                    assertThat(ObjectPath.eval("hits.total", searchResponse), equalTo(2));
                    List<String> ids = new ArrayList<>(2);
                    ids.add(ObjectPath.eval("hits.hits.0._id", searchResponse));
                    ids.add(ObjectPath.eval("hits.hits.1._id", searchResponse));

                    // should have both old and new ID formats
                    assertThat(ids, containsInAnyOrder("3310683722", "rollup-id-test$ehY4NAyVSy8xxUDZrNXXIA"));

                    List<Double> values = new ArrayList<>(2);
                    Map<String, Object> doc = ObjectPath.eval("hits.hits.0._source", searchResponse);
                    values.add((Double) doc.get("value.min.value"));
                    doc = ObjectPath.eval("hits.hits.1._source", searchResponse);
                    values.add((Double) doc.get("value.min.value"));

                    assertThat(values, containsInAnyOrder(123.0, 345.0));
                } catch (IOException e) {
                    fail();
                }
            });
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/34774")
    public void testSqlFailsOnIndexWithTwoTypes() throws IOException {
        // TODO this isn't going to trigger until we backport to 6.1
        assumeTrue(
            "It is only possible to build an index that sql doesn't like before 6.0.0",
            getOldClusterVersion().before(Version.V_6_0_0_alpha1)
        );
        if (isRunningAgainstOldCluster()) {
            Request doc1 = new Request("POST", "/testsqlfailsonindexwithtwotypes/type1");
            doc1.setJsonEntity("{}");
            client().performRequest(doc1);
            Request doc2 = new Request("POST", "/testsqlfailsonindexwithtwotypes/type2");
            doc2.setJsonEntity("{}");
            client().performRequest(doc2);
            return;
        }
        final Request sqlRequest = new Request("POST", getSQLEndpoint());

        sqlRequest.setJsonEntity("{\"query\":\"SELECT * FROM testsqlfailsonindexwithtwotypes\"}");
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(sqlRequest));
        assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
        assertThat(
            e.getMessage(),
            containsString("[testsqlfailsonindexwithtwotypes] contains more than one type [type1, type2] so it is incompatible with sql")
        );
    }

    private String loadWatch(String watch) throws IOException {
        return StreamsUtils.copyToStringFromClasspath("/org/elasticsearch/xpack/restart/" + watch);
    }

    private void assertOldTemplatesAreDeleted() throws IOException {
        Map<String, Object> templates = entityAsMap(client().performRequest(new Request("GET", "/_template")));
        assertThat(templates.keySet(), not(hasItems(is("watches"), startsWith("watch-history"), is("triggered_watches"))));
    }

    private void assertWatchIndexContentsWork() throws Exception {
        // Fetch a basic watch
        Request getRequest = new Request("GET", "_watcher/watch/bwc_watch");
        if (getOldClusterVersion().before(Version.V_7_0_0)) {
            getRequest.setOptions(expectWarnings(INDEX_ACTION_TYPES_DEPRECATION_MESSAGE, SEARCH_INPUT_TYPES_DEPRECATION_MESSAGE));
        } else {
            getRequest.setOptions(expectWarnings(INDEX_ACTION_TYPES_DEPRECATION_MESSAGE));
        }

        Map<String, Object> bwcWatch = entityAsMap(client().performRequest(getRequest));

        logger.error("-----> {}", bwcWatch);

        assertThat(bwcWatch.get("found"), equalTo(true));
        Map<?, ?> source = (Map<?, ?>) bwcWatch.get("watch");
        assertEquals(1000, source.get("throttle_period_in_millis"));
        int timeout = (int) timeValueSeconds(100).millis();
        assertThat(ObjectPath.eval("input.search.timeout_in_millis", source), equalTo(timeout));
        assertThat(ObjectPath.eval("actions.index_payload.transform.search.timeout_in_millis", source), equalTo(timeout));
        assertThat(ObjectPath.eval("actions.index_payload.index.index", source), equalTo("bwc_watch_index"));
        assertThat(ObjectPath.eval("actions.index_payload.index.timeout_in_millis", source), equalTo(timeout));

        // Fetch a watch with "fun" throttle periods
        getRequest = new Request("GET", "_watcher/watch/bwc_throttle_period");
        if (getOldClusterVersion().before(Version.V_7_0_0)) {
            getRequest.setOptions(expectWarnings(INDEX_ACTION_TYPES_DEPRECATION_MESSAGE, SEARCH_INPUT_TYPES_DEPRECATION_MESSAGE));
        } else {
            getRequest.setOptions(expectWarnings(INDEX_ACTION_TYPES_DEPRECATION_MESSAGE));
        }
        bwcWatch = entityAsMap(client().performRequest(getRequest));
        assertThat(bwcWatch.get("found"), equalTo(true));
        source = (Map<?, ?>) bwcWatch.get("watch");
        assertEquals(timeout, source.get("throttle_period_in_millis"));
        assertThat(ObjectPath.eval("actions.index_payload.throttle_period_in_millis", source), equalTo(timeout));

        /*
         * Fetch a watch with a funny timeout to verify loading fractional time
         * values.
         */
        bwcWatch = entityAsMap(client().performRequest(new Request("GET", "_watcher/watch/bwc_funny_timeout")));
        assertThat(bwcWatch.get("found"), equalTo(true));
        source = (Map<?, ?>) bwcWatch.get("watch");

        Map<String, Object> attachments = ObjectPath.eval("actions.work.email.attachments", source);
        Map<?, ?> attachment = (Map<?, ?>) attachments.get("test_report.pdf");
        Map<String, Object> request = ObjectPath.eval("http.request", attachment);
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
        Map<?, ?> hits = (Map<?, ?>) history.get("hits");
        assertThat((Integer) (hits.get("total")), greaterThanOrEqualTo(2));
    }

    private void assertBasicWatchInteractions() throws Exception {

        String watch = "{\"trigger\":{\"schedule\":{\"interval\":\"1s\"}},\"input\":{\"none\":{}},"
            + "\"condition\":{\"always\":{}},"
            + "\"actions\":{\"awesome\":{\"logging\":{\"level\":\"info\",\"text\":\"test\"}}}}";
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
        Map<?, ?> source = (Map<?, ?>) get.get("watch");
        Map<String, Object> logging = ObjectPath.eval("actions.awesome.logging", source);
        assertEquals("info", logging.get("level"));
        assertEquals("test", logging.get("text"));
    }

    private void waitForYellow(String indexName) throws IOException {
        Request request = new Request("GET", "/_cluster/health/" + indexName);
        request.addParameter("wait_for_status", "yellow");
        request.addParameter("timeout", "30s");
        request.addParameter("wait_for_no_relocating_shards", "true");
        if (getOldClusterVersion().onOrAfter(Version.V_6_2_0)) {
            request.addParameter("wait_for_no_initializing_shards", "true");
        }
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        assertThat(response.get("timed_out"), equalTo(Boolean.FALSE));
    }

    private void waitForHits(String indexName, int expectedHits) throws Exception {
        Request request = new Request("GET", "/" + indexName + "/_search");
        request.addParameter("size", "0");
        assertBusy(() -> {
            try {
                Map<String, Object> response = entityAsMap(client().performRequest(request));
                Map<?, ?> hits = (Map<?, ?>) response.get("hits");
                logger.info("Hits are: {}", hits);
                Integer total;
                if (getOldClusterVersion().onOrAfter(Version.V_7_0_0) || isRunningAgainstOldCluster() == false) {
                    total = (Integer) ((Map<?, ?>) hits.get("total")).get("value");
                } else {
                    total = (Integer) hits.get("total");
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

    private void startWatcher() throws Exception {
        Map<String, Object> startWatchResponse = entityAsMap(client().performRequest(new Request("POST", "_watcher/_start")));
        assertThat(startWatchResponse.get("acknowledged"), equalTo(Boolean.TRUE));
        assertBusy(() -> {
            Map<String, Object> statsWatchResponse = entityAsMap(client().performRequest(new Request("GET", "_watcher/stats")));
            List<?> states = ((List<?>) statsWatchResponse.get("stats")).stream()
                .map(o -> ((Map<?, ?>) o).get("watcher_state"))
                .collect(Collectors.toList());
            assertThat(states, everyItem(is("started")));
        });
    }

    private void stopWatcher() throws Exception {
        Map<String, Object> stopWatchResponse = entityAsMap(client().performRequest(new Request("POST", "_watcher/_stop")));
        assertThat(stopWatchResponse.get("acknowledged"), equalTo(Boolean.TRUE));
        assertBusy(() -> {
            Map<String, Object> statsStoppedWatchResponse = entityAsMap(client().performRequest(new Request("GET", "_watcher/stats")));
            List<?> states = ((List<?>) statsStoppedWatchResponse.get("stats")).stream()
                .map(o -> ((Map<?, ?>) o).get("watcher_state"))
                .collect(Collectors.toList());
            assertThat(states, everyItem(is("stopped")));
        });
    }

    static String toStr(Response response) throws IOException {
        return EntityUtils.toString(response.getEntity());
    }

    private void createUser(final boolean oldCluster) throws Exception {
        final String id = oldCluster ? "preupgrade_user" : "postupgrade_user";
        Request request = new Request("PUT", getSecurityEndpoint() + "/user/" + id);
        request.setJsonEntity(
            "{\n"
                + "   \"password\" : \"l0ng-r4nd0m-p@ssw0rd\",\n"
                + "   \"roles\" : [ \"admin\", \"other_role1\" ],\n"
                + "   \"full_name\" : \""
                + randomAlphaOfLength(5)
                + "\",\n"
                + "   \"email\" : \""
                + id
                + "@example.com\",\n"
                + "   \"enabled\": true\n"
                + "}"
        );
        client().performRequest(request);
    }

    private void createRole(final boolean oldCluster) throws Exception {
        final String id = oldCluster ? "preupgrade_role" : "postupgrade_role";
        Request request = new Request("PUT", getSecurityEndpoint() + "/role/" + id);
        request.setJsonEntity(
            "{\n"
                + "  \"run_as\": [ \"abc\" ],\n"
                + "  \"cluster\": [ \"monitor\" ],\n"
                + "  \"indices\": [\n"
                + "    {\n"
                + "      \"names\": [ \"events-*\" ],\n"
                + "      \"privileges\": [ \"read\" ],\n"
                + "      \"field_security\" : {\n"
                + "        \"grant\" : [ \"category\", \"@timestamp\", \"message\" ]\n"
                + "      },\n"
                + "      \"query\": \"{\\\"match\\\": {\\\"category\\\": \\\"click\\\"}}\"\n"
                + "    }\n"
                + "  ]\n"
                + "}"
        );
        client().performRequest(request);
    }

    private void assertUserInfo(final boolean oldCluster) throws Exception {
        final String user = oldCluster ? "preupgrade_user" : "postupgrade_user";
        Request request = new Request("GET", getSecurityEndpoint() + "/user/" + user);
        ;
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        Map<?, ?> userInfo = (Map<?, ?>) response.get(user);
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
        Map<?, ?> response = (Map<?, ?>) entityAsMap(client().performRequest(new Request("GET", getSecurityEndpoint() + "/role/" + role)))
            .get(role);
        assertNotNull(response.get("run_as"));
        assertNotNull(response.get("cluster"));
        assertNotNull(response.get("indices"));
    }

    private void assertRollUpJob(final String rollupJob) throws Exception {
        final Matcher<?> expectedStates = anyOf(equalTo("indexing"), equalTo("started"));
        waitForRollUpJob(rollupJob, expectedStates);

        // check that the rollup job is started using the RollUp API
        final Request getRollupJobRequest = new Request("GET", getRollupEndpoint() + "/job/" + rollupJob);
        Map<String, Object> getRollupJobResponse = entityAsMap(client().performRequest(getRollupJobRequest));
        Map<?, ?> job = getJob(getRollupJobResponse, rollupJob);
        assertNotNull(job);
        assertThat(ObjectPath.eval("status.job_state", job), expectedStates);

        // check that the rollup job is started using the Tasks API
        final Request taskRequest = new Request("GET", "_tasks");
        taskRequest.addParameter("detailed", "true");
        taskRequest.addParameter("actions", "xpack/rollup/*");
        Map<String, Object> taskResponse = entityAsMap(client().performRequest(taskRequest));
        Map<?, ?> taskResponseNodes = (Map<?, ?>) taskResponse.get("nodes");
        Map<?, ?> taskResponseNode = (Map<?, ?>) taskResponseNodes.values().iterator().next();
        Map<?, ?> taskResponseTasks = (Map<?, ?>) taskResponseNode.get("tasks");
        Map<?, ?> taskResponseStatus = (Map<?, ?>) taskResponseTasks.values().iterator().next();
        assertThat(ObjectPath.eval("status.job_state", taskResponseStatus), expectedStates);

        // check that the rollup job is started using the Cluster State API
        final Request clusterStateRequest = new Request("GET", "_cluster/state/metadata");
        Map<String, Object> clusterStateResponse = entityAsMap(client().performRequest(clusterStateRequest));
        List<Map<String, Object>> rollupJobTasks = ObjectPath.eval("metadata.persistent_tasks.tasks", clusterStateResponse);

        boolean hasRollupTask = false;
        for (Map<String, Object> task : rollupJobTasks) {
            if (ObjectPath.eval("id", task).equals(rollupJob)) {
                hasRollupTask = true;

                // Persistent task state field has been renamed in 6.4.0 from "status" to "state"
                final String stateFieldName = (isRunningAgainstOldCluster() && getOldClusterVersion().before(Version.V_6_4_0))
                    ? "status"
                    : "state";

                final String jobStateField = "task.xpack/rollup/job." + stateFieldName + ".job_state";
                assertThat(
                    "Expected field [" + jobStateField + "] to be started or indexing in " + task.get("id"),
                    ObjectPath.eval(jobStateField, task),
                    expectedStates
                );
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

            Map<?, ?> job = getJob(getRollupJobResponse, rollupJob);
            assertNotNull(job);
            assertThat(ObjectPath.eval("status.job_state", job), expectedStates);
        }, 30L, TimeUnit.SECONDS);
    }

    private Map<?, ?> getJob(Response response, String targetJobId) throws IOException {
        return getJob(ESRestTestCase.entityAsMap(response), targetJobId);
    }

    private Map<?, ?> getJob(Map<String, Object> jobsMap, String targetJobId) throws IOException {

        List<?> jobs = (List<?>) XContentMapValues.extractValue("jobs", jobsMap);

        if (jobs == null) {
            return null;
        }

        for (Object entry : jobs) {
            Map<?, ?> job = (Map<?, ?>) entry;
            String jobId = (String) ((Map<?, ?>) job.get("config")).get("id");
            if (jobId.equals(targetJobId)) {
                return job;
            }
        }
        return null;
    }

    public void testFrozenIndexAfterRestarted() throws Exception {
        final String index = "test_frozen_index";
        if (isRunningAgainstOldCluster()) {
            Settings.Builder settings = Settings.builder();
            if (minimumNodeVersion().onOrAfter(Version.V_6_5_0) && randomBoolean()) {
                settings.put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), randomBoolean());
            }
            createIndex(index, settings.build());
            ensureGreen(index);
            int numDocs = randomIntBetween(10, 500);
            for (int i = 0; i < numDocs; i++) {
                int id = randomIntBetween(0, 100);
                final Request indexRequest = new Request("POST", "/" + index + "/" + type + "/" + id);
                indexRequest.setJsonEntity(Strings.toString(JsonXContent.contentBuilder().startObject().field("f", "v").endObject()));
                assertOK(client().performRequest(indexRequest));
                if (rarely()) {
                    flush(index, randomBoolean());
                }
            }
        } else {
            ensureGreen(index);
            final int totalHits = (int) XContentMapValues.extractValue(
                "hits.total.value",
                entityAsMap(client().performRequest(new Request("GET", "/" + index + "/_search")))
            );
            Request freezeRequest = new Request("POST", index + "/_freeze");
            freezeRequest.setOptions(
                expectWarnings(
                    "Frozen indices are deprecated because they provide no benefit given "
                        + "improvements in heap memory utilization. They will be removed in a future release."
                )
            );
            assertOK(client().performRequest(freezeRequest));
            ensureGreen(index);
            assertNoFileBasedRecovery(index, n -> true);
            final Request request = new Request("GET", "/" + index + "/_search");
            request.setOptions(
                expectWarnings(
                    "[ignore_throttled] parameter is deprecated because frozen "
                        + "indices have been deprecated. Consider cold or frozen tiers in place of frozen indices.",
                    "Searching frozen indices ["
                        + index
                        + "] is deprecated. Consider cold or frozen tiers in place of frozen indices. "
                        + "The frozen feature will be removed in a feature release."
                )
            );
            request.addParameter("ignore_throttled", "false");
            assertThat(
                XContentMapValues.extractValue("hits.total.value", entityAsMap(client().performRequest(request))),
                equalTo(totalHits)
            );
            final Request unfreezeRequest = new Request("POST", index + "/_unfreeze");
            unfreezeRequest.setOptions(
                expectWarnings(
                    "Frozen indices are deprecated because they provide no benefit given "
                        + "improvements in heap memory utilization. They will be removed in a future release."
                )
            );
            assertOK(client().performRequest(unfreezeRequest));
            ensureGreen(index);
            assertNoFileBasedRecovery(index, n -> true);
        }
    }

    @SuppressWarnings("unchecked")
    public void testDataStreams() throws Exception {
        assumeTrue("no data streams in versions before " + Version.V_7_9_0, getOldClusterVersion().onOrAfter(Version.V_7_9_0));
        if (isRunningAgainstOldCluster()) {
            createComposableTemplate(client(), "dst", "ds");

            Request indexRequest = new Request("POST", "/ds/_doc/1?op_type=create&refresh");
            XContentBuilder builder = JsonXContent.contentBuilder()
                .startObject()
                .field("f", "v")
                .field("@timestamp", System.currentTimeMillis())
                .endObject();
            indexRequest.setJsonEntity(Strings.toString(builder));
            assertOK(client().performRequest(indexRequest));
        }

        // It's quite possible that this test will run where the data stream backing index is
        // created on one day, and then checked on a subsequent day. To avoid this failing the
        // test, we store the timestamp used when the document is indexed, then when we go to
        // check the backing index name, we retrieve the time and use it for the backing index
        // name resolution.
        Request getDoc = new Request("GET", "/ds/_search");
        Map<String, Object> doc = entityAsMap(client().performRequest(getDoc));
        logger.info("--> doc: {}", doc);
        Map<String, Object> hits = (Map<String, Object>) doc.get("hits");
        Map<String, Object> docBody = (Map<String, Object>) ((List<Object>) hits.get("hits")).get(0);
        Long timestamp = (Long) ((Map<String, Object>) docBody.get("_source")).get("@timestamp");
        logger.info("--> parsed out timestamp of {}", timestamp);

        Request getDataStream = new Request("GET", "/_data_stream/ds");
        Response response = client().performRequest(getDataStream);
        assertOK(response);
        List<Object> dataStreams = (List<Object>) entityAsMap(response).get("data_streams");
        assertEquals(1, dataStreams.size());
        Map<String, Object> ds = (Map<String, Object>) dataStreams.get(0);
        List<Map<String, String>> indices = (List<Map<String, String>>) ds.get("indices");
        assertEquals("ds", ds.get("name"));
        assertEquals(1, indices.size());
        assertEquals(DataStream.getDefaultBackingIndexName("ds", 1, timestamp, getOldClusterVersion()), indices.get(0).get("index_name"));
        assertNumHits(isRunningAgainstOldCluster(), "ds", 1, 1);
    }

    private static void createComposableTemplate(RestClient client, String templateName, String indexPattern) throws IOException {
        StringEntity templateJSON = new StringEntity(
            String.format(Locale.ROOT, "{\n" + "  \"index_patterns\": \"%s\",\n" + "  \"data_stream\": {}\n" + "}", indexPattern),
            ContentType.APPLICATION_JSON
        );
        Request createIndexTemplateRequest = new Request("PUT", "_index_template/" + templateName);
        createIndexTemplateRequest.setEntity(templateJSON);
        client.performRequest(createIndexTemplateRequest);
    }

    private RequestOptions.Builder systemIndexWarningHandlerOptions() {
        return RequestOptions.DEFAULT.toBuilder()
            .setWarningsHandler(w -> w.size() > 0 && w.get(0).contains("this request accesses system indices") == false);
    }
}
