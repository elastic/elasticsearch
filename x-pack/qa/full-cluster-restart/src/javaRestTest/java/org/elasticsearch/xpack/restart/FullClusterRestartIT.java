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
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.test.StreamsUtils;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.RestTestLegacyFeatures;
import org.elasticsearch.upgrades.FullClusterRestartUpgradeStatus;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;
import org.elasticsearch.xpack.core.slm.SnapshotLifecycleStats;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.elasticsearch.upgrades.FullClusterRestartIT.assertNumHits;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;

public class FullClusterRestartIT extends AbstractXpackFullClusterRestartTestCase {

    public static final int UPGRADE_FIELD_EXPECTED_INDEX_FORMAT_VERSION = 6;
    public static final int SECURITY_EXPECTED_INDEX_FORMAT_VERSION = 6;

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
        String docLocation = "/testsingledoc/_doc/1";
        String doc = "{\"test\": \"test\"}";

        if (isRunningAgainstOldCluster()) {
            Request createDoc = new Request("PUT", docLocation);
            createDoc.addParameter("refresh", "true");
            createDoc.setJsonEntity(doc);
            createDoc.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(fieldNamesFieldOk()));
            client().performRequest(createDoc);
        }

        Request getRequest = new Request("GET", docLocation);
        assertThat(toStr(client().performRequest(getRequest)), containsString(doc));
    }

    public void testSecurityNativeRealm() throws Exception {
        if (isRunningAgainstOldCluster()) {
            createUser(true);
            createRole(true);
        } else {
            waitForYellow(".security");
            final Request getSettingsRequest = new Request("GET", "/.security/_settings/index.format");
            getSettingsRequest.setOptions(systemIndexWarningHandlerOptions(".security-7"));
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

    public void testWatcher() throws Exception {
        if (isRunningAgainstOldCluster()) {
            logger.info("Adding a watch on old cluster {}", getOldClusterVersion());
            Request createBwcWatch = new Request("PUT", "/_watcher/watch/bwc_watch");
            createBwcWatch.setJsonEntity(loadWatch("simple-watch.json"));
            client().performRequest(createBwcWatch);

            logger.info("Adding a watch with \"fun\" throttle periods on old cluster");
            Request createBwcThrottlePeriod = new Request("PUT", "/_watcher/watch/bwc_throttle_period");
            createBwcThrottlePeriod.setJsonEntity(loadWatch("throttle-period-watch.json"));
            client().performRequest(createBwcThrottlePeriod);

            logger.info("Adding a watch with \"fun\" read timeout on old cluster");
            Request createFunnyTimeout = new Request("PUT", "/_watcher/watch/bwc_funny_timeout");
            createFunnyTimeout.setJsonEntity(loadWatch("funny-timeout-watch.json"));
            client().performRequest(createFunnyTimeout);

            logger.info("Waiting for watch results index to fill up...");
            try {
                waitForYellow(".watches,bwc_watch_index,.watcher-history*");
            } catch (ResponseException e) {
                {
                    String rsp = toStr(client().performRequest(new Request("GET", "/_cluster/state")));
                    logger.info("cluster_state_response=\n{}", rsp);
                }
                {
                    Request request = new Request("GET", "/_watcher/stats/_all");
                    request.addParameter("emit_stacktraces", "true");
                    String rsp = toStr(client().performRequest(request));
                    logger.info("watcher_stats_response=\n{}", rsp);
                }
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
            var getClusterStateResponse = entityAsMap(client().performRequest(new Request("GET", "/_cluster/state/metadata/.watches")));
            Map<?, ?> indices = ObjectPath.eval("metadata.indices", getClusterStateResponse);
            var dotWatchesIndex = indices.get(".watches"); // ObjectPath.eval(...) doesn't handle keys containing .
            var indexFormat = Integer.parseInt(ObjectPath.eval("settings.index.format", dotWatchesIndex));
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
        final Request getWatchStatusRequest = new Request("GET", "/_watcher/watch/watch_with_api_key");

        if (isRunningAgainstOldCluster()) {
            final Request createApiKeyRequest = new Request("PUT", "/_security/api_key");
            createApiKeyRequest.setJsonEntity("""
                {
                   "name": "key-1",
                   "role_descriptors": {
                     "r": {
                       "cluster": [ "all" ],
                       "indices": [
                         {
                           "names": [ "*" ],
                           "privileges": [ "all" ]
                         }
                       ]
                     }
                   }
                 }""");
            final Response response = client().performRequest(createApiKeyRequest);
            final Map<String, Object> createApiKeyResponse = entityAsMap(response);

            Request createWatchWithApiKeyRequest = new Request("PUT", "/_watcher/watch/watch_with_api_key");
            createWatchWithApiKeyRequest.setJsonEntity(loadWatch("logging-watch.json"));
            final byte[] keyBytes = (createApiKeyResponse.get("id") + ":" + createApiKeyResponse.get("api_key")).getBytes(
                StandardCharsets.UTF_8
            );
            final String authHeader = "ApiKey " + Base64.getEncoder().encodeToString(keyBytes);
            createWatchWithApiKeyRequest.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authHeader));
            client().performRequest(createWatchWithApiKeyRequest);

            assertBusy(() -> {
                final Map<String, Object> getWatchStatusResponse = entityAsMap(client().performRequest(getWatchStatusRequest));
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
        @UpdateForV9(owner = UpdateForV9.Owner.SECURITY)
        var originalClusterSupportsServiceAccounts = oldClusterHasFeature(RestTestLegacyFeatures.SERVICE_ACCOUNTS_SUPPORTED);
        assumeTrue("no service accounts in versions before 7.13", originalClusterSupportsServiceAccounts);

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

    public void testApiKeySuperuser() throws IOException {
        if (isRunningAgainstOldCluster()) {
            final Request createUserRequest = new Request("PUT", "/_security/user/api_key_super_creator");
            createUserRequest.setJsonEntity("""
                {
                   "password" : "l0ng-r4nd0m-p@ssw0rd",
                   "roles" : [ "superuser", "monitoring_user" ]
                }""");
            client().performRequest(createUserRequest);

            // Create API key
            final Request createApiKeyRequest = new Request("PUT", "/_security/api_key");
            createApiKeyRequest.setOptions(
                RequestOptions.DEFAULT.toBuilder()
                    .addHeader(
                        "Authorization",
                        UsernamePasswordToken.basicAuthHeaderValue(
                            "api_key_super_creator",
                            new SecureString("l0ng-r4nd0m-p@ssw0rd".toCharArray())
                        )
                    )
            );
            createApiKeyRequest.setJsonEntity("""
                {
                   "name": "super_legacy_key"
                }""");
            final Map<String, Object> createApiKeyResponse = entityAsMap(client().performRequest(createApiKeyRequest));
            final byte[] keyBytes = (createApiKeyResponse.get("id") + ":" + createApiKeyResponse.get("api_key")).getBytes(
                StandardCharsets.UTF_8
            );
            final String apiKeyAuthHeader = "ApiKey " + Base64.getEncoder().encodeToString(keyBytes);
            // Save the API key info across restart
            final Request saveApiKeyRequest = new Request("PUT", "/api_keys/_doc/super_legacy_key");
            saveApiKeyRequest.setJsonEntity("{\"auth_header\":\"" + apiKeyAuthHeader + "\"}");
            assertOK(client().performRequest(saveApiKeyRequest));
        } else {
            final Request getRequest = new Request("GET", "/api_keys/_doc/super_legacy_key");
            final Map<String, Object> getResponseMap = responseAsMap(client().performRequest(getRequest));
            @SuppressWarnings("unchecked")
            final String apiKeyAuthHeader = ((Map<String, String>) getResponseMap.get("_source")).get("auth_header");

            // read is ok
            final Request searchRequest = new Request("GET", ".security/_search");
            searchRequest.setOptions(systemIndexWarningHandlerOptions(".security-7").addHeader("Authorization", apiKeyAuthHeader));
            assertOK(client().performRequest(searchRequest));

            // write must not be allowed
            final Request indexRequest = new Request("POST", ".security/_doc");
            indexRequest.setJsonEntity("""
                {
                  "doc_type": "foo"
                }""");
            indexRequest.setOptions(systemIndexWarningHandlerOptions(".security-7").addHeader("Authorization", apiKeyAuthHeader));
            final ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(indexRequest));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(e.getMessage(), containsString("is unauthorized"));
        }
    }

    /**
     * Tests that a RollUp job created on a old cluster is correctly restarted after the upgrade.
     */
    public void testRollupAfterRestart() throws Exception {
        if (isRunningAgainstOldCluster()) {
            // create dummy rollup index to circumvent the check that prohibits rollup usage in empty clusters:
            {
                Request req = new Request("PUT", "dummy-rollup-index");
                req.setJsonEntity("""
                    {
                        "mappings":{
                            "_meta": {
                                "_rollup":{
                                    "my-id": {}
                                }
                            }
                        }
                    }
                    """);
                req.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(fieldNamesFieldOk()));
                client().performRequest(req);
            }

            final int numDocs = 59;
            final int year = randomIntBetween(1970, 2018);

            // index documents for the rollup job
            final StringBuilder bulk = new StringBuilder();
            for (int i = 0; i < numDocs; i++) {
                bulk.append("{\"index\":{\"_index\":\"rollup-docs\"}}\n");
                String date = Strings.format("%04d-01-01T00:%02d:00Z", year, i);
                bulk.append("{\"timestamp\":\"").append(date).append("\",\"value\":").append(i).append("}\n");
            }
            bulk.append("\r\n");

            final Request bulkRequest = new Request("POST", "/_bulk");
            bulkRequest.setJsonEntity(bulk.toString());
            bulkRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(fieldNamesFieldOk()));
            client().performRequest(bulkRequest);

            // create the rollup job
            final Request createRollupJobRequest = new Request("PUT", "/_rollup/job/rollup-job-test");
            createRollupJobRequest.setJsonEntity("""
                {
                  "index_pattern": "rollup-*",
                  "rollup_index": "results-rollup",
                  "cron": "*/30 * * * * ?",
                  "page_size": 100,
                  "groups": {
                    "date_histogram": {
                      "field": "timestamp",
                      "fixed_interval": "5m"
                    }
                  },
                  "metrics": [
                    {
                      "field": "value",
                      "metrics": [ "min", "max", "sum" ]
                    }
                  ]
                }""");

            Map<String, Object> createRollupJobResponse = entityAsMap(client().performRequest(createRollupJobRequest));
            assertThat(createRollupJobResponse.get("acknowledged"), equalTo(Boolean.TRUE));

            // start the rollup job
            final Request startRollupJobRequest = new Request("POST", "/_rollup/job/rollup-job-test/_start");
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

    public void testTransformLegacyTemplateCleanup() throws Exception {
        @UpdateForV9(owner = UpdateForV9.Owner.MACHINE_LEARNING)
        var originalClusterSupportsTransform = oldClusterHasFeature(RestTestLegacyFeatures.TRANSFORM_SUPPORTED);
        assumeTrue("Before 7.2 transforms didn't exist", originalClusterSupportsTransform);

        if (isRunningAgainstOldCluster()) {

            // create the source index
            final Request createIndexRequest = new Request("PUT", "customers");
            createIndexRequest.setJsonEntity("""
                {
                  "mappings": {
                    "properties": {
                      "customer_id": {
                        "type": "keyword"
                      },
                      "price": {
                        "type": "double"
                      }
                    }
                  }
                }""");
            createIndexRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(fieldNamesFieldOk()));
            Map<String, Object> createIndexResponse = entityAsMap(client().performRequest(createIndexRequest));
            assertThat(createIndexResponse.get("acknowledged"), equalTo(Boolean.TRUE));

            // create a transform
            final Request createTransformRequest = new Request("PUT", "_transform/transform-full-cluster-restart-test");
            createTransformRequest.setJsonEntity("""
                {
                  "source": {
                    "index": "customers"
                  },
                  "description": "testing",
                  "dest": {
                    "index": "max_price"
                  },
                  "pivot": {
                    "group_by": {
                      "customer_id": {
                        "terms": {
                          "field": "customer_id"
                        }
                      }
                    },
                    "aggregations": {
                      "max_price": {
                        "max": {
                          "field": "price"
                        }
                      }
                    }
                  }
                }""");

            Map<String, Object> createTransformResponse = entityAsMap(client().performRequest(createTransformRequest));
            assertThat(createTransformResponse.get("acknowledged"), equalTo(Boolean.TRUE));
        } else {
            // legacy index templates created in previous releases should not be present anymore
            assertBusy(() -> {
                Request request = new Request("GET", "/_template/.transform-*,.data-frame-*");
                try {
                    Response response = client().performRequest(request);
                    Map<String, Object> responseLevel = entityAsMap(response);
                    assertNotNull(responseLevel);
                    assertThat(responseLevel.keySet(), empty());
                } catch (ResponseException e) {
                    // not found is fine
                    assertThat(
                        "Unexpected failure getting templates: " + e.getResponse().getStatusLine(),
                        e.getResponse().getStatusLine().getStatusCode(),
                        is(404)
                    );
                }
            });
        }
    }

    public void testSlmPolicyAndStats() throws IOException {
        @UpdateForV9(owner = UpdateForV9.Owner.DATA_MANAGEMENT)
        var originalClusterSupportsSlm = oldClusterHasFeature(RestTestLegacyFeatures.SLM_SUPPORTED);

        SnapshotLifecyclePolicy slmPolicy = new SnapshotLifecyclePolicy(
            "test-policy",
            "test-policy",
            "* * * 31 FEB ? *",
            "test-repo",
            Collections.singletonMap("indices", Collections.singletonList("*")),
            null
        );
        if (isRunningAgainstOldCluster() && originalClusterSupportsSlm) {
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

        if (isRunningAgainstOldCluster() == false && originalClusterSupportsSlm) {
            Request getSlmPolicyRequest = new Request("GET", "_slm/policy/test-policy");
            Response response = client().performRequest(getSlmPolicyRequest);
            Map<String, Object> responseMap = entityAsMap(response);
            Map<?, ?> policy = (Map<?, ?>) ((Map<?, ?>) responseMap.get("test-policy")).get("policy");
            assertEquals(slmPolicy.getName(), policy.get("name"));
            assertEquals(slmPolicy.getRepository(), policy.get("repository"));
            assertEquals(slmPolicy.getSchedule(), policy.get("schedule"));
            assertEquals(slmPolicy.getConfig(), policy.get("config"));
        }

        if (isRunningAgainstOldCluster() == false) {
            Response response = client().performRequest(new Request("GET", "_slm/stats"));
            XContentType xContentType = XContentType.fromMediaType(response.getEntity().getContentType().getValue());
            try (
                XContentParser parser = xContentType.xContent()
                    .createParser(XContentParserConfiguration.EMPTY, response.getEntity().getContent())
            ) {
                assertEquals(new SnapshotLifecycleStats(), SnapshotLifecycleStats.parse(parser));
            }
        }
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
        assertThat((Integer) hits.get("total"), greaterThanOrEqualTo(2));
    }

    private void assertBasicWatchInteractions() throws Exception {
        String watch = """
            {
              "trigger": {
                "schedule": {
                  "interval": "1s"
                }
              },
              "input": {
                "none": {}
              },
              "condition": {
                "always": {}
              },
              "actions": {
                "awesome": {
                  "logging": {
                    "level": "info",
                    "text": "test"
                  }
                }
              }
            }""";
        Request createWatchRequest = new Request("PUT", "_watcher/watch/new_watch");
        createWatchRequest.setJsonEntity(watch);
        Map<String, Object> createWatch = entityAsMap(client().performRequest(createWatchRequest));

        logger.info("create watch {}", createWatch);

        assertThat(createWatch.get("created"), equalTo(true));
        assertThat(createWatch.get("_version"), equalTo(1));

        Map<String, Object> updateWatch = entityAsMap(client().performRequest(createWatchRequest));
        assertThat(updateWatch.get("created"), equalTo(false));
        assertThat((int) updateWatch.get("_version"), greaterThanOrEqualTo(2));

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
        request.addParameter("wait_for_no_initializing_shards", "true");
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        assertThat(response.get("timed_out"), equalTo(Boolean.FALSE));
    }

    private void waitForHits(String indexName, int expectedHits) throws Exception {
        Request request = new Request("GET", "/" + indexName + "/_search");
        request.addParameter("ignore_unavailable", "true");
        request.addParameter("size", "0");
        assertBusy(() -> {
            try {
                Map<String, Object> response = entityAsMap(client().performRequest(request));
                Map<?, ?> hits = (Map<?, ?>) response.get("hits");
                logger.info("Hits are: {}", hits);
                Integer total;
                total = (Integer) ((Map<?, ?>) hits.get("total")).get("value");
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
        Request request = new Request("PUT", "/_security/user/" + id);
        request.setJsonEntity(Strings.format("""
            {
               "password" : "l0ng-r4nd0m-p@ssw0rd",
               "roles" : [ "admin", "other_role1" ],
               "full_name" : "%s",
               "email" : "%s@example.com",
               "enabled": true
            }""", randomAlphaOfLength(5), id));
        client().performRequest(request);
    }

    private void createRole(final boolean oldCluster) throws Exception {
        final String id = oldCluster ? "preupgrade_role" : "postupgrade_role";
        Request request = new Request("PUT", "/_security/role/" + id);
        request.setJsonEntity("""
            {
              "run_as": [ "abc" ],
              "cluster": [ "monitor" ],
              "indices": [
                {
                  "names": [ "events-*" ],
                  "privileges": [ "read" ],
                  "field_security" : {
                    "grant" : [ "category", "@timestamp", "message" ]
                  },
                  "query": "{\\"match\\": {\\"category\\": \\"click\\"}}"
                }
              ]
            }""");
        client().performRequest(request);
    }

    private void assertUserInfo(final boolean oldCluster) throws Exception {
        final String user = oldCluster ? "preupgrade_user" : "postupgrade_user";
        Request request = new Request("GET", "/_security/user/" + user);
        ;
        Map<String, Object> response = entityAsMap(client().performRequest(request));
        Map<?, ?> userInfo = (Map<?, ?>) response.get(user);
        assertEquals(user + "@example.com", userInfo.get("email"));
        assertNotNull(userInfo.get("full_name"));
        assertNotNull(userInfo.get("roles"));
    }

    private void assertRoleInfo(final boolean oldCluster) throws Exception {
        final String role = oldCluster ? "preupgrade_role" : "postupgrade_role";
        Map<?, ?> response = (Map<?, ?>) entityAsMap(client().performRequest(new Request("GET", "/_security/role/" + role))).get(role);
        assertNotNull(response.get("run_as"));
        assertNotNull(response.get("cluster"));
        assertNotNull(response.get("indices"));
    }

    private void assertRollUpJob(final String rollupJob) throws Exception {
        final Matcher<?> expectedStates = anyOf(equalTo("indexing"), equalTo("started"));
        waitForRollUpJob(rollupJob, expectedStates);

        // check that the rollup job is started using the RollUp API
        final Request getRollupJobRequest = new Request("GET", "_rollup/job/" + rollupJob);
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
                final String jobStateField = "task.xpack/rollup/job.state.job_state";
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
            final Request getRollupJobRequest = new Request("GET", "/_rollup/job/" + rollupJob);

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

    @SuppressWarnings("unchecked")
    public void testDataStreams() throws Exception {

        @UpdateForV9(owner = UpdateForV9.Owner.DATA_MANAGEMENT)
        var originalClusterSupportsDataStreams = oldClusterHasFeature(RestTestLegacyFeatures.DATA_STREAMS_SUPPORTED);

        @UpdateForV9(owner = UpdateForV9.Owner.DATA_MANAGEMENT)
        var originalClusterDataStreamHasDateInIndexName = oldClusterHasFeature(RestTestLegacyFeatures.NEW_DATA_STREAMS_INDEX_NAME_FORMAT);

        assumeTrue("no data streams in versions before 7.9.0", originalClusterSupportsDataStreams);
        if (isRunningAgainstOldCluster()) {
            createComposableTemplate(client(), "dst", "ds");

            Request indexRequest = new Request("POST", "/ds/_doc/1?op_type=create&refresh");
            XContentBuilder builder = JsonXContent.contentBuilder()
                .startObject()
                .field("f", "v")
                .field("@timestamp", System.currentTimeMillis())
                .endObject();
            indexRequest.setJsonEntity(Strings.toString(builder));
            indexRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(fieldNamesFieldOk()));
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
        assertEquals(
            DataStreamTestHelper.getLegacyDefaultBackingIndexName("ds", 1, timestamp, originalClusterDataStreamHasDateInIndexName),
            indices.get(0).get("index_name")
        );
        assertNumHits("ds", 1, 1);
    }

    /**
     * Tests that a single document survives. Super basic smoke test.
     */
    @UpdateForV9(owner = UpdateForV9.Owner.SEARCH_FOUNDATIONS) // Can be removed
    public void testDisableFieldNameField() throws IOException {
        assumeFalse(
            "can only disable field names field before 8.0",
            oldClusterHasFeature(RestTestLegacyFeatures.DISABLE_FIELD_NAMES_FIELD_REMOVED)
        );

        String docLocation = "/nofnf/_doc/1";
        String doc = """
            {
              "dv": "test",
              "no_dv": "test"
            }""";

        if (isRunningAgainstOldCluster()) {
            Request createIndex = new Request("PUT", "/nofnf");
            createIndex.setJsonEntity("""
                {
                  "settings": {
                    "index": {
                      "number_of_replicas": 1
                    }
                  },
                  "mappings": {
                    "_field_names": { "enabled": false },
                    "properties": {
                      "dv": { "type": "keyword" },
                      "no_dv": { "type": "keyword", "doc_values": false }
                    }
                  }
                }""");
            createIndex.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(fieldNamesFieldOk()));
            client().performRequest(createIndex);

            Request createDoc = new Request("PUT", docLocation);
            createDoc.addParameter("refresh", "true");
            createDoc.setJsonEntity(doc);
            client().performRequest(createDoc);
        }

        Request getRequest = new Request("GET", docLocation);
        assertThat(toStr(client().performRequest(getRequest)), containsString(doc));

        if (isRunningAgainstOldCluster() == false) {
            Request esql = new Request("POST", "_query");
            esql.setJsonEntity("""
                {
                  "query": "FROM nofnf | LIMIT 1"
                }""");
            // {"columns":[{"name":"dv","type":"keyword"},{"name":"no_dv","type":"keyword"}],"values":[["test",null]]}
            try {
                Map<String, Object> result = entityAsMap(client().performRequest(esql));
                MapMatcher mapMatcher = matchesMap();
                if (result.get("took") != null) {
                    mapMatcher = mapMatcher.entry("took", ((Integer) result.get("took")).intValue());
                }
                assertMap(
                    result,
                    mapMatcher.entry(
                        "columns",
                        List.of(Map.of("name", "dv", "type", "keyword"), Map.of("name", "no_dv", "type", "keyword"))
                    ).entry("values", List.of(List.of("test", "test")))
                );
            } catch (ResponseException e) {
                logger.error(
                    "failed to query index without field name field. Existing indices:\n{}",
                    EntityUtils.toString(client().performRequest(new Request("GET", "_cat/indices")).getEntity())
                );
                throw e;
            }
        }
    }

    /**
     * Ignore the warning about the {@code _field_names} field. We intentionally
     * turn that field off sometimes. And other times old versions spuriously
     * send it back to us.
     */
    private WarningsHandler fieldNamesFieldOk() {
        return warnings -> switch (warnings.size()) {
            case 0 -> false;  // old versions don't return a warning
            case 1 -> false == warnings.get(0).contains("_field_names");
            default -> true;
        };
    }

    private static void createComposableTemplate(RestClient client, String templateName, String indexPattern) throws IOException {
        StringEntity templateJSON = new StringEntity(Strings.format("""
            {
              "index_patterns": "%s",
              "data_stream": {}
            }""", indexPattern), ContentType.APPLICATION_JSON);
        Request createIndexTemplateRequest = new Request("PUT", "_index_template/" + templateName);
        createIndexTemplateRequest.setEntity(templateJSON);
        client.performRequest(createIndexTemplateRequest);
    }

    private RequestOptions.Builder systemIndexWarningHandlerOptions(String index) {
        return RequestOptions.DEFAULT.toBuilder()
            .setWarningsHandler(
                w -> w.size() > 0
                    && w.contains(
                        "this request accesses system indices: ["
                            + index
                            + "], but in a future major "
                            + "version, direct access to system indices will be prevented by default"
                    ) == false
            );
    }
}
