/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.slm;

import org.apache.http.util.EntityUtils;
import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.ilm.RolloverAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.ilm.Step;
import org.elasticsearch.xpack.core.ilm.WaitForRolloverReadyStep;
import org.elasticsearch.xpack.core.slm.SnapshotLifecyclePolicy;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.slm.history.SnapshotHistoryStore.SLM_HISTORY_INDEX_PREFIX;
import static org.elasticsearch.xpack.ilm.TimeSeriesLifecycleActionsIT.getStepKeyForIndex;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

@AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/pull/46205")
public class SnapshotLifecycleIT extends ESRestTestCase {

    @Override
    protected boolean waitForAllSnapshotsWiped() {
        return true;
    }

    public void testMissingRepo() throws Exception {
        final String policyId = "test-policy";
        final String missingRepoName = "missing-repo";
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(policyId, "snap",
            "*/1 * * * * ?", missingRepoName, Collections.emptyMap());

        Request putLifecycle = new Request("PUT", "/_slm/policy/test-policy");
        XContentBuilder lifecycleBuilder = JsonXContent.contentBuilder();
        policy.toXContent(lifecycleBuilder, ToXContent.EMPTY_PARAMS);
        putLifecycle.setJsonEntity(Strings.toString(lifecycleBuilder));
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(putLifecycle));
        Response resp = e.getResponse();
        assertThat(resp.getStatusLine().getStatusCode(), equalTo(400));
        String jsonError = EntityUtils.toString(resp.getEntity());
        assertThat(jsonError, containsString("\"type\":\"illegal_argument_exception\""));
        assertThat(jsonError, containsString("\"reason\":\"no such repository [missing-repo]\""));
    }

    @SuppressWarnings("unchecked")
    public void testFullPolicySnapshot() throws Exception {
        final String indexName = "test";
        final String policyName = "test-policy";
        final String repoId = "my-repo";
        int docCount = randomIntBetween(10, 50);
        List<IndexRequestBuilder> indexReqs = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            index(client(), indexName, "" + i, "foo", "bar");
        }

        // Create a snapshot repo
        inializeRepo(repoId);

        createSnapshotPolicy(policyName, "snap", "*/1 * * * * ?", repoId, indexName, true);

        // Check that the snapshot was actually taken
        assertBusy(() -> {
            Response response = client().performRequest(new Request("GET", "/_snapshot/" + repoId + "/_all"));
            Map<String, Object> snapshotResponseMap;
            try (InputStream is = response.getEntity().getContent()) {
                snapshotResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
            }
            assertThat(snapshotResponseMap.size(), greaterThan(0));
            List<Map<String, Object>> snapResponse = ((List<Map<String, Object>>) snapshotResponseMap.get("responses")).stream()
                .findFirst()
                .map(m -> (List<Map<String, Object>>) m.get("snapshots"))
                .orElseThrow(() -> new AssertionError("failed to find snapshot response in " + snapshotResponseMap));
            assertThat(snapResponse.size(), greaterThan(0));
            assertThat(snapResponse.get(0).get("snapshot").toString(), startsWith("snap-"));
            assertThat(snapResponse.get(0).get("indices"), equalTo(Collections.singletonList(indexName)));
            Map<String, Object> metadata = (Map<String, Object>) snapResponse.get(0).get("metadata");
            assertNotNull(metadata);
            assertThat(metadata.get("policy"), equalTo(policyName));
            assertHistoryIsPresent(policyName, true, repoId);

            // Check that the last success date was written to the cluster state
            Request getReq = new Request("GET", "/_slm/policy/" + policyName);
            Response policyMetadata = client().performRequest(getReq);
            Map<String, Object> policyResponseMap;
            try (InputStream is = policyMetadata.getEntity().getContent()) {
                policyResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
            }
            Map<String, Object> policyMetadataMap = (Map<String, Object>) policyResponseMap.get(policyName);
            Map<String, Object> lastSuccessObject = (Map<String, Object>) policyMetadataMap.get("last_success");
            assertNotNull(lastSuccessObject);
            Long lastSuccess = (Long) lastSuccessObject.get("time");
            Long modifiedDate = (Long) policyMetadataMap.get("modified_date_millis");
            assertNotNull(lastSuccess);
            assertNotNull(modifiedDate);
            assertThat(lastSuccess, greaterThan(modifiedDate));

            String lastSnapshotName = (String) lastSuccessObject.get("snapshot_name");
            assertThat(lastSnapshotName, startsWith("snap-"));

            assertHistoryIsPresent(policyName, true, repoId);
        });

        Request delReq = new Request("DELETE", "/_slm/policy/" + policyName);
        assertOK(client().performRequest(delReq));
    }

    @SuppressWarnings("unchecked")
    public void testPolicyFailure() throws Exception {
        final String policyName = "test-policy";
        final String repoName = "test-repo";
        final String indexPattern = "index-doesnt-exist";
        inializeRepo(repoName);

        // Create a policy with ignore_unvailable: false and an index that doesn't exist
        createSnapshotPolicy(policyName, "snap", "*/1 * * * * ?", repoName, indexPattern, false);

        assertBusy(() -> {
            // Check that the failure is written to the cluster state
            Request getReq = new Request("GET", "/_slm/policy/" + policyName);
            Response policyMetadata = client().performRequest(getReq);
            try (InputStream is = policyMetadata.getEntity().getContent()) {
                Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
                Map<String, Object> policyMetadataMap = (Map<String, Object>) responseMap.get(policyName);
                Map<String, Object> lastFailureObject = (Map<String, Object>) policyMetadataMap.get("last_failure");
                assertNotNull(lastFailureObject);

                Long lastFailure = (Long) lastFailureObject.get("time");
                Long modifiedDate = (Long) policyMetadataMap.get("modified_date_millis");
                assertNotNull(lastFailure);
                assertNotNull(modifiedDate);
                assertThat(lastFailure, greaterThan(modifiedDate));

                String lastFailureInfo = (String) lastFailureObject.get("details");
                assertNotNull(lastFailureInfo);
                assertThat(lastFailureInfo, containsString("no such index [index-doesnt-exist]"));

                String snapshotName = (String) lastFailureObject.get("snapshot_name");
                assertNotNull(snapshotName);
                assertThat(snapshotName, startsWith("snap-"));
            }
            assertHistoryIsPresent(policyName, false, repoName);
        });
    }

    public void testPolicyManualExecution() throws Exception {
        final String indexName = "test";
        final String policyName = "test-policy";
        final String repoId = "my-repo";
        int docCount = randomIntBetween(10, 50);
        for (int i = 0; i < docCount; i++) {
            index(client(), indexName, "" + i, "foo", "bar");
        }

        // Create a snapshot repo
        inializeRepo(repoId);

        createSnapshotPolicy(policyName, "snap", "1 2 3 4 5 ?", repoId, indexName, true);

        ResponseException badResp = expectThrows(ResponseException.class,
            () -> client().performRequest(new Request("PUT", "/_slm/policy/" + policyName + "-bad/_execute")));
        assertThat(EntityUtils.toString(badResp.getResponse().getEntity()),
            containsString("no such snapshot lifecycle policy [" + policyName + "-bad]"));

        Response goodResp = client().performRequest(new Request("PUT", "/_slm/policy/" + policyName + "/_execute"));

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, EntityUtils.toByteArray(goodResp.getEntity()))) {
            final String snapshotName = parser.mapStrings().get("snapshot_name");

            // Check that the executed snapshot is created
            assertBusy(() -> {
                try {
                    Response response = client().performRequest(new Request("GET", "/_snapshot/" + repoId + "/" + snapshotName));
                    Map<String, Object> snapshotResponseMap;
                    try (InputStream is = response.getEntity().getContent()) {
                        snapshotResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
                    }
                    assertThat(snapshotResponseMap.size(), greaterThan(0));
                    final Map<String, Object> metadata = extractMetadata(snapshotResponseMap, snapshotName);
                    assertNotNull(metadata);
                    assertThat(metadata.get("policy"), equalTo(policyName));
                    assertHistoryIsPresent(policyName, true, repoId);
                } catch (ResponseException e) {
                    fail("expected snapshot to exist but it does not: " + EntityUtils.toString(e.getResponse().getEntity()));
                }
            });
        }
    }

    @SuppressWarnings("unchecked")
    public void testSnapshotInProgress() throws Exception {
        final String indexName = "test";
        final String policyName = "test-policy";
        final String repoId = "my-repo";
        int docCount = 20;
        for (int i = 0; i < docCount; i++) {
            index(client(), indexName, "" + i, "foo", "bar");
        }

        // Create a snapshot repo
        inializeRepo(repoId, 1);

        createSnapshotPolicy(policyName, "snap", "1 2 3 4 5 ?", repoId, indexName, true);

        Response executeRepsonse = client().performRequest(new Request("PUT", "/_slm/policy/" + policyName + "/_execute"));

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(NamedXContentRegistry.EMPTY,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION, EntityUtils.toByteArray(executeRepsonse.getEntity()))) {
            final String snapshotName = parser.mapStrings().get("snapshot_name");

            // Check that the executed snapshot shows up in the SLM output
            assertBusy(() -> {
                try {
                    Response response = client().performRequest(new Request("GET", "/_slm/policy" + (randomBoolean() ? "" : "?human")));
                    Map<String, Object> policyResponseMap;
                    try (InputStream content = response.getEntity().getContent()) {
                        policyResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), content, true);
                    }
                    assertThat(policyResponseMap.size(), greaterThan(0));
                    Optional<Map<String, Object>> inProgress = Optional.ofNullable((Map<String, Object>) policyResponseMap.get(policyName))
                        .map(policy -> (Map<String, Object>) policy.get("in_progress"));

                    if (inProgress.isPresent()) {
                        Map<String, Object> inProgressMap = inProgress.get();
                        assertThat(inProgressMap.get("name"), equalTo(snapshotName));
                        assertNotNull(inProgressMap.get("uuid"));
                        assertThat(inProgressMap.get("state"), equalTo("STARTED"));
                        assertThat((long) inProgressMap.get("start_time_millis"), greaterThan(0L));
                        assertNull(inProgressMap.get("failure"));
                    } else {
                        fail("expected in_progress to contain a running snapshot, but the response was " + policyResponseMap);
                    }
                } catch (ResponseException e) {
                    fail("expected policy to exist but it does not: " + EntityUtils.toString(e.getResponse().getEntity()));
                }
            });

            // Cancel the snapshot since it is not going to complete quickly
            assertOK(client().performRequest(new Request("DELETE", "/_snapshot/" + repoId + "/" + snapshotName)));
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> extractMetadata(Map<String, Object> snapshotResponseMap, String snapshotPrefix) {
        List<Map<String, Object>> snapResponse = ((List<Map<String, Object>>) snapshotResponseMap.get("responses")).stream()
            .findFirst()
            .map(m -> (List<Map<String, Object>>) m.get("snapshots"))
            .orElseThrow(() -> new AssertionError("failed to find snapshot response in " + snapshotResponseMap));
        return snapResponse.stream()
            .filter(snapshot -> ((String) snapshot.get("snapshot")).startsWith(snapshotPrefix))
            .map(snapshot -> (Map<String, Object>) snapshot.get("metadata"))
            .findFirst()
            .orElse(null);
    }

    // This method should be called inside an assertBusy, it has no retry logic of its own
    private void assertHistoryIsPresent(String policyName, boolean success, String repository) throws IOException {
        final Request historySearchRequest = new Request("GET", ".slm-history*/_search");
        historySearchRequest.setJsonEntity("{\n" +
            "  \"query\": {\n" +
            "    \"bool\": {\n" +
            "      \"must\": [\n" +
            "        {\n" +
            "          \"term\": {\n" +
            "            \"policy\": \"" + policyName + "\"\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"term\": {\n" +
            "            \"success\": " + success + "\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"term\": {\n" +
            "            \"repository\": \"" + repository + "\"\n" +
            "          }\n" +
            "        },\n" +
            "        {\n" +
            "          \"term\": {\n" +
            "            \"operation\": \"CREATE\"\n" +
            "          }\n" +
            "        }\n" +
            "      ]\n" +
            "    }\n" +
            "  }\n" +
            "}");
        Response historyResponse;
        try {
            historyResponse = client().performRequest(historySearchRequest);
            Map<String, Object> historyResponseMap;
            try (InputStream is = historyResponse.getEntity().getContent()) {
                historyResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
            }
            assertThat((int)((Map<String, Object>) ((Map<String, Object>) historyResponseMap.get("hits")).get("total")).get("value"),
                greaterThanOrEqualTo(1));
        } catch (ResponseException e) {
            // Throw AssertionError instead of an exception if the search fails so that assertBusy works as expected
            logger.error(e);
            fail("failed to perform search:" + e.getMessage());
        }

        // Finally, check that the history index is in a good state
        assertHistoryIndexWaitingForRollover();
    }

    private void assertHistoryIndexWaitingForRollover() throws IOException {
        Step.StepKey stepKey = getStepKeyForIndex(SLM_HISTORY_INDEX_PREFIX + "000001");
        assertEquals("hot", stepKey.getPhase());
        assertEquals(RolloverAction.NAME, stepKey.getAction());
        assertEquals(WaitForRolloverReadyStep.NAME, stepKey.getName());
    }

    private void createSnapshotPolicy(String policyName, String snapshotNamePattern, String schedule, String repoId,
                                      String indexPattern, boolean ignoreUnavailable) throws IOException {
        Map<String, Object> snapConfig = new HashMap<>();
        snapConfig.put("indices", Collections.singletonList(indexPattern));
        snapConfig.put("ignore_unavailable", ignoreUnavailable);
        if (randomBoolean()) {
            Map<String, Object> metadata = new HashMap<>();
            int fieldCount = randomIntBetween(2,5);
            for (int i = 0; i < fieldCount; i++) {
                metadata.put(randomValueOtherThanMany(key -> "policy".equals(key) || metadata.containsKey(key),
                    () -> randomAlphaOfLength(5)), randomAlphaOfLength(4));
            }
        }
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(policyName, snapshotNamePattern, schedule, repoId, snapConfig);

        Request putLifecycle = new Request("PUT", "/_slm/policy/" + policyName);
        XContentBuilder lifecycleBuilder = JsonXContent.contentBuilder();
        policy.toXContent(lifecycleBuilder, ToXContent.EMPTY_PARAMS);
        putLifecycle.setJsonEntity(Strings.toString(lifecycleBuilder));
        assertOK(client().performRequest(putLifecycle));
    }

    private void inializeRepo(String repoName) throws IOException {
        inializeRepo(repoName, 256);
    }

    private void inializeRepo(String repoName, int maxBytesPerSecond) throws IOException {
        Request request = new Request("PUT", "/_snapshot/" + repoName);
        request.setJsonEntity(Strings
            .toString(JsonXContent.contentBuilder()
                .startObject()
                .field("type", "fs")
                .startObject("settings")
                .field("compress", randomBoolean())
                .field("location", System.getProperty("tests.path.repo"))
                .field("max_snapshot_bytes_per_sec", maxBytesPerSecond + "b")
                .endObject()
                .endObject()));
        assertOK(client().performRequest(request));
    }

    private static void index(RestClient client, String index, String id, Object... fields) throws IOException {
        XContentBuilder document = jsonBuilder().startObject();
        for (int i = 0; i < fields.length; i += 2) {
            document.field((String) fields[i], fields[i + 1]);
        }
        document.endObject();
        final Request request = new Request("POST", "/" + index + "/_doc/" + id);
        request.setJsonEntity(Strings.toString(document));
        assertOK(client.performRequest(request));
    }
}
