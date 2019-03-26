/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.snapshotlifecycle;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.snapshotlifecycle.SnapshotLifecyclePolicy;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.startsWith;

public class SnapshotLifecycleIT extends ESRestTestCase {

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

        createSnapshotPolicy(policyName, "snap", "*/1 * * * * ?", repoId, indexName);

        // Check that the snapshot was actually taken
        assertBusy(() -> {
            Response response = client().performRequest(new Request("GET", "/_snapshot/" + repoId + "/_all"));
            Map<String, Object> snapshotResponseMap;
            try (InputStream is = response.getEntity().getContent()) {
                snapshotResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
            }
            assertThat(snapshotResponseMap.size(), greaterThan(0));
            assertThat(((List<Map<String, Object>>) snapshotResponseMap.get("snapshots")).size(), greaterThan(0));
            Map<String, Object> snapResponse = ((List<Map<String, Object>>) snapshotResponseMap.get("snapshots")).get(0);
            assertThat(snapResponse.get("snapshot").toString(), startsWith("snap-"));
            assertThat((List<String>)snapResponse.get("indices"), equalTo(Collections.singletonList(indexName)));

            // Check that the last success date was written to the cluster state
            Request getReq = new Request("GET", "/_ilm/snapshot/" + policyName);
            Response policyMetadata = client().performRequest(getReq);
            Map<String, Object> policyResponseMap;
            try (InputStream is = policyMetadata.getEntity().getContent()) {
                policyResponseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
            }
            Map<String, Object> policyMetadataMap = (Map<String, Object>) policyResponseMap.get(policyName);
            Long lastSuccess = (Long) policyMetadataMap.get("last_success_date");
            Long modifiedDate = (Long) policyMetadataMap.get("modified_date");
            assertNotNull(lastSuccess);
            assertNotNull(modifiedDate);
            assertThat(lastSuccess, greaterThan(modifiedDate));
        });

        Request delReq = new Request("DELETE", "/_ilm/snapshot/" + policyName);
        assertOK(client().performRequest(delReq));

        // It's possible there could have been a snapshot in progress when the
        // policy is deleted, so wait for it to be finished
        assertBusy(() -> {
            assertThat(wipeSnapshots().size(), equalTo(0));
        });
    }

    public void testPolicyFailure() throws Exception {
        final String indexName = "test";
        final String policyName = "test-policy";
        int docCount = randomIntBetween(10, 50);
        List<IndexRequestBuilder> indexReqs = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            index(client(), indexName, "" + i, "foo", "bar");
        }

        // Create a policy with a repo that doesn't exist
        createSnapshotPolicy(policyName, "snap", "*/1 * * * * ?", "bad-repo", indexName);

        assertBusy(() -> {
            // Check that the failure is written to the cluster state
            Request getReq = new Request("GET", "/_ilm/snapshot/" + policyName);
            Response policyMetadata = client().performRequest(getReq);
            try (InputStream is = policyMetadata.getEntity().getContent()) {
                Map<String, Object> responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
                Map<String, Object> policyMetadataMap = (Map<String, Object>) responseMap.get(policyName);
                Long lastFailure = (Long) policyMetadataMap.get("last_failure_date");
                String lastFailureInfo = (String) policyMetadataMap.get("last_failure_info");
                Long modifiedDate = (Long) policyMetadataMap.get("modified_date");
                assertNotNull(lastFailure);
                assertNotNull(modifiedDate);
                assertThat(lastFailure, greaterThan(modifiedDate));

                assertNotNull(lastFailureInfo);
                assertThat(lastFailureInfo, containsString("[bad-repo] missing"));
            }
        });

        Request delReq = new Request("DELETE", "/_ilm/snapshot/" + policyName);
        assertOK(client().performRequest(delReq));
    }

    private void createSnapshotPolicy(String policyName, String snapshotNamePattern, String schedule, String repoId, String indexPattern) throws IOException {
        Map<String, Object> snapConfig = new HashMap<>();
        snapConfig.put("indices", Collections.singletonList(indexPattern));
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy(policyName, snapshotNamePattern, schedule, repoId, snapConfig);

        Request putLifecycle = new Request("PUT", "/_ilm/snapshot/" + policyName);
        XContentBuilder lifecycleBuilder = JsonXContent.contentBuilder();
        policy.toXContent(lifecycleBuilder, ToXContent.EMPTY_PARAMS);
        putLifecycle.setJsonEntity(Strings.toString(lifecycleBuilder));
        assertOK(client().performRequest(putLifecycle));
    }

    private void inializeRepo(String repoName) throws IOException {
        Request request = new Request("PUT", "/_snapshot/" + repoName);
        request.setJsonEntity(Strings
            .toString(JsonXContent.contentBuilder()
                .startObject()
                .field("type", "fs")
                .startObject("settings")
                .field("compress", randomBoolean())
                .field("location", System.getProperty("tests.path.repo"))
                .field("max_snapshot_bytes_per_sec", "256b")
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
