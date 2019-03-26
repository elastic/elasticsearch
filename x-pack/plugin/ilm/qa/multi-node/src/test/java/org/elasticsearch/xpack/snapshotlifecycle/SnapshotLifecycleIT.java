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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.startsWith;

public class SnapshotLifecycleIT extends ESRestTestCase {

    @SuppressWarnings("unchecked")
    public void testFullPolicySnapshot() throws Exception {
        final String IDX = "test";
        int docCount = randomIntBetween(10, 50);
        List<IndexRequestBuilder> indexReqs = new ArrayList<>();
        for (int i = 0; i < docCount; i++) {
            index(client(), IDX, "" + i, "foo", "bar");
        }

        // Create a snapshot repo
        Request request = new Request("PUT", "/_snapshot/my-repo");
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

        Map<String, Object> snapConfig = new HashMap<>();
        snapConfig.put("indices", Collections.singletonList(IDX));
        SnapshotLifecyclePolicy policy = new SnapshotLifecyclePolicy("test-policy", "snap", "*/1 * * * * ?", "my-repo", snapConfig);

        Request putLifecycle = new Request("PUT", "/_ilm/snapshot/test-policy");
        XContentBuilder lifecycleBuilder = JsonXContent.contentBuilder();
        policy.toXContent(lifecycleBuilder, ToXContent.EMPTY_PARAMS);
        putLifecycle.setJsonEntity(Strings.toString(lifecycleBuilder));
        assertOK(client().performRequest(putLifecycle));

        // Check that the snapshot was actually taken
        assertBusy(() -> {
            Response response = client().performRequest(new Request("GET", "/_snapshot/my-repo/_all"));
            Map<String, Object> responseMap;
            try (InputStream is = response.getEntity().getContent()) {
                responseMap = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true);
            }
            assertThat(responseMap.size(), greaterThan(0));
            assertThat(((List<Map<String, Object>>) responseMap.get("snapshots")).size(), greaterThan(0));
            Map<String, Object> snapResponse = ((List<Map<String, Object>>) responseMap.get("snapshots")).get(0);
            assertThat(snapResponse.get("snapshot").toString(), startsWith("snap-"));
            assertThat((List<String>)snapResponse.get("indices"), equalTo(Collections.singletonList(IDX)));
        });

        Request delReq = new Request("DELETE", "/_ilm/snapshot/test-policy");
        assertOK(client().performRequest(delReq));

        // It's possible there could have been a snapshot in progress when the
        // policy is deleted, so wait for it to be finished
        assertBusy(() -> {
            assertThat(wipeSnapshots().size(), equalTo(0));
        });
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
