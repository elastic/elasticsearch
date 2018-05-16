/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr;

import org.apache.http.HttpHost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class FollowIndexIT extends ESRestTestCase {

    private final boolean runningAgainstLeaderCluster = Booleans.parseBoolean(System.getProperty("tests.is_leader_cluster"));

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    public void testFollowIndex() throws Exception {
        final int numDocs = 128;
        final String leaderIndexName = "test_index1";
        if (runningAgainstLeaderCluster) {
            logger.info("Running against leader cluster");
            Settings indexSettings = Settings.builder()
                    .put("index.soft_deletes.enabled", true)
                    .build();
            createIndex(leaderIndexName, indexSettings);
            for (int i = 0; i < numDocs; i++) {
                logger.info("Indexing doc [{}]", i);
                index(client(), leaderIndexName, Integer.toString(i), "field", i);
            }
            refresh(leaderIndexName);
            verifyDocuments(leaderIndexName, numDocs);
        } else {
            logger.info("Running against follow cluster");
            final String followIndexName = "test_index2";
            Settings indexSettings = Settings.builder()
                    .put("index.xpack.ccr.following_index", true)
                    .build();
            // TODO: remove mapping here when ccr syncs mappings too
            createIndex(followIndexName, indexSettings, "\"doc\": { \"properties\": { \"field\": { \"type\": \"integer\" }}}");
            ensureYellow(followIndexName);

            followIndex("leader_cluster:" + leaderIndexName, followIndexName);
            assertBusy(() -> verifyDocuments(followIndexName, numDocs));

            try (RestClient leaderClient = buildLeaderClient()) {
                int id = numDocs;
                index(leaderClient, leaderIndexName, Integer.toString(id), "field", id);
                index(leaderClient, leaderIndexName, Integer.toString(id + 1), "field", id + 1);
                index(leaderClient, leaderIndexName, Integer.toString(id + 2), "field", id + 2);
            }

            assertBusy(() -> verifyDocuments(followIndexName, numDocs + 3));
        }
    }

    private static void index(RestClient client, String index, String id, Object... fields) throws IOException {
        XContentBuilder document = jsonBuilder().startObject();
        for (int i = 0; i < fields.length; i += 2) {
            document.field((String) fields[i], fields[i + 1]);
        }
        document.endObject();
        assertOK(client.performRequest("POST", "/" + index + "/doc/" + id, emptyMap(),
                new StringEntity(Strings.toString(document), ContentType.APPLICATION_JSON)));
    }

    private static void refresh(String index) throws IOException {
        assertOK(client().performRequest("POST", "/" + index + "/_refresh"));
    }

    private static void followIndex(String leaderIndex, String followIndex) throws IOException {
        Map<String, String> params = Collections.singletonMap("leader_index", leaderIndex);
        assertOK(client().performRequest("POST", "/" + followIndex + "/_xpack/ccr/_follow", params));
    }

    private static void verifyDocuments(String index, int expectedNumDocs) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("size", Integer.toString(expectedNumDocs));
        params.put("sort", "field:asc");
        Map<String, ?> response = toMap(client().performRequest("GET", "/" + index + "/_search", params));

        int numDocs = (int) XContentMapValues.extractValue("hits.total", response);
        assertThat(numDocs, equalTo(expectedNumDocs));

        List<?> hits = (List<?>) XContentMapValues.extractValue("hits.hits", response);
        assertThat(hits.size(), equalTo(expectedNumDocs));
        for (int i = 0; i < expectedNumDocs; i++) {
            int value = (int) XContentMapValues.extractValue("_source.field", (Map<?, ?>) hits.get(i));
            assertThat(i, equalTo(value));
        }
    }

    private static Map<String, Object> toMap(Response response) throws IOException {
        return toMap(EntityUtils.toString(response.getEntity()));
    }

    private static Map<String, Object> toMap(String response) {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response, false);
    }

    private static void ensureYellow(String index) throws IOException {
        Map<String, String> params = new HashMap<>();
        params.put("wait_for_status", "yellow");
        params.put("wait_for_no_relocating_shards", "true");
        params.put("timeout", "30s");
        params.put("level", "shards");
        assertOK(client().performRequest("GET", "_cluster/health/" + index, params));
    }

    private RestClient buildLeaderClient() throws IOException {
        assert runningAgainstLeaderCluster == false;
        String leaderUrl = System.getProperty("tests.leader_host");
        int portSeparator = leaderUrl.lastIndexOf(':');
        HttpHost httpHost = new HttpHost(leaderUrl.substring(0, portSeparator),
                Integer.parseInt(leaderUrl.substring(portSeparator + 1)), getProtocol());
        return buildClient(Settings.EMPTY, new HttpHost[]{httpHost});
    }

}
