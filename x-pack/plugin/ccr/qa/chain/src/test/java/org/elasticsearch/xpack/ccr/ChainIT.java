/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr;

import org.apache.http.HttpHost;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class ChainIT extends ESRestTestCase {

    private final String targetCluster = System.getProperty("tests.target_cluster");

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    public void testFollowIndex() throws Exception {
        final int numDocs = 128;
        final String leaderIndexName = "leader";
        final String middleIndexName = "middle";
        if ("leader".equals(targetCluster)) {
            logger.info("Running against leader cluster");
            String mapping = "";
            if (randomBoolean()) { // randomly do source filtering on indexing
                mapping =
                        "\"_doc\": {" +
                                "  \"_source\": {" +
                                "    \"includes\": [\"field\"]," +
                                "    \"excludes\": [\"filtered_field\"]" +
                                "   }"+
                                "}";
            }
            Settings indexSettings = Settings.builder()
                    .put("index.soft_deletes.enabled", true)
                    .build();
            createIndex(leaderIndexName, indexSettings, mapping);
            for (int i = 0; i < numDocs; i++) {
                logger.info("Indexing doc [{}]", i);
                index(client(), leaderIndexName, Integer.toString(i), "field", i, "filtered_field", "true");
            }
            refresh(leaderIndexName);
            verifyDocuments(leaderIndexName, numDocs);
        } else if ("middle".equals(targetCluster)) {
            logger.info("Running against middle cluster");
            followIndex("leader_cluster:" + leaderIndexName, middleIndexName);
            assertBusy(() -> verifyDocuments(middleIndexName, numDocs));
            try (RestClient leaderClient = buildLeaderClient()) {
                int id = numDocs;
                index(leaderClient, leaderIndexName, Integer.toString(id), "field", id, "filtered_field", "true");
                index(leaderClient, leaderIndexName, Integer.toString(id + 1), "field", id + 1, "filtered_field", "true");
                index(leaderClient, leaderIndexName, Integer.toString(id + 2), "field", id + 2, "filtered_field", "true");
            }
            assertBusy(() -> verifyDocuments(middleIndexName, numDocs + 3));
        } else if ("follow".equals(targetCluster)) {
            logger.info("Running against follow cluster");
            final String followIndexName = "follow";
            followIndex("middle_cluster:" + middleIndexName, followIndexName);
            assertBusy(() -> verifyDocuments(followIndexName, numDocs + 3));

            try (RestClient leaderClient = buildLeaderClient()) {
                int id = numDocs + 3;
                index(leaderClient, leaderIndexName, Integer.toString(id), "field", id, "filtered_field", "true");
                index(leaderClient, leaderIndexName, Integer.toString(id + 1), "field", id + 1, "filtered_field", "true");
                index(leaderClient, leaderIndexName, Integer.toString(id + 2), "field", id + 2, "filtered_field", "true");
            }

            try (RestClient middleClient = buildMiddleClient()) {
                assertBusy(() -> verifyDocuments(middleIndexName, numDocs + 6, middleClient));
            }

            assertBusy(() -> verifyDocuments(followIndexName, numDocs + 6));
        } else {
            fail("unexpected target cluster [" + targetCluster + "]");
        }
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

    private static void refresh(String index) throws IOException {
        assertOK(client().performRequest(new Request("POST", "/" + index + "/_refresh")));
    }

    private static void followIndex(String leaderIndex, String followIndex) throws IOException {
        final Request request = new Request("PUT", "/" + followIndex + "/_ccr/follow");
        request.setJsonEntity("{\"leader_index\": \"" + leaderIndex + "\", \"poll_timeout\": \"10ms\"}");
        assertOK(client().performRequest(request));
    }

    private static void verifyDocuments(String index, int expectedNumDocs) throws IOException {
        verifyDocuments(index, expectedNumDocs, client());
    }

    private static void verifyDocuments(final String index, final int expectedNumDocs, final RestClient client) throws IOException {
        final Request request = new Request("GET", "/" + index + "/_search");
        request.addParameter("size", Integer.toString(expectedNumDocs));
        request.addParameter("sort", "field:asc");
        request.addParameter("q", "filtered_field:true");
        Map<String, ?> response = toMap(client.performRequest(request));

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

    private RestClient buildLeaderClient() throws IOException {
        assert "leader".equals(targetCluster) == false;
        return buildClient(System.getProperty("tests.leader_host"));
    }

    private RestClient buildMiddleClient() throws IOException {
        assert "middle".equals(targetCluster) == false;
        return buildClient(System.getProperty("tests.middle_host"));
    }

    private RestClient buildClient(final String url) throws IOException {
        int portSeparator = url.lastIndexOf(':');
        HttpHost httpHost = new HttpHost(url.substring(0, portSeparator),
                Integer.parseInt(url.substring(portSeparator + 1)), getProtocol());
        return buildClient(Settings.EMPTY, new HttpHost[]{httpHost});
    }

}
