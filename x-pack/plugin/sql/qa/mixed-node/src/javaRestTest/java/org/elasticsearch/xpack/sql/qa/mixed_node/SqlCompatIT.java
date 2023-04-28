/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.mixed_node;

import org.apache.http.HttpHost;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.ql.TestNode;
import org.elasticsearch.xpack.ql.TestNodes;
import org.elasticsearch.xpack.sql.qa.rest.BaseRestSqlTestCase;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ql.TestUtils.buildNodeAndVersions;

public class SqlCompatIT extends BaseRestSqlTestCase {

    private static RestClient newNodesClient;
    private static RestClient oldNodesClient;
    private static TransportVersion bwcVersion;

    @Before
    public void initBwcClients() throws IOException {
        if (newNodesClient == null) {
            assertNull(oldNodesClient);

            TestNodes nodes = buildNodeAndVersions(client());
            bwcVersion = nodes.getBWCTransportVersion();
            newNodesClient = buildClient(
                restClientSettings(),
                nodes.getNewNodes().stream().map(TestNode::publishAddress).toArray(HttpHost[]::new)
            );
            oldNodesClient = buildClient(
                restClientSettings(),
                nodes.getBWCNodes().stream().map(TestNode::publishAddress).toArray(HttpHost[]::new)
            );
        }
    }

    @AfterClass
    public static void cleanUpClients() throws IOException {
        IOUtils.close(newNodesClient, oldNodesClient, () -> {
            newNodesClient = null;
            oldNodesClient = null;
            bwcVersion = null;
        });
    }

    public void testNullsOrderWithMissingOrderSupportQueryingNewNode() throws IOException {
        testNullsOrderWithMissingOrderSupport(newNodesClient);
    }

    public void testNullsOrderWithMissingOrderSupportQueryingOldNode() throws IOException {
        testNullsOrderWithMissingOrderSupport(oldNodesClient);
    }

    private void testNullsOrderWithMissingOrderSupport(RestClient client) throws IOException {
        List<Integer> result = runOrderByNullsLastQuery(client);

        assertEquals(3, result.size());
        assertEquals(Integer.valueOf(1), result.get(0));
        assertEquals(Integer.valueOf(2), result.get(1));
        assertNull(result.get(2));
    }

    private void indexDocs() throws IOException {
        Request putIndex = new Request("PUT", "/test");
        putIndex.setJsonEntity("""
            {"settings":{"index":{"number_of_shards":3}}}""");
        client().performRequest(putIndex);

        Request indexDocs = new Request("POST", "/test/_bulk");
        indexDocs.addParameter("refresh", "true");
        StringBuilder bulk = new StringBuilder();
        for (String doc : Arrays.asList("{\"int\":1,\"kw\":\"foo\"}", "{\"int\":2,\"kw\":\"bar\"}", "{\"kw\":\"bar\"}")) {
            bulk.append("{\"index\":{}}\n").append(doc).append("\n");
        }

        indexDocs.setJsonEntity(bulk.toString());
        client().performRequest(indexDocs);
    }

    @SuppressWarnings("unchecked")
    private List<Integer> runOrderByNullsLastQuery(RestClient queryClient) throws IOException {
        indexDocs();

        Request query = new Request("POST", "_sql");
        query.setJsonEntity(sqlQueryEntityWithOptionalMode("SELECT int FROM test GROUP BY 1 ORDER BY 1 NULLS LAST"));
        Map<String, Object> result = performRequestAndReadBodyAsJson(queryClient, query);

        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        return rows.stream().map(row -> (Integer) row.get(0)).collect(Collectors.toList());
    }

    public static String sqlQueryEntityWithOptionalMode(String query) throws IOException {
        return sqlQueryEntityWithOptionalMode(Map.of("query", query));
    }

    public static String sqlQueryEntityWithOptionalMode(Map<String, Object> fields) throws IOException {
        XContentBuilder json = XContentFactory.jsonBuilder().startObject();
        for (Map.Entry<String, Object> entry : fields.entrySet()) {
            json.field(entry.getKey(), entry.getValue());
        }
        json.endObject();

        return Strings.toString(json);
    }

    public void testHistoricCursorFromOldNodeFailsOnNewNode() throws IOException {
        assumeTrue("BwC checks only enabled for <=8.7.0", bwcVersion.before(TransportVersion.V_8_8_0));
        assertCursorNotCompatibleAcrossVersions(bwcVersion, oldNodesClient, TransportVersion.CURRENT, newNodesClient);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/83726")
    public void testCursorFromNewNodeFailsOnOldNode() throws IOException {
        assertCursorNotCompatibleAcrossVersions(TransportVersion.CURRENT, newNodesClient, bwcVersion, oldNodesClient);
    }

    private void assertCursorNotCompatibleAcrossVersions(
        TransportVersion version1,
        RestClient client1,
        TransportVersion version2,
        RestClient client2
    ) throws IOException {
        indexDocs();

        Request req = new Request("POST", "_sql");
        req.setJsonEntity(sqlQueryEntityWithOptionalMode(Map.of("query", "SELECT int FROM test", "fetch_size", 1)));
        Map<String, Object> json = performRequestAndReadBodyAsJson(client1, req);
        String cursor = (String) json.get("cursor");
        assertThat(cursor, Matchers.not(Matchers.emptyOrNullString()));

        Request scrollReq = new Request("POST", "_sql");
        scrollReq.setJsonEntity(Strings.format("{\"cursor\": \"%s\"}", cursor));
        ResponseException exception = expectThrows(ResponseException.class, () -> client2.performRequest(scrollReq));

        assertThat(
            exception.getMessage(),
            Matchers.containsString("Unsupported cursor version [" + version1 + "], expected [" + version2 + "]")
        );
    }

    private Map<String, Object> performRequestAndReadBodyAsJson(RestClient client, Request request) throws IOException {
        Response response = client.performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
        try (InputStream content = response.getEntity().getContent()) {
            return XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        }
    }

    public void testCreateCursorWithFormatTxtOnNewNode() throws IOException {
        testCreateCursorWithFormatTxt(newNodesClient);
    }

    public void testCreateCursorWithFormatTxtOnOldNode() throws IOException {
        testCreateCursorWithFormatTxt(oldNodesClient);
    }

    /**
     * Tests covering https://github.com/elastic/elasticsearch/issues/83581
     */
    public void testCreateCursorWithFormatTxt(RestClient client) throws IOException {
        index("{\"foo\":1}", "{\"foo\":2}");

        Request query = new Request("POST", "_sql");
        XContentBuilder json = XContentFactory.jsonBuilder()
            .startObject()
            .field("query", randomFrom("SELECT foo FROM test", "SELECT foo FROM test GROUP BY foo"))
            .field("fetch_size", 1)
            .endObject();

        query.setJsonEntity(Strings.toString(json));
        query.addParameter("format", "txt");

        Response response = client.performRequest(query);
        assertOK(response);
        assertFalse(Strings.isNullOrEmpty(response.getHeader("Cursor")));
    }

}
