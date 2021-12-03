/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.qa.mixed_node;

import org.apache.http.HttpHost;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.ql.TestNode;
import org.elasticsearch.xpack.ql.TestNodes;
import org.elasticsearch.xpack.sql.qa.rest.BaseRestSqlTestCase;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.ql.TestUtils.buildNodeAndVersions;
import static org.elasticsearch.xpack.ql.execution.search.QlSourceBuilder.INTRODUCING_MISSING_ORDER_IN_COMPOSITE_AGGS_VERSION;

public class SqlCompatIT extends BaseRestSqlTestCase {

    private static RestClient newNodesClient;
    private static RestClient oldNodesClient;
    private static Version bwcVersion;

    @Before
    public void initBwcClients() throws IOException {
        if (newNodesClient == null) {
            assertNull(oldNodesClient);

            TestNodes nodes = buildNodeAndVersions(client());
            bwcVersion = nodes.getBWCVersion();
            newNodesClient = buildClient(
                restClientSettings(),
                nodes.getNewNodes().stream().map(TestNode::getPublishAddress).toArray(HttpHost[]::new)
            );
            oldNodesClient = buildClient(
                restClientSettings(),
                nodes.getBWCNodes().stream().map(TestNode::getPublishAddress).toArray(HttpHost[]::new)
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

    public void testNullsOrderBeforeMissingOrderSupportQueryingNewNode() throws IOException {
        testNullsOrderBeforeMissingOrderSupport(newNodesClient);
    }

    public void testNullsOrderBeforeMissingOrderSupportQueryingOldNode() throws IOException {
        testNullsOrderBeforeMissingOrderSupport(oldNodesClient);
    }

    private void testNullsOrderBeforeMissingOrderSupport(RestClient client) throws IOException {
        assumeTrue(
            "expected some nodes without support for missing_order but got none",
            bwcVersion.before(INTRODUCING_MISSING_ORDER_IN_COMPOSITE_AGGS_VERSION)
        );

        List<Integer> result = runOrderByNullsLastQuery(client);

        assertEquals(3, result.size());
        assertNull(result.get(0));
        assertEquals(Integer.valueOf(1), result.get(1));
        assertEquals(Integer.valueOf(2), result.get(2));
    }

    public void testNullsOrderWithMissingOrderSupportQueryingNewNode() throws IOException {
        testNullsOrderWithMissingOrderSupport(newNodesClient);
    }

    public void testNullsOrderWithMissingOrderSupportQueryingOldNode() throws IOException {
        testNullsOrderWithMissingOrderSupport(oldNodesClient);
    }

    private void testNullsOrderWithMissingOrderSupport(RestClient client) throws IOException {
        assumeTrue(
            "expected all nodes with support for missing_order but got some without",
            bwcVersion.onOrAfter(INTRODUCING_MISSING_ORDER_IN_COMPOSITE_AGGS_VERSION)
        );

        List<Integer> result = runOrderByNullsLastQuery(client);

        assertEquals(3, result.size());
        assertEquals(Integer.valueOf(1), result.get(0));
        assertEquals(Integer.valueOf(2), result.get(1));
        assertNull(result.get(2));
    }

    @SuppressWarnings("unchecked")
    private List<Integer> runOrderByNullsLastQuery(RestClient queryClient) throws IOException {
        Request putIndex = new Request("PUT", "/test");
        putIndex.setJsonEntity("{\"settings\":{\"index\":{\"number_of_shards\":3}}}");
        client().performRequest(putIndex);

        Request indexDocs = new Request("POST", "/test/_bulk");
        indexDocs.addParameter("refresh", "true");
        StringBuilder bulk = new StringBuilder();
        for (String doc : Arrays.asList("{\"int\":1,\"kw\":\"foo\"}", "{\"int\":2,\"kw\":\"bar\"}", "{\"kw\":\"bar\"}")) {
            bulk.append("{\"index\":{}\n").append(doc).append("\n");
        }
        indexDocs.setJsonEntity(bulk.toString());
        client().performRequest(indexDocs);

        Request query = new Request("POST", "_sql");
        query.setJsonEntity(sqlQueryEntityWithOptionalMode("SELECT int FROM test GROUP BY 1 ORDER BY 1 NULLS LAST", bwcVersion));
        Response queryResponse = queryClient.performRequest(query);

        assertEquals(200, queryResponse.getStatusLine().getStatusCode());

        InputStream content = queryResponse.getEntity().getContent();
        Map<String, Object> result = XContentHelper.convertToMap(JsonXContent.jsonXContent, content, false);
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        return rows.stream().map(row -> (Integer) row.get(0)).collect(Collectors.toList());
    }

    public static String sqlQueryEntityWithOptionalMode(String query, Version bwcVersion) throws IOException {
        XContentBuilder json = XContentFactory.jsonBuilder().startObject();
        json.field("query", query);
        if (bwcVersion.before(Version.V_7_12_0)) {
            // a bug previous to 7.12 caused a NullPointerException when accessing displaySize in ColumnInfo. The bug has been addressed in
            // https://github.com/elastic/elasticsearch/pull/68802/files
            // #diff-2faa4e2df98a4636300a19d9d890a1bd7174e9b20dd3a8589d2c78a3d9e5cbc0L110
            // as a workaround, use JDBC (driver) mode in versions prior to 7.12
            json.field("mode", "jdbc");
            json.field("binary_format", false);
            json.field("version", bwcVersion.toString());
        }
        json.endObject();

        return Strings.toString(json);
    }

}
