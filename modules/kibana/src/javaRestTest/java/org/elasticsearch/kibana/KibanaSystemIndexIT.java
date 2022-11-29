/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.kibana;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class KibanaSystemIndexIT extends ESRestTestCase {

    private final String indexName;

    public KibanaSystemIndexIT(@Name("indexName") String indexName) {
        this.indexName = indexName;
    }

    @ParametersFactory
    public static Iterable<Object[]> data() {
        return Arrays.asList(
            new Object[] { ".kibana" },
            new Object[] { ".kibana_1" },
            new Object[] { ".reporting-1" },
            new Object[] { ".apm-agent-configuration" },
            new Object[] { ".apm-custom-link" }
        );
    }

    public void testCreateIndex() throws IOException {
        Request request = request("PUT", "/" + indexName);
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    public void testAliases() throws IOException {
        assumeFalse("In this test, .kibana is the alias name", ".kibana".equals(indexName));
        Request request = request("PUT", "/" + indexName);
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        request = request("PUT", "/" + indexName + "/_alias/.kibana");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        request = request("GET", "/_aliases");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        assertThat(EntityUtils.toString(response.getEntity()), containsString(".kibana"));
    }

    public void testBulkToKibanaIndex() throws IOException {
        var request = request("POST", "/_bulk").setJsonEntity(formatted("""
            { "index" : { "_index" : "%s", "_id" : "1" } }
            { "foo" : "bar" }
            """, indexName));
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    public void testRefresh() throws IOException {
        var request = request("POST", "/_bulk").setJsonEntity(formatted("""
            { "index" : { "_index" : "%s", "_id" : "1" } }
            { "foo" : "bar" }
            """, indexName));
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        response = client().performRequest(request("GET", "/" + indexName + "/_refresh"));
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request getRequest = request("GET", "/" + indexName + "/_doc/1");
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        String responseBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(responseBody, containsString("foo"));
        assertThat(responseBody, containsString("bar"));
    }

    public void testGetFromKibanaIndex() throws IOException {
        var request = request("POST", "/_bulk").setJsonEntity(formatted("""
            { "index" : { "_index" : "%s", "_id" : "1" } }
            { "foo" : "bar" }
            """, indexName));
        request.addParameter("refresh", "true");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request getRequest = request("GET", "/" + indexName + "/_doc/1");
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        String responseBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(responseBody, containsString("foo"));
        assertThat(responseBody, containsString("bar"));
    }

    public void testMultiGetFromKibanaIndex() throws IOException {
        var request = request("POST", "/_bulk").setJsonEntity(formatted("""
            { "index" : { "_index" : "%s", "_id" : "1" } }
            { "foo" : "bar" }
            { "index" : { "_index" : "%s", "_id" : "2" } }
            { "baz" : "tag" }
            """, indexName, indexName)).addParameter("refresh", "true");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        var getRequest = request("GET", "/_mget").setJsonEntity(formatted("""
            {
              "docs": [
                {
                  "_index": "%s",
                  "_id": "1"
                },
                {
                  "_index": "%s",
                  "_id": "2"
                }
              ]
            }""", indexName, indexName));
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        String responseBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(responseBody, containsString("foo"));
        assertThat(responseBody, containsString("bar"));
        assertThat(responseBody, containsString("baz"));
        assertThat(responseBody, containsString("tag"));
    }

    public void testSearchFromKibanaIndex() throws IOException {
        var request = request("POST", "/_bulk").setJsonEntity(formatted("""
            { "index" : { "_index" : "%s", "_id" : "1" } }
            { "foo" : "bar" }
            { "index" : { "_index" : "%s", "_id" : "2" } }
            { "baz" : "tag" }
            """, indexName, indexName)).addParameter("refresh", "true");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        var searchRequest = request("GET", "/" + indexName + "/_search").setJsonEntity("""
            { "query" : { "match_all" : {} } }
            """);
        Response getResponse = client().performRequest(searchRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        String responseBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(responseBody, containsString("foo"));
        assertThat(responseBody, containsString("bar"));
        assertThat(responseBody, containsString("baz"));
        assertThat(responseBody, containsString("tag"));
    }

    public void testDeleteFromKibanaIndex() throws IOException {
        var request = request("POST", "/_bulk").setJsonEntity(formatted("""
            { "index" : { "_index" : "%s", "_id" : "1" } }
            { "foo" : "bar" }
            { "index" : { "_index" : "%s", "_id" : "2" } }
            { "baz" : "tag" }
            """, indexName, indexName)).addParameter("refresh", "true");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request deleteRequest = request("DELETE", "/" + indexName + "/_doc/1");
        Response deleteResponse = client().performRequest(deleteRequest);
        assertThat(deleteResponse.getStatusLine().getStatusCode(), is(200));
    }

    public void testDeleteByQueryFromKibanaIndex() throws IOException {
        var request = request("POST", "/_bulk").setJsonEntity(formatted("""
            { "index" : { "_index" : "%s", "_id" : "1" } }
            { "foo" : "bar" }
            { "index" : { "_index" : "%s", "_id" : "2" } }
            { "baz" : "tag" }
            """, indexName, indexName)).addParameter("refresh", "true");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        var dbqRequest = request("POST", "/" + indexName + "/_delete_by_query").setJsonEntity("""
            { "query" : { "match_all" : {} } }
            """);
        Response dbqResponse = client().performRequest(dbqRequest);
        assertThat(dbqResponse.getStatusLine().getStatusCode(), is(200));
    }

    public void testUpdateIndexSettings() throws IOException {
        Request request = request("PUT", "/" + indexName);
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        request = request("PUT", "/" + indexName + "/_settings");
        request.setJsonEntity("{ \"index.blocks.read_only\" : false }");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    public void testCannotCreateVisibleSystemIndex() {
        var request = request("PUT", "/" + indexName).setJsonEntity("{\"settings\": {\"index.hidden\":\"false\"}}");
        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(request));
        assertThat(
            exception.getMessage(),
            containsString("Cannot create system index [" + indexName + "] with [index.hidden] set to 'false'")
        );
    }

    public void testCannotSetVisible() throws IOException {
        Request putIndexRequest = request("PUT", "/" + indexName);
        Response response = client().performRequest(putIndexRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request putSettingsRequest = request("PUT", "/" + indexName + "/_settings");
        putSettingsRequest.setJsonEntity("{ \"index.hidden\" : false }");
        ResponseException exception = expectThrows(ResponseException.class, () -> client().performRequest(putSettingsRequest));
        assertThat(exception.getMessage(), containsString("Cannot set [index.hidden] to 'false' on system indices: [" + indexName + "]"));
    }

    public void testGetIndex() throws IOException {
        Request request = request("PUT", "/" + indexName);
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        request = request("GET", "/" + indexName);
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        assertThat(EntityUtils.toString(response.getEntity()), containsString(indexName));
    }

    public void testIndexingAndUpdatingDocs() throws IOException {
        var request = request("PUT", "/" + indexName + "/_doc/1").setJsonEntity("{ \"foo\" : \"bar\" }");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(201));

        request = request("PUT", "/" + indexName + "/_create/2");
        request.setJsonEntity("{ \"foo\" : \"bar\" }");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(201));

        request = request("POST", "/" + indexName + "/_doc");
        request.setJsonEntity("{ \"foo\" : \"bar\" }");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(201));

        request = request("GET", "/" + indexName + "/_refresh");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        request = request("POST", "/" + indexName + "/_update/1");
        request.setJsonEntity("{ \"doc\" : { \"foo\" : \"baz\" } }");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    public void testScrollingDocs() throws IOException {
        var request = request("POST", "/_bulk").setJsonEntity(formatted("""
            { "index" : { "_index" : "%s", "_id" : "1" } }
            { "foo" : "bar" }
            { "index" : { "_index" : "%s", "_id" : "2" } }
            { "baz" : "tag" }
            { "index" : { "_index" : "%s", "_id" : "3" } }
            { "baz" : "tag" }
            """, indexName, indexName, indexName)).addParameter("refresh", "true");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        var searchRequest = request("GET", "/" + indexName + "/_search").setJsonEntity("""
            { "size" : 1, "query" : { "match_all" : {} } }
            """).addParameter("scroll", "1m");
        response = client().performRequest(searchRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
        assertNotNull(map.get("_scroll_id"));
        String scrollId = (String) map.get("_scroll_id");

        Request scrollRequest = request("POST", "/_search/scroll");
        scrollRequest.addParameter("scroll_id", scrollId);
        scrollRequest.addParameter("scroll", "1m");
        response = client().performRequest(scrollRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        map = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
        assertNotNull(map.get("_scroll_id"));
        scrollId = (String) map.get("_scroll_id");

        Request clearScrollRequest = request("DELETE", "/_search/scroll");
        clearScrollRequest.addParameter("scroll_id", scrollId);
        response = client().performRequest(clearScrollRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    private Request request(String method, String endpoint) {
        return new Request(method, endpoint).setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("X-elastic-product-origin", "kibana").build()
        );
    }
}
