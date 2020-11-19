/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.fleet;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class FleetSystemIndexIT extends ESRestTestCase {

    private final String indexName;

    public FleetSystemIndexIT(@Name("indexName") String indexName) {
        this.indexName = indexName;
    }

    @ParametersFactory
    public static Iterable<Object[]> data() {
        return Arrays.asList(
            new Object[] { ".fleet-outputs" },
            new Object[] { ".fleet-policies" },
            new Object[] { ".fleet-policies-enrollment-keys" },
            new Object[] { ".fleet-policies-inputs" },
            new Object[] { ".fleet-agents" },
            new Object[] { ".fleet-agents-checkins" }
        );
    }

    public void testCreateIndex() throws IOException {
        Request request = new Request("PUT", "/_fleet/" + indexName);
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    public void testBulkToFleetIndex() throws IOException {
        Request request = new Request("POST", "/_fleet/_bulk");
        request.setJsonEntity("{ \"index\" : { \"_index\" : \"" + indexName + "\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    public void testRefresh() throws IOException {
        Request request = new Request("POST", "/_fleet/_bulk");
        request.setJsonEntity("{ \"index\" : { \"_index\" : \"" + indexName + "\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        request = new Request("GET", "/_fleet/" + indexName + "/_refresh");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request getRequest = new Request("GET", "/_fleet/" + indexName + "/_doc/1");
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        String responseBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(responseBody, containsString("foo"));
        assertThat(responseBody, containsString("bar"));
    }

    public void testGetFromFleetIndex() throws IOException {
        Request request = new Request("POST", "/_fleet/_bulk");
        request.setJsonEntity("{ \"index\" : { \"_index\" : \"" + indexName + "\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n");
        request.addParameter("refresh", "true");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request getRequest = new Request("GET", "/_fleet/" + indexName + "/_doc/1");
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        String responseBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(responseBody, containsString("foo"));
        assertThat(responseBody, containsString("bar"));
    }

    public void testSearchFromFleetIndex() throws IOException {
        Request request = new Request("POST", "/_fleet/_bulk");
        request.setJsonEntity(
            "{ \"index\" : { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n"
                + "{ \"index\" : { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"2\" } }\n{ \"baz\" : \"tag\" }\n"
        );
        request.addParameter("refresh", "true");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request searchRequest = new Request("GET", "/_fleet/" + indexName + "/_search");
        searchRequest.setJsonEntity("{ \"query\" : { \"match_all\" : {} } }\n");
        Response getResponse = client().performRequest(searchRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        String responseBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(responseBody, containsString("foo"));
        assertThat(responseBody, containsString("bar"));
        assertThat(responseBody, containsString("baz"));
        assertThat(responseBody, containsString("tag"));
    }

    public void testDeleteFromFleetIndex() throws IOException {
        Request request = new Request("POST", "/_fleet/_bulk");
        request.setJsonEntity(
            "{ \"index\" : { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n"
                + "{ \"index\" : { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"2\" } }\n{ \"baz\" : \"tag\" }\n"
        );
        request.addParameter("refresh", "true");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request deleteRequest = new Request("DELETE", "/_fleet/" + indexName + "/_doc/1");
        Response deleteResponse = client().performRequest(deleteRequest);
        assertThat(deleteResponse.getStatusLine().getStatusCode(), is(200));
    }

    public void testDeleteByQueryFromFleetIndex() throws IOException {
        Request request = new Request("POST", "/_fleet/_bulk");
        request.setJsonEntity(
            "{ \"index\" : { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n"
                + "{ \"index\" : { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"2\" } }\n{ \"baz\" : \"tag\" }\n"
        );
        request.addParameter("refresh", "true");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request dbqRequest = new Request("POST", "/_fleet/" + indexName + "/_delete_by_query");
        dbqRequest.setJsonEntity("{ \"query\" : { \"match_all\" : {} } }\n");
        Response dbqResponse = client().performRequest(dbqRequest);
        assertThat(dbqResponse.getStatusLine().getStatusCode(), is(200));
    }

    public void testIndexingAndUpdatingDocs() throws IOException {
        Request request = new Request("PUT", "/_fleet/" + indexName + "/_doc/1");
        request.setJsonEntity("{ \"foo\" : \"bar\" }");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(201));

        request = new Request("PUT", "/_fleet/" + indexName + "/_create/2");
        request.setJsonEntity("{ \"foo\" : \"bar\" }");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(201));

        request = new Request("POST", "/_fleet/" + indexName + "/_doc");
        request.setJsonEntity("{ \"foo\" : \"bar\" }");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(201));

        request = new Request("GET", "/_fleet/" + indexName + "/_refresh");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        request = new Request("POST", "/_fleet/" + indexName + "/_update/1");
        request.setJsonEntity("{ \"doc\" : { \"foo\" : \"baz\" } }");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    public void testScrollingDocs() throws IOException {
        Request request = new Request("POST", "/_fleet/_bulk");
        request.setJsonEntity(
            "{ \"index\" : { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n"
                + "{ \"index\" : { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"2\" } }\n{ \"baz\" : \"tag\" }\n"
                + "{ \"index\" : { \"_index\" : \""
                + indexName
                + "\", \"_id\" : \"3\" } }\n{ \"baz\" : \"tag\" }\n"
        );
        request.addParameter("refresh", "true");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request searchRequest = new Request("GET", "/_fleet/" + indexName + "/_search");
        searchRequest.setJsonEntity("{ \"size\" : 1,\n\"query\" : { \"match_all\" : {} } }\n");
        searchRequest.addParameter("scroll", "1m");
        response = client().performRequest(searchRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
        assertNotNull(map.get("_scroll_id"));
        String scrollId = (String) map.get("_scroll_id");

        Request scrollRequest = new Request("POST", "/_fleet/_search/scroll");
        scrollRequest.addParameter("scroll_id", scrollId);
        scrollRequest.addParameter("scroll", "1m");
        response = client().performRequest(scrollRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        map = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
        assertNotNull(map.get("_scroll_id"));
        scrollId = (String) map.get("_scroll_id");

        Request clearScrollRequest = new Request("DELETE", "/_fleet/_search/scroll");
        clearScrollRequest.addParameter("scroll_id", scrollId);
        response = client().performRequest(clearScrollRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }
}
