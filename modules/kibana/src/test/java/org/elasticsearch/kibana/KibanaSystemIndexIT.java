/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.kibana;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class KibanaSystemIndexIT extends ESRestTestCase {

    public void testCreateIndex() throws IOException {
        Request request = new Request("PUT", "/_kibana/.kibana-1");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    public void testAliases() throws IOException {
        Request request = new Request("PUT", "/_kibana/.kibana-1");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        request = new Request("PUT", "/_kibana/.kibana-1/_alias/.kibana");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        request = new Request("GET", "/_kibana/_aliases");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        assertThat(EntityUtils.toString(response.getEntity()), containsString(".kibana"));
    }

    public void testBulkToKibanaIndex() throws IOException {
        Request request = new Request("POST", "/_kibana/_bulk");
        request.setJsonEntity("{ \"index\" : { \"_index\" : \".kibana\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    public void testRefresh() throws IOException {
        Request request = new Request("POST", "/_kibana/_bulk");
        request.setJsonEntity("{ \"index\" : { \"_index\" : \".kibana\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        request = new Request("GET", "/_kibana/.kibana/_refresh");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request getRequest = new Request("GET", "/_kibana/.kibana/_doc/1");
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        String responseBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(responseBody, containsString("foo"));
        assertThat(responseBody, containsString("bar"));
    }

    public void testGetFromKibanaIndex() throws IOException {
        Request request = new Request("POST", "/_kibana/_bulk");
        request.setJsonEntity("{ \"index\" : { \"_index\" : \".kibana\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n");
        request.addParameter("refresh", "true");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request getRequest = new Request("GET", "/_kibana/.kibana/_doc/1");
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        String responseBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(responseBody, containsString("foo"));
        assertThat(responseBody, containsString("bar"));
    }

    public void testMultiGetFromKibanaIndex() throws IOException {
        Request request = new Request("POST", "/_kibana/_bulk");
        request.setJsonEntity(
            "{ \"index\" : { \"_index\" : \".kibana\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n"
                + "{ \"index\" : { \"_index\" : \".kibana\", \"_id\" : \"2\" } }\n{ \"baz\" : \"tag\" }\n"
        );
        request.addParameter("refresh", "true");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request getRequest = new Request("GET", "/_kibana/_mget");
        getRequest.setJsonEntity(
            "{ \"docs\" : [ { \"_index\" : \".kibana\", \"_id\" : \"1\" }, " + "{ \"_index\" : \".kibana\", \"_id\" : \"2\" } ] }\n"
        );
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        String responseBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(responseBody, containsString("foo"));
        assertThat(responseBody, containsString("bar"));
        assertThat(responseBody, containsString("baz"));
        assertThat(responseBody, containsString("tag"));
    }

    public void testSearchFromKibanaIndex() throws IOException {
        Request request = new Request("POST", "/_kibana/_bulk");
        request.setJsonEntity(
            "{ \"index\" : { \"_index\" : \".kibana\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n"
                + "{ \"index\" : { \"_index\" : \".kibana\", \"_id\" : \"2\" } }\n{ \"baz\" : \"tag\" }\n"
        );
        request.addParameter("refresh", "true");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request searchRequest = new Request("GET", "/_kibana/.kibana/_search");
        searchRequest.setJsonEntity("{ \"query\" : { \"match_all\" : {} } }\n");
        Response getResponse = client().performRequest(searchRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        String responseBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(responseBody, containsString("foo"));
        assertThat(responseBody, containsString("bar"));
        assertThat(responseBody, containsString("baz"));
        assertThat(responseBody, containsString("tag"));
    }

    public void testDeleteFromKibanaIndex() throws IOException {
        Request request = new Request("POST", "/_kibana/_bulk");
        request.setJsonEntity(
            "{ \"index\" : { \"_index\" : \".kibana\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n"
                + "{ \"index\" : { \"_index\" : \".kibana\", \"_id\" : \"2\" } }\n{ \"baz\" : \"tag\" }\n"
        );
        request.addParameter("refresh", "true");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request deleteRequest = new Request("DELETE", "/_kibana/.kibana/_doc/1");
        Response deleteResponse = client().performRequest(deleteRequest);
        assertThat(deleteResponse.getStatusLine().getStatusCode(), is(200));
    }

    public void testDeleteByQueryFromKibanaIndex() throws IOException {
        Request request = new Request("POST", "/_kibana/_bulk");
        request.setJsonEntity(
            "{ \"index\" : { \"_index\" : \".kibana\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n"
                + "{ \"index\" : { \"_index\" : \".kibana\", \"_id\" : \"2\" } }\n{ \"baz\" : \"tag\" }\n"
        );
        request.addParameter("refresh", "true");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request dbqRequest = new Request("POST", "/_kibana/.kibana/_delete_by_query");
        dbqRequest.setJsonEntity("{ \"query\" : { \"match_all\" : {} } }\n");
        Response dbqResponse = client().performRequest(dbqRequest);
        assertThat(dbqResponse.getStatusLine().getStatusCode(), is(200));
    }

    public void testUpdateIndexSettings() throws IOException {
        Request request = new Request("PUT", "/_kibana/.kibana-1");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        request = new Request("PUT", "/_kibana/.kibana-1/_settings");
        request.setJsonEntity("{ \"index.blocks.read_only\" : false }");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    public void testGetIndex() throws IOException {
        Request request = new Request("PUT", "/_kibana/.kibana-1");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        request = new Request("GET", "/_kibana/.kibana-1");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        assertThat(EntityUtils.toString(response.getEntity()), containsString(".kibana-1"));
    }

    public void testIndexingAndUpdatingDocs() throws IOException {
        Request request = new Request("PUT", "/_kibana/.kibana-1/_doc/1");
        request.setJsonEntity("{ \"foo\" : \"bar\" }");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(201));

        request = new Request("PUT", "/_kibana/.kibana-1/_create/2");
        request.setJsonEntity("{ \"foo\" : \"bar\" }");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(201));

        request = new Request("POST", "/_kibana/.kibana-1/_doc");
        request.setJsonEntity("{ \"foo\" : \"bar\" }");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(201));

        request = new Request("GET", "/_kibana/.kibana-1/_refresh");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        request = new Request("POST", "/_kibana/.kibana-1/_update/1");
        request.setJsonEntity("{ \"doc\" : { \"foo\" : \"baz\" } }");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }

    public void testScrollingDocs() throws IOException {
        Request request = new Request("POST", "/_kibana/_bulk");
        request.setJsonEntity(
            "{ \"index\" : { \"_index\" : \".kibana\", \"_id\" : \"1\" } }\n{ \"foo\" : \"bar\" }\n"
                + "{ \"index\" : { \"_index\" : \".kibana\", \"_id\" : \"2\" } }\n{ \"baz\" : \"tag\" }\n"
                + "{ \"index\" : { \"_index\" : \".kibana\", \"_id\" : \"3\" } }\n{ \"baz\" : \"tag\" }\n"
        );
        request.addParameter("refresh", "true");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(200));

        Request searchRequest = new Request("GET", "/_kibana/.kibana/_search");
        searchRequest.setJsonEntity("{ \"size\" : 1,\n\"query\" : { \"match_all\" : {} } }\n");
        searchRequest.addParameter("scroll", "1m");
        response = client().performRequest(searchRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
        assertNotNull(map.get("_scroll_id"));
        String scrollId = (String) map.get("_scroll_id");

        Request scrollRequest = new Request("POST", "/_kibana/_search/scroll");
        scrollRequest.addParameter("scroll_id", scrollId);
        scrollRequest.addParameter("scroll", "1m");
        response = client().performRequest(scrollRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        map = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
        assertNotNull(map.get("_scroll_id"));
        scrollId = (String) map.get("_scroll_id");

        Request clearScrollRequest = new Request("DELETE", "/_kibana/_search/scroll");
        clearScrollRequest.addParameter("scroll_id", scrollId);
        response = client().performRequest(clearScrollRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }
}
