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

package org.elasticsearch.logstash;

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

public class LogstashSystemIndexIT extends ESRestTestCase {

    public void testIndexingDoc() throws IOException {
        Request request = new Request("POST", "/_logstash/.logstash/_doc/1");
        request.setJsonEntity("{ \"foo\" : \"bar\" }");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(201));
    }

    public void testDeleteFromLogstashIndex() throws IOException {
        Request request = new Request("POST", "/_logstash/.logstash/_doc/1");
        request.setJsonEntity("{ \"foo\" : \"bar\" }");
        request.addParameter("refresh", "true");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(201));

        Request deleteRequest = new Request("DELETE", "/_logstash/.logstash/_doc/1");
        Response deleteResponse = client().performRequest(deleteRequest);
        assertThat(deleteResponse.getStatusLine().getStatusCode(), is(200));
    }

    public void testGetFromLogstashIndex() throws IOException {
        Request request = new Request("POST", "/_logstash/.logstash/_doc/1");
        request.setJsonEntity("{ \"foo\" : \"bar\" }");

        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(201));

        Request getRequest = new Request("GET", "/_logstash/.logstash/_doc/1");
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        String responseBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(responseBody, containsString("foo"));
        assertThat(responseBody, containsString("bar"));
    }

    public void testMultiGetFromLogstashIndex() throws IOException {
        Request request = new Request("POST", "/_logstash/.logstash/_doc/1");
        request.setJsonEntity("{ \"foo\" : \"bar\" }");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(201));
        request = new Request("POST", "/_logstash/.logstash/_doc/2");
        request.setJsonEntity("{ \"baz\" : \"tag\" }");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(201));

        Request getRequest = new Request("GET", "/_logstash/_mget");
        getRequest.setJsonEntity("{ \"docs\" : [ { \"_index\" : \".logstash\", \"_id\" : \"1\" }, " +
            "{ \"_index\" : \".logstash\", \"_id\" : \"2\" } ] }\n");
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));
        String responseBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(responseBody, containsString("foo"));
        assertThat(responseBody, containsString("bar"));
        assertThat(responseBody, containsString("baz"));
        assertThat(responseBody, containsString("tag"));
    }

    public void testScrollingDocs() throws IOException {
        Request request = new Request("POST", "/_logstash/.logstash/_doc/1");
        request.setJsonEntity("{ \"foo\" : \"bar\" }");
        Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(201));
        request = new Request("POST", "/_logstash/.logstash/_doc/2");
        request.setJsonEntity("{ \"baz\" : \"tag\" }");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(201));
        request = new Request("POST", "/_logstash/.logstash/_doc/3");
        request.setJsonEntity("{ \"boom\" : \"ab\" }");
        request.addParameter("refresh", "true");
        response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(201));

        Request searchRequest = new Request("GET", "/_logstash/.logstash/_search");
        searchRequest.setJsonEntity("{ \"size\" : 1,\n\"query\" : { \"match_all\" : {} } }\n");
        searchRequest.addParameter("scroll", "1m");
        response = client().performRequest(searchRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        Map<String, Object> map = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
        assertNotNull(map.get("_scroll_id"));
        String scrollId = (String) map.get("_scroll_id");

        Request scrollRequest = new Request("POST", "/_logstash/_search/scroll");
        scrollRequest.addParameter("scroll_id", scrollId);
        scrollRequest.addParameter("scroll", "1m");
        response = client().performRequest(scrollRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
        map = XContentHelper.convertToMap(JsonXContent.jsonXContent, EntityUtils.toString(response.getEntity()), false);
        assertNotNull(map.get("_scroll_id"));
        scrollId = (String) map.get("_scroll_id");

        Request clearScrollRequest = new Request("DELETE", "/_logstash/_search/scroll");
        clearScrollRequest.addParameter("scroll_id", scrollId);
        response = client().performRequest(clearScrollRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(200));
    }
}
