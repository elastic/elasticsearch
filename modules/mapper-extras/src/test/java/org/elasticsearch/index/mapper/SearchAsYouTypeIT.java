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

package org.elasticsearch.index.mapper;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.util.List;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.equalTo;

public class SearchAsYouTypeIT extends ESRestTestCase {

    public void testTerms() throws IOException {
        createIndex("test",
            Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .build(),
            "\"_doc\": { \"properties\": {\"a_field\": {\"type\": \"search_as_you_type\", \"max_shingle_size\": 4}}}");

        final String text = "quick red fox lazy brown dog";

        {
            final Request request = new Request(HttpPut.METHOD_NAME, "/test/_doc/1");
            request.addParameter("refresh", "true");
            request.setJsonEntity("{\"a_field\": \"" + text + "\"}");
            client().performRequest(request);
        }

        {
            final Request request = new Request(HttpGet.METHOD_NAME, "/test/_doc/1");
            final Response response = client().performRequest(request);
            final ObjectPath objectPath = ObjectPath.createFromResponse(response);
            assertTrue(objectPath.evaluate("found"));
            assertThat(objectPath.evaluate("_source.a_field"), equalTo(text));
        }

        termQuery("a_field._2gram", asList("quick red", "red fox", "fox lazy", "lazy brown", "brown dog"), text);
        termQuery("a_field._3gram", asList("quick red fox", "red fox lazy", "fox lazy brown", "lazy brown dog"), text);
        termQuery("a_field._4gram", asList("quick red fox lazy", "red fox lazy brown", "fox lazy brown dog"), text);
    }

    private static void termQuery(String fieldName, List<String> terms, String fieldValue) throws IOException {
        for (String term : terms) {
            final Request request = new Request(HttpGet.METHOD_NAME, "/test/_search");
            request.setJsonEntity("{\"query\":{\"term\":{\"" + fieldName + "\":\"" + term + "\"}}}");
            final Response response = client().performRequest(request);
            final ObjectPath objectPath = ObjectPath.createFromResponse(response);
            assertThat(objectPath.evaluate("hits.total.value"), equalTo(1));
            assertThat(objectPath.evaluate("hits.hits.0._source.a_field"), equalTo(fieldValue));
        }

        final Request request = new Request(HttpGet.METHOD_NAME, "/test/_search");
        request.setJsonEntity("{\"query\":{\"term\":{\"" + fieldName + "\":\"no match\"}}}");
        final Response response = client().performRequest(request);
        final ObjectPath objectPath = ObjectPath.createFromResponse(response);
        assertThat(objectPath.evaluate("hits.total.value"), equalTo(0));
    }
}
