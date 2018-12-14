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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;

public class SearchAsYouTypeIT extends ESRestTestCase {

    public void testFoo() throws IOException {
        createIndex("test",
            Settings.builder()
                .put("number_of_shards", 1)
                .put("number_of_replicas", 0)
                .build(),
            "\"_doc\": { \"properties\": {\"a_field\": {\"type\": \"search_as_you_type\"}}}");

        final String text = "quick brown fox lazy brown dog";
        final Request indexRequest = new Request(HttpPut.METHOD_NAME, "/test/_doc/1?error_trace=true");
        indexRequest.setJsonEntity("{\"a_field\": \"" + text + "\"}");
        client().performRequest(indexRequest);

        final Request getRequest = new Request(HttpGet.METHOD_NAME, "/test/_doc/1?error_trace=true");
        client().performRequest(getRequest);
    }
}
