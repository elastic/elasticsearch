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

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.eql.EqlSearchRequest;
import org.elasticsearch.client.eql.EqlSearchResponse;
import org.junit.Before;

import static org.hamcrest.Matchers.equalTo;

public class EqlIT extends ESRestHighLevelClientTestCase {

    @Before
    public void setupRemoteClusterConfig() throws Exception {
        setupRemoteClusterConfig("local_cluster");
    }

    public void testBasicSearch() throws Exception {

        Request doc1 = new Request(HttpPut.METHOD_NAME, "/index/_doc/1");
        doc1.setJsonEntity("{\"event_subtype_full\": \"already_running\", " +
                "\"event_type\": \"process\", " +
                "\"event_type_full\": \"process_event\", " +
                "\"opcode\": 3," +
                "\"pid\": 0," +
                "\"process_name\": \"System Idle Process\"," +
                "\"serial_event_id\": 1," +
                "\"subtype\": \"create\"," +
                "\"timestamp\": 116444736000000000," +
                "\"unique_pid\": 1}");
        client().performRequest(doc1);
        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));

        EqlClient eql = highLevelClient().eql();
        EqlSearchRequest request = new EqlSearchRequest("index", "process where true");
        EqlSearchResponse response = execute(request, eql::search, eql::searchAsync);
        assertNotNull(response);
        assertFalse(response.isTimeout());
        assertNotNull(response.hits());
        assertNull(response.hits().sequences());
        assertNull(response.hits().counts());
        assertNotNull(response.hits().events());
        assertThat(response.hits().events().size(), equalTo(1));
    }
}
