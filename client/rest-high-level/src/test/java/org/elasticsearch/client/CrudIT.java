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

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class CrudIT extends ESRestHighLevelClientTestCase {

    public void testGet() throws IOException {
        String document = "{\"field1\":\"value1\",\"field2\":\"value2\"}";
        StringEntity stringEntity = new StringEntity(document, ContentType.APPLICATION_JSON);
        Response response = client().performRequest("PUT", "/index/type/id", Collections.singletonMap("refresh", "wait_for"), stringEntity);
        assertEquals(201, response.getStatusLine().getStatusCode());
        {
            GetRequest getRequest = new GetRequest("index", "type", "id");
            if (randomBoolean()) {
                getRequest.version(1L);
            }
            GetResponse getResponse = execute(getRequest, highLevelClient()::get, highLevelClient()::getAsync);
            assertEquals("index", getResponse.getIndex());
            assertEquals("type", getResponse.getType());
            assertEquals("id", getResponse.getId());
            assertTrue(getResponse.isExists());
            assertFalse(getResponse.isSourceEmpty());
            assertEquals(1L, getResponse.getVersion());
            assertEquals(document, getResponse.getSourceAsString());
        }
        {
            GetRequest getRequest = new GetRequest("index", "type", "does_not_exist");
            GetResponse getResponse = execute(getRequest, highLevelClient()::get, highLevelClient()::getAsync);
            assertEquals("index", getResponse.getIndex());
            assertEquals("type", getResponse.getType());
            assertEquals("does_not_exist", getResponse.getId());
            assertFalse(getResponse.isExists());
            assertEquals(-1, getResponse.getVersion());
            assertTrue(getResponse.isSourceEmpty());
            assertNull(getResponse.getSourceAsString());
        }
        {
            GetRequest getRequest = new GetRequest("index", "type", "id");
            getRequest.fetchSourceContext(new FetchSourceContext(false, Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY));
            GetResponse getResponse = execute(getRequest, highLevelClient()::get, highLevelClient()::getAsync);
            assertEquals("index", getResponse.getIndex());
            assertEquals("type", getResponse.getType());
            assertEquals("id", getResponse.getId());
            assertTrue(getResponse.isExists());
            assertTrue(getResponse.isSourceEmpty());
            assertEquals(1L, getResponse.getVersion());
            assertNull(getResponse.getSourceAsString());
        }
        {
            GetRequest getRequest = new GetRequest("index", "type", "id");
            if (randomBoolean()) {
                getRequest.fetchSourceContext(new FetchSourceContext(true, new String[]{"field1"}, Strings.EMPTY_ARRAY));
            } else {
                getRequest.fetchSourceContext(new FetchSourceContext(true, Strings.EMPTY_ARRAY, new String[]{"field2"}));
            }

            GetResponse getResponse = execute(getRequest, highLevelClient()::get, highLevelClient()::getAsync);
            assertEquals("index", getResponse.getIndex());
            assertEquals("type", getResponse.getType());
            assertEquals("id", getResponse.getId());
            assertTrue(getResponse.isExists());
            assertFalse(getResponse.isSourceEmpty());
            assertEquals(1L, getResponse.getVersion());
            Map<String, Object> sourceAsMap = getResponse.getSourceAsMap();
            assertEquals(1, sourceAsMap.size());
            assertEquals("value1", sourceAsMap.get("field1"));
        }
    }

    //TODO test exceptions once we are able to parse them back
    //another exception that could be tested is IndexNotFoundException
    /*
    {
        GetRequest getRequest = new GetRequest("index", "type", "id").version(2);
        GetResponse getResponse = highLevelClient().get(getRequest);
        assertEquals("index", getResponse.getIndex());
        assertEquals("type", getResponse.getType());
        assertEquals("id", getResponse.getId());
        assertFalse(getResponse.isExists());
        assertEquals(-1, getResponse.getVersion());
        assertTrue(getResponse.isSourceEmpty());
        assertNull(getResponse.getSourceAsString());

        409 Conflict
        {"error":{"root_cause":[{"type":"version_conflict_engine_exception",
        "reason":"[type][id]: version conflict, current version [1] is different than the one provided [2]",
        "index_uuid":"fMhefwt5S-m3NLiFhniBwA","shard":"4","index":"index"}],
        "type":"version_conflict_engine_exception",
        "reason":"[type][id]: version conflict, current version [1] is different than the one provided [2]",
        "index_uuid":"fMhefwt5S-m3NLiFhniBwA","shard":"4","index":"index"},"status":409}
    }
    */
}
