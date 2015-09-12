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

package org.elasticsearch.action.bulk;

import java.nio.charset.StandardCharsets;

import org.apache.lucene.util.Constants;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.script.Script;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class BulkRequestTests extends ESTestCase {

    @Test
    public void testSimpleBulk1() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk.json");
        // translate Windows line endings (\r\n) to standard ones (\n)
        if (Constants.WINDOWS) {
            bulkAction = Strings.replace(bulkAction, "\r\n", "\n");
        }
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, null);
        assertThat(bulkRequest.numberOfActions(), equalTo(3));
        assertThat(((IndexRequest) bulkRequest.requests().get(0)).source().toBytes(), equalTo(new BytesArray("{ \"field1\" : \"value1\" }").toBytes()));
        assertThat(bulkRequest.requests().get(1), instanceOf(DeleteRequest.class));
        assertThat(((IndexRequest) bulkRequest.requests().get(2)).source().toBytes(), equalTo(new BytesArray("{ \"field1\" : \"value3\" }").toBytes()));
    }

    @Test
    public void testSimpleBulk2() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk2.json");
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, null);
        assertThat(bulkRequest.numberOfActions(), equalTo(3));
    }

    @Test
    public void testSimpleBulk3() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk3.json");
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, null);
        assertThat(bulkRequest.numberOfActions(), equalTo(3));
    }

    @Test
    public void testSimpleBulk4() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk4.json");
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, null);
        assertThat(bulkRequest.numberOfActions(), equalTo(4));
        assertThat(((UpdateRequest) bulkRequest.requests().get(0)).id(), equalTo("1"));
        assertThat(((UpdateRequest) bulkRequest.requests().get(0)).retryOnConflict(), equalTo(2));
        assertThat(((UpdateRequest) bulkRequest.requests().get(0)).doc().source().toUtf8(), equalTo("{\"field\":\"value\"}"));
        assertThat(((UpdateRequest) bulkRequest.requests().get(1)).id(), equalTo("0"));
        assertThat(((UpdateRequest) bulkRequest.requests().get(1)).type(), equalTo("type1"));
        assertThat(((UpdateRequest) bulkRequest.requests().get(1)).index(), equalTo("index1"));
        Script script = ((UpdateRequest) bulkRequest.requests().get(1)).script();
        assertThat(script, notNullValue());
        assertThat(script.getScript(), equalTo("counter += param1"));
        assertThat(script.getLang(), equalTo("js"));
        Map<String, Object> scriptParams = script.getParams();
        assertThat(scriptParams, notNullValue());
        assertThat(scriptParams.size(), equalTo(1));
        assertThat(((Integer) scriptParams.get("param1")), equalTo(1));
        assertThat(((UpdateRequest) bulkRequest.requests().get(1)).upsertRequest().source().toUtf8(), equalTo("{\"counter\":1}"));
    }

    @Test
    public void testBulkAllowExplicitIndex() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk.json");
        try {
            new BulkRequest().add(new BytesArray(bulkAction.getBytes(StandardCharsets.UTF_8)), null, null, false);
            fail();
        } catch (Exception e) {

        }

        bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk5.json");
        new BulkRequest().add(new BytesArray(bulkAction.getBytes(StandardCharsets.UTF_8)), "test", null, false);
    }

    @Test
    public void testBulkAddIterable() {
        BulkRequest bulkRequest = Requests.bulkRequest();
        List<ActionRequest> requests = new ArrayList<>();
        requests.add(new IndexRequest("test", "test", "id").source("field", "value"));
        requests.add(new UpdateRequest("test", "test", "id").doc("field", "value"));
        requests.add(new DeleteRequest("test", "test", "id"));
        bulkRequest.add(requests);
        assertThat(bulkRequest.requests().size(), equalTo(3));
        assertThat(bulkRequest.requests().get(0), instanceOf(IndexRequest.class));
        assertThat(bulkRequest.requests().get(1), instanceOf(UpdateRequest.class));
        assertThat(bulkRequest.requests().get(2), instanceOf(DeleteRequest.class));
    }

    @Test
    public void testSimpleBulk6() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk6.json");
        BulkRequest bulkRequest = new BulkRequest();
        try {
            bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, null);
            fail("should have thrown an exception about the wrong format of line 1");
        } catch (IllegalArgumentException e) {
            assertThat("message contains error about the wrong format of line 1: " + e.getMessage(),
                    e.getMessage().contains("Malformed action/metadata line [1], expected a simple value for field [_source] but found [START_OBJECT]"), equalTo(true));
        }
    }

    @Test
    public void testSimpleBulk7() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk7.json");
        BulkRequest bulkRequest = new BulkRequest();
        try {
            bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, null);
            fail("should have thrown an exception about the wrong format of line 5");
        } catch (IllegalArgumentException e) {
            assertThat("message contains error about the wrong format of line 5: " + e.getMessage(),
                    e.getMessage().contains("Malformed action/metadata line [5], expected a simple value for field [_unkown] but found [START_ARRAY]"), equalTo(true));
        }
    }

    @Test
    public void testSimpleBulk8() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk8.json");
        BulkRequest bulkRequest = new BulkRequest();
        try {
            bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, null);
            fail("should have thrown an exception about the unknown paramater _foo");
        } catch (IllegalArgumentException e) {
            assertThat("message contains error about the unknown paramater _foo: " + e.getMessage(),
                    e.getMessage().contains("Action/metadata line [3] contains an unknown parameter [_foo]"), equalTo(true));
        }
    }

    @Test
    public void testSimpleBulk9() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk9.json");
        BulkRequest bulkRequest = new BulkRequest();
        try {
            bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, null);
            fail("should have thrown an exception about the wrong format of line 3");
        } catch (IllegalArgumentException e) {
            assertThat("message contains error about the wrong format of line 3: " + e.getMessage(),
                    e.getMessage().contains("Malformed action/metadata line [3], expected START_OBJECT or END_OBJECT but found [START_ARRAY]"), equalTo(true));
        }
    }

    @Test
    public void testSimpleBulk10() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk10.json");
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, null);
        assertThat(bulkRequest.numberOfActions(), equalTo(9));
    }
}
