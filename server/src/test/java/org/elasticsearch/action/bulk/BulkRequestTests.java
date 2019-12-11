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

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest.RefreshPolicy;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.Script;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.StreamsUtils.copyToStringFromClasspath;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class BulkRequestTests extends ESTestCase {
    public void testSimpleBulk1() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk.json");
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, XContentType.JSON);
        assertThat(bulkRequest.numberOfActions(), equalTo(3));
        assertThat(((IndexRequest) bulkRequest.requests().get(0)).source(), equalTo(new BytesArray("{ \"field1\" : \"value1\" }")));
        assertThat(bulkRequest.requests().get(1), instanceOf(DeleteRequest.class));
        assertThat(((IndexRequest) bulkRequest.requests().get(2)).source(), equalTo(new BytesArray("{ \"field1\" : \"value3\" }")));
    }

    public void testSimpleBulkWithCarriageReturn() throws Exception {
        String bulkAction = "{ \"index\":{\"_index\":\"test\",\"_id\":\"1\"} }\r\n{ \"field1\" : \"value1\" }\r\n";
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, XContentType.JSON);
        assertThat(bulkRequest.numberOfActions(), equalTo(1));
        assertThat(((IndexRequest) bulkRequest.requests().get(0)).source(), equalTo(new BytesArray("{ \"field1\" : \"value1\" }")));
        Map<String, Object> sourceMap = XContentHelper.convertToMap(((IndexRequest) bulkRequest.requests().get(0)).source(),
            false, XContentType.JSON).v2();
        assertEquals("value1", sourceMap.get("field1"));
    }

    public void testSimpleBulk2() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk2.json");
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, XContentType.JSON);
        assertThat(bulkRequest.numberOfActions(), equalTo(3));
    }

    public void testSimpleBulk3() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk3.json");
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, XContentType.JSON);
        assertThat(bulkRequest.numberOfActions(), equalTo(3));
    }

    public void testSimpleBulk4() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk4.json");
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, XContentType.JSON);
        assertThat(bulkRequest.numberOfActions(), equalTo(4));
        assertThat(bulkRequest.requests().get(0).id(), equalTo("1"));
        assertThat(((UpdateRequest) bulkRequest.requests().get(0)).retryOnConflict(), equalTo(2));
        assertThat(((UpdateRequest) bulkRequest.requests().get(0)).doc().source().utf8ToString(), equalTo("{\"field\":\"value\"}"));
        assertThat(bulkRequest.requests().get(1).id(), equalTo("0"));
        assertThat(bulkRequest.requests().get(1).index(), equalTo("index1"));
        Script script = ((UpdateRequest) bulkRequest.requests().get(1)).script();
        assertThat(script, notNullValue());
        assertThat(script.getIdOrCode(), equalTo("counter += param1"));
        assertThat(script.getLang(), equalTo("javascript"));
        Map<String, Object> scriptParams = script.getParams();
        assertThat(scriptParams, notNullValue());
        assertThat(scriptParams.size(), equalTo(1));
        assertThat(scriptParams.get("param1"), equalTo(1));
        assertThat(((UpdateRequest) bulkRequest.requests().get(1)).upsertRequest().source().utf8ToString(), equalTo("{\"counter\":1}"));
    }

    public void testBulkAllowExplicitIndex() throws Exception {
        String bulkAction1 = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk.json");
        Exception ex = expectThrows(Exception.class,
            () -> new BulkRequest().add(
                new BytesArray(bulkAction1.getBytes(StandardCharsets.UTF_8)), null, false, XContentType.JSON));
        assertEquals("explicit index in bulk is not allowed", ex.getMessage());

        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk5.json");
        new BulkRequest().add(new BytesArray(bulkAction.getBytes(StandardCharsets.UTF_8)), "test", false, XContentType.JSON);
    }

    public void testBulkAddIterable() {
        BulkRequest bulkRequest = Requests.bulkRequest();
        List<DocWriteRequest<?>> requests = new ArrayList<>();
        requests.add(new IndexRequest("test").id("id").source(Requests.INDEX_CONTENT_TYPE, "field", "value"));
        requests.add(new UpdateRequest("test", "id").doc(Requests.INDEX_CONTENT_TYPE, "field", "value"));
        requests.add(new DeleteRequest("test", "id"));
        bulkRequest.add(requests);
        assertThat(bulkRequest.requests().size(), equalTo(3));
        assertThat(bulkRequest.requests().get(0), instanceOf(IndexRequest.class));
        assertThat(bulkRequest.requests().get(1), instanceOf(UpdateRequest.class));
        assertThat(bulkRequest.requests().get(2), instanceOf(DeleteRequest.class));
    }

    public void testSimpleBulk6() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk6.json");
        BulkRequest bulkRequest = new BulkRequest();
        ParsingException exc = expectThrows(ParsingException.class,
            () -> bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, XContentType.JSON));
        assertThat(exc.getMessage(), containsString("Unknown key for a VALUE_STRING in [hello]"));
    }

    public void testSimpleBulk7() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk7.json");
        BulkRequest bulkRequest = new BulkRequest();
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class,
            () -> bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, XContentType.JSON));
        assertThat(exc.getMessage(),
            containsString("Malformed action/metadata line [5], expected a simple value for field [_unknown] but found [START_ARRAY]"));
    }

    public void testSimpleBulk8() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk8.json");
        BulkRequest bulkRequest = new BulkRequest();
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class,
            () -> bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, XContentType.JSON));
        assertThat(exc.getMessage(), containsString("Action/metadata line [3] contains an unknown parameter [_foo]"));
    }

    public void testSimpleBulk9() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk9.json");
        BulkRequest bulkRequest = new BulkRequest();
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class,
            () -> bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, XContentType.JSON));
        assertThat(exc.getMessage(),
            containsString("Malformed action/metadata line [3], expected START_OBJECT or END_OBJECT but found [START_ARRAY]"));
    }

    public void testSimpleBulk10() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk10.json");
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, XContentType.JSON);
        assertThat(bulkRequest.numberOfActions(), equalTo(9));
    }

    public void testBulkActionShouldNotContainArray() throws Exception {
        String bulkAction = "{ \"index\":{\"_index\":[\"index1\", \"index2\"],\"_id\":\"1\"} }\r\n"
            + "{ \"field1\" : \"value1\" }\r\n";
        BulkRequest bulkRequest = new BulkRequest();
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class,
            () -> bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, XContentType.JSON));
        assertEquals(exc.getMessage(), "Malformed action/metadata line [1]" +
            ", expected a simple value for field [_index] but found [START_ARRAY]");
    }

    public void testBulkEmptyObject() throws Exception {
        String bulkIndexAction = "{ \"index\":{\"_index\":\"test\",\"_id\":\"1\"} }\r\n";
        String bulkIndexSource = "{ \"field1\" : \"value1\" }\r\n";
        String emptyObject = "{}\r\n";
        StringBuilder bulk = new StringBuilder();
        int emptyLine;
        if (randomBoolean()) {
            bulk.append(emptyObject);
            emptyLine = 1;
        } else {
            int actions = randomIntBetween(1, 10);
            int emptyAction = randomIntBetween(1, actions);
            emptyLine = emptyAction * 2 - 1;
            for (int i = 1; i <= actions; i++) {
                bulk.append(i == emptyAction ? emptyObject : bulkIndexAction);
                bulk.append(randomBoolean() ? emptyObject : bulkIndexSource);
            }
        }
        String bulkAction = bulk.toString();
        BulkRequest bulkRequest = new BulkRequest();
        IllegalArgumentException exc = expectThrows(IllegalArgumentException.class,
            () -> bulkRequest.add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, XContentType.JSON));
        assertThat(exc.getMessage(), containsString("Malformed action/metadata line ["
            + emptyLine + "], expected FIELD_NAME but found [END_OBJECT]"));
    }

    // issue 7361
    public void testBulkRequestWithRefresh() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        // We force here a "id is missing" validation error
        bulkRequest.add(new DeleteRequest("index", null).setRefreshPolicy(RefreshPolicy.IMMEDIATE));
        bulkRequest.add(new DeleteRequest("index", "id").setRefreshPolicy(RefreshPolicy.IMMEDIATE));
        bulkRequest.add(new UpdateRequest("index", "id").doc("{}", XContentType.JSON).setRefreshPolicy(RefreshPolicy.IMMEDIATE));
        bulkRequest.add(new IndexRequest("index").id("id").source("{}", XContentType.JSON).setRefreshPolicy(RefreshPolicy.IMMEDIATE));
        ActionRequestValidationException validate = bulkRequest.validate();
        assertThat(validate, notNullValue());
        assertThat(validate.validationErrors(), not(empty()));
        assertThat(validate.validationErrors(), contains(
                "RefreshPolicy is not supported on an item request. Set it on the BulkRequest instead.",
                "id is missing",
                "RefreshPolicy is not supported on an item request. Set it on the BulkRequest instead.",
                "RefreshPolicy is not supported on an item request. Set it on the BulkRequest instead.",
                "RefreshPolicy is not supported on an item request. Set it on the BulkRequest instead."));
    }

    // issue 15120
    public void testBulkNoSource() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new UpdateRequest("index", "id"));
        bulkRequest.add(new IndexRequest("index").id("id"));
        ActionRequestValidationException validate = bulkRequest.validate();
        assertThat(validate, notNullValue());
        assertThat(validate.validationErrors(), not(empty()));
        assertThat(validate.validationErrors(), contains(
                "script or doc is missing",
                "source is missing",
                "content type is missing"));
    }

    public void testCannotAddNullRequests() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        expectThrows(NullPointerException.class, () -> bulkRequest.add((IndexRequest) null));
        expectThrows(NullPointerException.class, () -> bulkRequest.add((UpdateRequest) null));
        expectThrows(NullPointerException.class, () -> bulkRequest.add((DeleteRequest) null));
    }

    public void testSmileIsSupported() throws IOException {
        XContentType xContentType = XContentType.SMILE;
        BytesReference data;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            try(XContentBuilder builder = XContentFactory.contentBuilder(xContentType, out)) {
                builder.startObject();
                builder.startObject("index");
                builder.field("_index", "index");
                builder.field("_id", "test");
                builder.endObject();
                builder.endObject();
            }
            out.write(xContentType.xContent().streamSeparator());
            try(XContentBuilder builder = XContentFactory.contentBuilder(xContentType, out)) {
                builder.startObject();
                builder.field("field", "value");
                builder.endObject();
            }
            out.write(xContentType.xContent().streamSeparator());
            data = out.bytes();
        }

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(data, null, xContentType);
        assertEquals(1, bulkRequest.requests().size());
        DocWriteRequest<?> docWriteRequest = bulkRequest.requests().get(0);
        assertEquals(DocWriteRequest.OpType.INDEX, docWriteRequest.opType());
        assertEquals("index", docWriteRequest.index());
        assertEquals("test", docWriteRequest.id());
        assertThat(docWriteRequest, instanceOf(IndexRequest.class));
        IndexRequest request = (IndexRequest) docWriteRequest;
        assertEquals(1, request.sourceAsMap().size());
        assertEquals("value", request.sourceAsMap().get("field"));
    }

    public void testToValidateUpsertRequestAndCASInBulkRequest() throws IOException {
        XContentType xContentType = XContentType.SMILE;
        BytesReference data;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            try (XContentBuilder builder = XContentFactory.contentBuilder(xContentType, out)) {
                builder.startObject();
                builder.startObject("update");
                builder.field("_index", "index");
                builder.field("_id", "id");
                builder.field("if_seq_no", 1L);
                builder.field("if_primary_term", 100L);
                builder.endObject();
                builder.endObject();
            }
            out.write(xContentType.xContent().streamSeparator());
            try(XContentBuilder builder = XContentFactory.contentBuilder(xContentType, out)) {
                builder.startObject();
                builder.startObject("doc").endObject();
                Map<String,Object> values = new HashMap<>();
                values.put("if_seq_no", 1L);
                values.put("if_primary_term", 100L);
                values.put("_index", "index");
                builder.field("upsert", values);
                builder.endObject();
            }
            out.write(xContentType.xContent().streamSeparator());
            data = out.bytes();
        }
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(data, null, xContentType);
        assertThat(bulkRequest.validate().validationErrors(), contains("upsert requests don't support `if_seq_no` and `if_primary_term`"));
    }

    public void testBulkTerminatedByNewline() throws Exception {
        String bulkAction = copyToStringFromClasspath("/org/elasticsearch/action/bulk/simple-bulk11.json");
        IllegalArgumentException expectThrows = expectThrows(IllegalArgumentException.class, () -> new BulkRequest()
                .add(bulkAction.getBytes(StandardCharsets.UTF_8), 0, bulkAction.length(), null, XContentType.JSON));
        assertEquals("The bulk request must be terminated by a newline [\\n]", expectThrows.getMessage());

        String bulkActionWithNewLine = bulkAction + "\n";
        BulkRequest bulkRequestWithNewLine = new BulkRequest();
        bulkRequestWithNewLine.add(bulkActionWithNewLine.getBytes(StandardCharsets.UTF_8), 0, bulkActionWithNewLine.length(), null,
                XContentType.JSON);
        assertEquals(3, bulkRequestWithNewLine.numberOfActions());
    }
}
