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
package org.elasticsearch.action.index;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class IndexRequestTests extends ESTestCase {
    public void testIndexRequestOpTypeFromString() throws Exception {
        String create = "create";
        String index = "index";
        String createUpper = "CREATE";
        String indexUpper = "INDEX";

        IndexRequest indexRequest = new IndexRequest("");
        indexRequest.opType(create);
        assertThat(indexRequest.opType(), equalTo(DocWriteRequest.OpType.CREATE));
        indexRequest.opType(createUpper);
        assertThat(indexRequest.opType(), equalTo(DocWriteRequest.OpType.CREATE));
        indexRequest.opType(index);
        assertThat(indexRequest.opType(), equalTo(DocWriteRequest.OpType.INDEX));
        indexRequest.opType(indexUpper);
        assertThat(indexRequest.opType(), equalTo(DocWriteRequest.OpType.INDEX));
    }

    public void testReadIncorrectOpType() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new IndexRequest("").opType("foobar"));
        assertThat(e.getMessage(), equalTo("opType must be 'create' or 'index', found: [foobar]"));
    }

    public void testCreateOperationRejectsVersions() {
        Set<VersionType> allButInternalSet = new HashSet<>(Arrays.asList(VersionType.values()));
        allButInternalSet.remove(VersionType.INTERNAL);
        VersionType[] allButInternal = allButInternalSet.toArray(new VersionType[]{});
        IndexRequest request = new IndexRequest("index").id("1");
        request.opType(IndexRequest.OpType.CREATE);
        request.versionType(randomFrom(allButInternal));
        assertThat(request.validate().validationErrors(), not(empty()));

        request.versionType(VersionType.INTERNAL);
        request.version(randomIntBetween(0, Integer.MAX_VALUE));
        assertThat(request.validate().validationErrors(), not(empty()));
    }

    public void testIndexingRejectsLongIds() {
        String id = randomAlphaOfLength(511);
        IndexRequest request = new IndexRequest("index").id( id);
        request.source("{}", XContentType.JSON);
        ActionRequestValidationException validate = request.validate();
        assertNull(validate);

        id = randomAlphaOfLength(512);
        request = new IndexRequest("index").id( id);
        request.source("{}", XContentType.JSON);
        validate = request.validate();
        assertNull(validate);

        id = randomAlphaOfLength(513);
        request = new IndexRequest("index").id( id);
        request.source("{}", XContentType.JSON);
        validate = request.validate();
        assertThat(validate, notNullValue());
        assertThat(validate.getMessage(),
                containsString("id [" + id + "] is too long, must be no longer than 512 bytes but was: 513"));
    }

    public void testWaitForActiveShards() {
        IndexRequest request = new IndexRequest("index");
        final int count = randomIntBetween(0, 10);
        request.waitForActiveShards(ActiveShardCount.from(count));
        assertEquals(request.waitForActiveShards(), ActiveShardCount.from(count));
        // test negative shard count value not allowed
        expectThrows(IllegalArgumentException.class, () -> request.waitForActiveShards(ActiveShardCount.from(randomIntBetween(-10, -1))));
    }

    public void testAutoGenIdTimestampIsSet() {
        IndexRequest request = new IndexRequest("index");
        request.process(Version.CURRENT, null, "index");
        assertTrue("expected > 0 but got: " + request.getAutoGeneratedTimestamp(), request.getAutoGeneratedTimestamp() > 0);
        request = new IndexRequest("index").id("1");
        request.process(Version.CURRENT, null, "index");
        assertEquals(IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, request.getAutoGeneratedTimestamp());
    }

    public void testIndexResponse() {
        ShardId shardId = new ShardId(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10), randomIntBetween(0, 1000));
        String id = randomAlphaOfLengthBetween(3, 10);
        long version = randomLong();
        boolean created = randomBoolean();
        IndexResponse indexResponse = new IndexResponse(shardId, id, SequenceNumbers.UNASSIGNED_SEQ_NO, 0, version, created);
        int total = randomIntBetween(1, 10);
        int successful = randomIntBetween(1, 10);
        ReplicationResponse.ShardInfo shardInfo = new ReplicationResponse.ShardInfo(total, successful);
        indexResponse.setShardInfo(shardInfo);
        boolean forcedRefresh = false;
        if (randomBoolean()) {
            forcedRefresh = randomBoolean();
            indexResponse.setForcedRefresh(forcedRefresh);
        }
        assertEquals(id, indexResponse.getId());
        assertEquals(version, indexResponse.getVersion());
        assertEquals(shardId, indexResponse.getShardId());
        assertEquals(created ? RestStatus.CREATED : RestStatus.OK, indexResponse.status());
        assertEquals(total, indexResponse.getShardInfo().getTotal());
        assertEquals(successful, indexResponse.getShardInfo().getSuccessful());
        assertEquals(forcedRefresh, indexResponse.forcedRefresh());
        assertEquals("IndexResponse[index=" + shardId.getIndexName() + ",id="+ id +
                ",version=" + version + ",result=" + (created ? "created" : "updated") +
                ",seqNo=" + SequenceNumbers.UNASSIGNED_SEQ_NO +
                ",primaryTerm=" + 0 +
                ",shards={\"total\":" + total + ",\"successful\":" + successful + ",\"failed\":0}]",
                indexResponse.toString());
    }

    public void testIndexRequestXContentSerialization() throws IOException {
        IndexRequest indexRequest = new IndexRequest("foo").id("1");
        indexRequest.source("{}", XContentType.JSON);
        assertEquals(XContentType.JSON, indexRequest.getContentType());

        BytesStreamOutput out = new BytesStreamOutput();
        indexRequest.writeTo(out);
        StreamInput in = StreamInput.wrap(out.bytes().toBytesRef().bytes);
        IndexRequest serialized = new IndexRequest(in);
        assertEquals(XContentType.JSON, serialized.getContentType());
        assertEquals(new BytesArray("{}"), serialized.source());
    }

    // reindex makes use of index requests without a source so this needs to be handled
    public void testSerializationOfEmptyRequestWorks() throws IOException {
        IndexRequest request = new IndexRequest("index");
        assertNull(request.getContentType());
        assertEquals("index", request.index());

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                IndexRequest serialized = new IndexRequest(in);
                assertNull(serialized.getContentType());
                assertEquals("index", serialized.index());
            }
        }
    }

    public void testToStringSizeLimit() throws UnsupportedEncodingException {
        IndexRequest request = new IndexRequest("index");

        String source = "{\"name\":\"value\"}";
        request.source(source, XContentType.JSON);
        assertEquals("index {[index][null], source[" + source + "]}", request.toString());

        source = "{\"name\":\"" + randomUnicodeOfLength(IndexRequest.MAX_SOURCE_LENGTH_IN_TOSTRING) + "\"}";
        request.source(source, XContentType.JSON);
        int actualBytes = source.getBytes("UTF-8").length;
        assertEquals("index {[index][null], source[n/a, actual length: [" + new ByteSizeValue(actualBytes).toString() +
                "], max length: " + new ByteSizeValue(IndexRequest.MAX_SOURCE_LENGTH_IN_TOSTRING).toString() + "]}", request.toString());
    }

    public void testRejectsEmptyStringPipeline() {
        IndexRequest request = new IndexRequest("index");
        request.source("{}", XContentType.JSON);
        request.setPipeline("");
        ActionRequestValidationException validate = request.validate();
        assertThat(validate, notNullValue());
        assertThat(validate.getMessage(),
            containsString("pipeline cannot be an empty string"));
    }
}
