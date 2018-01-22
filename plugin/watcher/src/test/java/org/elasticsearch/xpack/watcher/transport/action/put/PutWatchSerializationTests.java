/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.watcher.transport.action.put;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.watcher.transport.actions.put.PutWatchRequest;

import static org.hamcrest.Matchers.is;

public class PutWatchSerializationTests extends ESTestCase {

    // https://github.com/elastic/x-plugins/issues/2490
    public void testPutWatchSerialization() throws Exception {
        PutWatchRequest request = new PutWatchRequest();
        request.setId(randomAlphaOfLength(10));
        request.setActive(randomBoolean());
        request.setSource(
                new BytesArray(JsonXContent.contentBuilder().startObject().field("foo", randomAlphaOfLength(20)).endObject().string()),
                XContentType.JSON);

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        request.writeTo(streamOutput);

        PutWatchRequest readRequest = new PutWatchRequest(streamOutput.bytes().streamInput());
        assertThat(readRequest.isActive(), is(request.isActive()));
        assertThat(readRequest.getId(), is(request.getId()));
        assertThat(readRequest.getSource(), is(request.getSource()));
        assertThat(readRequest.xContentType(), is(request.xContentType()));
    }

    public void testPutWatchSerializationXContent() throws Exception {
        PutWatchRequest request = new PutWatchRequest();
        request.setId(randomAlphaOfLength(10));
        request.setActive(randomBoolean());
        request.setSource(
                new BytesArray(JsonXContent.contentBuilder().startObject().field("foo", randomAlphaOfLength(20)).endObject().string()),
                XContentType.JSON);
        assertEquals(XContentType.JSON, request.xContentType());

        BytesStreamOutput streamOutput = new BytesStreamOutput();
        request.writeTo(streamOutput);

        PutWatchRequest readRequest = new PutWatchRequest(streamOutput.bytes().streamInput());
        assertThat(readRequest.isActive(), is(request.isActive()));
        assertThat(readRequest.getId(), is(request.getId()));
        assertThat(readRequest.getSource(), is(request.getSource()));
        assertThat(readRequest.xContentType(), is(XContentType.JSON));
    }
}
