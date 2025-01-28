/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.close;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

public class CloseIndexRequestTests extends ESTestCase {

    public void testSerialization() throws Exception {
        final CloseIndexRequest request = randomRequest();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);

            final CloseIndexRequest deserializedRequest;
            try (StreamInput in = out.bytes().streamInput()) {
                deserializedRequest = new CloseIndexRequest(in);
            }
            assertEquals(request.ackTimeout(), deserializedRequest.ackTimeout());
            assertEquals(request.masterNodeTimeout(), deserializedRequest.masterNodeTimeout());
            assertEquals(request.indicesOptions(), deserializedRequest.indicesOptions());
            assertEquals(request.getParentTask(), deserializedRequest.getParentTask());
            assertEquals(request.waitForActiveShards(), deserializedRequest.waitForActiveShards());
            assertArrayEquals(request.indices(), deserializedRequest.indices());
        }
    }

    public void testBwcSerialization() throws Exception {
        {
            final CloseIndexRequest request = randomRequest();
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setTransportVersion(TransportVersionUtils.randomCompatibleVersion(random()));
                request.writeTo(out);

                try (StreamInput in = out.bytes().streamInput()) {
                    in.setTransportVersion(out.getTransportVersion());
                    assertEquals(request.getParentTask(), TaskId.readFromStream(in));
                    assertEquals(request.masterNodeTimeout(), in.readTimeValue());
                    if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                        assertEquals(request.masterTerm(), in.readVLong());
                    }
                    assertEquals(request.ackTimeout(), in.readTimeValue());
                    assertArrayEquals(request.indices(), in.readStringArray());
                    final IndicesOptions indicesOptions = IndicesOptions.readIndicesOptions(in);
                    assertEquals(request.indicesOptions(), indicesOptions);
                    assertEquals(request.waitForActiveShards(), ActiveShardCount.readFrom(in));
                }
            }
        }
        {
            final CloseIndexRequest sample = randomRequest();
            final TransportVersion version = TransportVersionUtils.randomCompatibleVersion(random());
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setTransportVersion(version);
                sample.getParentTask().writeTo(out);
                out.writeTimeValue(sample.masterNodeTimeout());
                if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_15_0)) {
                    out.writeVLong(sample.masterTerm());
                }
                out.writeTimeValue(sample.ackTimeout());
                out.writeStringArray(sample.indices());
                sample.indicesOptions().writeIndicesOptions(out);
                sample.waitForActiveShards().writeTo(out);

                final CloseIndexRequest deserializedRequest;
                try (StreamInput in = out.bytes().streamInput()) {
                    in.setTransportVersion(version);
                    deserializedRequest = new CloseIndexRequest(in);
                }
                assertEquals(sample.getParentTask(), deserializedRequest.getParentTask());
                assertEquals(sample.masterNodeTimeout(), deserializedRequest.masterNodeTimeout());
                assertEquals(sample.ackTimeout(), deserializedRequest.ackTimeout());
                assertArrayEquals(sample.indices(), deserializedRequest.indices());
                assertEquals(sample.indicesOptions(), deserializedRequest.indicesOptions());
                assertEquals(sample.waitForActiveShards(), deserializedRequest.waitForActiveShards());
            }
        }
    }

    private CloseIndexRequest randomRequest() {
        CloseIndexRequest request = new CloseIndexRequest();
        request.indices(generateRandomStringArray(10, 5, false, false));
        if (randomBoolean()) {
            request.indicesOptions(
                IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
            );
        }
        if (randomBoolean()) {
            request.ackTimeout(randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            request.masterNodeTimeout(randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            request.setParentTask(randomAlphaOfLength(5), randomNonNegativeLong());
        }
        if (randomBoolean()) {
            request.waitForActiveShards(
                randomFrom(ActiveShardCount.DEFAULT, ActiveShardCount.NONE, ActiveShardCount.ONE, ActiveShardCount.ALL)
            );
        }
        return request;
    }
}
