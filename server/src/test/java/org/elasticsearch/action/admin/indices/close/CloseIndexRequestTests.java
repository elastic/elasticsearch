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

package org.elasticsearch.action.admin.indices.close;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

public class CloseIndexRequestTests extends ESTestCase {

    public void testSerialization() throws Exception {
        final CloseIndexRequest request = randomRequest();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);

            final CloseIndexRequest deserializedRequest;
            try (StreamInput in = out.bytes().streamInput()) {
                deserializedRequest = new CloseIndexRequest(in);
            }
            assertEquals(request.timeout(), deserializedRequest.timeout());
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
                out.setVersion(VersionUtils.randomVersionBetween(random(), VersionUtils.getFirstVersion(),
                    VersionUtils.getPreviousVersion(Version.V_7_2_0)));
                request.writeTo(out);

                try (StreamInput in = out.bytes().streamInput()) {
                    assertEquals(request.getParentTask(), TaskId.readFromStream(in));
                    assertEquals(request.masterNodeTimeout(), in.readTimeValue());
                    assertEquals(request.timeout(), in.readTimeValue());
                    assertArrayEquals(request.indices(), in.readStringArray());
                    assertEquals(request.indicesOptions(), IndicesOptions.readIndicesOptions(in));
                }
            }
        }
        {
            final CloseIndexRequest sample = randomRequest();
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                sample.getParentTask().writeTo(out);
                out.writeTimeValue(sample.masterNodeTimeout());
                out.writeTimeValue(sample.timeout());
                out.writeStringArray(sample.indices());
                sample.indicesOptions().writeIndicesOptions(out);

                final CloseIndexRequest deserializedRequest;
                try (StreamInput in = out.bytes().streamInput()) {
                    in.setVersion(VersionUtils.randomVersionBetween(random(), VersionUtils.getFirstVersion(),
                        VersionUtils.getPreviousVersion(Version.V_7_2_0)));
                    deserializedRequest = new CloseIndexRequest(in);
                }
                assertEquals(sample.getParentTask(), deserializedRequest.getParentTask());
                assertEquals(sample.masterNodeTimeout(), deserializedRequest.masterNodeTimeout());
                assertEquals(sample.timeout(), deserializedRequest.timeout());
                assertArrayEquals(sample.indices(), deserializedRequest.indices());
                assertEquals(sample.indicesOptions(), deserializedRequest.indicesOptions());
                assertEquals(ActiveShardCount.NONE, deserializedRequest.waitForActiveShards());
            }
        }
    }

    private CloseIndexRequest randomRequest() {
        CloseIndexRequest request = new CloseIndexRequest();
        request.indices(generateRandomStringArray(10, 5, false, false));
        if (randomBoolean()) {
            request.indicesOptions(IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
        }
        if (randomBoolean()) {
            request.timeout(randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            request.masterNodeTimeout(randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            request.setParentTask(randomAlphaOfLength(5), randomNonNegativeLong());
        }
        if (randomBoolean()) {
            request.waitForActiveShards(randomFrom(ActiveShardCount.DEFAULT, ActiveShardCount.NONE, ActiveShardCount.ONE,
                ActiveShardCount.ALL));
        }
        return request;
    }
}
