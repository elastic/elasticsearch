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

import java.util.EnumSet;

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
                out.setVersion(VersionUtils.randomCompatibleVersion(random(), Version.CURRENT));
                request.writeTo(out);

                try (StreamInput in = out.bytes().streamInput()) {
                    in.setVersion(out.getVersion());
                    assertEquals(request.getParentTask(), TaskId.readFromStream(in));
                    assertEquals(request.masterNodeTimeout(), in.readTimeValue());
                    assertEquals(request.timeout(), in.readTimeValue());
                    assertArrayEquals(request.indices(), in.readStringArray());
                    final IndicesOptions indicesOptions = IndicesOptions.readIndicesOptions(in);
                    // indices options are not equivalent when sent to an older version and re-read due
                    // to the addition of hidden indices as expand to hidden indices is always true when
                    // read from a prior version
                    // TODO update version on backport!

                    // Options changes in 8.0.0 (for now, fix on backport!)
                    if (out.getVersion().before(Version.V_8_0_0)) {
                        EnumSet<IndicesOptions.Option> expectedOptions = request.indicesOptions().getOptions();
                        expectedOptions.add(IndicesOptions.Option.ALLOW_SYSTEM_INDEX_ACCESS);
                        assertEquals(expectedOptions, indicesOptions.getOptions());
                    } else {
                        assertEquals(request.indicesOptions().getOptions(), indicesOptions.getOptions());
                    }

                    // Wildcard states changed in 7.7.0
                    if (out.getVersion().before(Version.V_7_7_0)) {
                        EnumSet<IndicesOptions.WildcardStates> expectedWildcardOptions = request.indicesOptions().getExpandWildcards();
                        expectedWildcardOptions.add(IndicesOptions.WildcardStates.HIDDEN);
                        assertEquals(expectedWildcardOptions, indicesOptions.getExpandWildcards());
                    } else {
                        assertEquals(request.indicesOptions().getExpandWildcards(), indicesOptions.getExpandWildcards());
                    }

                    if (in.getVersion().onOrAfter(Version.V_7_2_0)) {
                        assertEquals(request.waitForActiveShards(), ActiveShardCount.readFrom(in));
                    } else {
                        assertEquals(0, in.available());
                    }
                }
            }
        }
        {
            final CloseIndexRequest sample = randomRequest();
            final Version version = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT);
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(version);
                sample.getParentTask().writeTo(out);
                out.writeTimeValue(sample.masterNodeTimeout());
                out.writeTimeValue(sample.timeout());
                out.writeStringArray(sample.indices());
                sample.indicesOptions().writeIndicesOptions(out);
                if (out.getVersion().onOrAfter(Version.V_7_2_0)) {
                    sample.waitForActiveShards().writeTo(out);
                }

                final CloseIndexRequest deserializedRequest;
                try (StreamInput in = out.bytes().streamInput()) {
                    in.setVersion(version);
                    deserializedRequest = new CloseIndexRequest(in);
                }
                assertEquals(sample.getParentTask(), deserializedRequest.getParentTask());
                assertEquals(sample.masterNodeTimeout(), deserializedRequest.masterNodeTimeout());
                assertEquals(sample.timeout(), deserializedRequest.timeout());
                assertArrayEquals(sample.indices(), deserializedRequest.indices());
                // indices options are not equivalent when sent to an older version and re-read due
                // to the addition of hidden indices as expand to hidden indices is always true when
                // read from a prior version
                // TODO change version on backport

                // Options changes in 8.0.0 (for now, fix on backport!)
                if (out.getVersion().before(Version.V_8_0_0)) {
                    EnumSet<IndicesOptions.Option> expectedOptions = sample.indicesOptions().getOptions();
                    expectedOptions.add(IndicesOptions.Option.ALLOW_SYSTEM_INDEX_ACCESS);
                    assertEquals(expectedOptions, deserializedRequest.indicesOptions().getOptions());
                } else {
                    assertEquals(sample.indicesOptions().getOptions(), deserializedRequest.indicesOptions().getOptions());
                }

                // Wildcard states changed in 7.7.0
                if (out.getVersion().before(Version.V_7_7_0)) {
                    EnumSet<IndicesOptions.WildcardStates> expectedWildcardOptions =
                        deserializedRequest.indicesOptions().getExpandWildcards();
                    expectedWildcardOptions.add(IndicesOptions.WildcardStates.HIDDEN);
                    assertEquals(expectedWildcardOptions, deserializedRequest.indicesOptions().getExpandWildcards());
                } else {
                    assertEquals(sample.indicesOptions().getExpandWildcards(), deserializedRequest.indicesOptions().getExpandWildcards());
                }

                if (out.getVersion().onOrAfter(Version.V_7_2_0)) {
                    assertEquals(sample.waitForActiveShards(), deserializedRequest.waitForActiveShards());
                } else {
                    assertEquals(ActiveShardCount.NONE, deserializedRequest.waitForActiveShards());
                }
            }
        }
    }

    private CloseIndexRequest randomRequest() {
        CloseIndexRequest request = new CloseIndexRequest();
        request.indices(generateRandomStringArray(10, 5, false, false));
        if (randomBoolean()) {
            request.indicesOptions(
                IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
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
