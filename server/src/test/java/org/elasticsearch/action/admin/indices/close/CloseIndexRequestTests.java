/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
                    if (out.getVersion().onOrAfter(Version.V_7_7_0) || request.indicesOptions().expandWildcardsHidden()) {
                        assertEquals(request.indicesOptions(), indicesOptions);
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
                if (out.getVersion().onOrAfter(Version.V_7_7_0) || sample.indicesOptions().expandWildcardsHidden()) {
                    assertEquals(sample.indicesOptions(), deserializedRequest.indicesOptions());
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
