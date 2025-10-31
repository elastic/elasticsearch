/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.cluster.shards;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.MasterNodeReadRequest;
import org.elasticsearch.action.support.master.TransportMasterNodeReadAction;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.UpdateForV10;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

import java.io.IOException;

public class ClusterSearchShardsRequestTests extends ESTestCase {

    /**
     * AP prior to 9.3 {@link TransportClusterSearchShardsAction} was a {@link TransportMasterNodeReadAction}
     * so for BwC we must remain able to read these requests until we no longer need to support calling this action remotely.
     * This test method can be removed once we no longer need to keep serialization and deserialization around.
     */
    @UpdateForV10(owner = UpdateForV10.Owner.DISTRIBUTED_COORDINATION)
    public void testSerialization() throws Exception {
        ClusterSearchShardsRequest request = new ClusterSearchShardsRequest(TEST_REQUEST_TIMEOUT);
        if (randomBoolean()) {
            int numIndices = randomIntBetween(1, 5);
            String[] indices = new String[numIndices];
            for (int i = 0; i < numIndices; i++) {
                indices[i] = randomAlphaOfLengthBetween(3, 10);
            }
            request.indices(indices);
        }
        if (randomBoolean()) {
            request.indicesOptions(
                IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
            );
        }
        if (randomBoolean()) {
            request.preference(randomAlphaOfLengthBetween(3, 10));
        }
        if (randomBoolean()) {
            int numRoutings = randomIntBetween(1, 3);
            String[] routings = new String[numRoutings];
            for (int i = 0; i < numRoutings; i++) {
                routings[i] = randomAlphaOfLengthBetween(3, 10);
            }
            request.routing(routings);
        }

        TransportVersion version = TransportVersionUtils.randomCompatibleVersion(random());
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(version);
            new WriteToWrapper(request).writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(version);
                ClusterSearchShardsRequest deserialized = new ClusterSearchShardsRequest(in);
                assertArrayEquals(request.indices(), deserialized.indices());
                assertEquals(request.indicesOptions(), deserialized.indicesOptions());
                assertEquals(request.routing(), deserialized.routing());
                assertEquals(request.preference(), deserialized.preference());
            }
        }
    }

    public void testIndicesMustNotBeNull() {
        ClusterSearchShardsRequest request = new ClusterSearchShardsRequest(TEST_REQUEST_TIMEOUT);
        assertNotNull(request.indices());
        expectThrows(NullPointerException.class, () -> request.indices((String[]) null));
        expectThrows(NullPointerException.class, () -> request.indices((String) null));
        expectThrows(NullPointerException.class, () -> request.indices(new String[] { "index1", null, "index3" }));
    }

    /**
     * AP prior to 9.3 {@link TransportClusterSearchShardsAction} was a {@link TransportMasterNodeReadAction}
     * so for BwC we must remain able to read these requests until we no longer need to support calling this action remotely.
     * This class preserves the {@link Writeable#writeTo(StreamOutput)} functionality of the request so that
     * the necessary deserialization can be tested.
     */
    private static class WriteToWrapper extends MasterNodeReadRequest<WriteToWrapper> {
        private final ClusterSearchShardsRequest request;

        WriteToWrapper(ClusterSearchShardsRequest request) {
            super(request.masterTimeout());
            this.request = request;
        }

        @Override
        public ActionRequestValidationException validate() {
            return null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeStringArray(request.indices());
            out.writeOptionalString(request.routing());
            out.writeOptionalString(request.preference());
            request.indicesOptions().writeIndicesOptions(out);
        }
    }
}
