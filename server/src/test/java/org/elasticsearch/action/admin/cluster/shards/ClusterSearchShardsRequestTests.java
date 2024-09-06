/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.cluster.shards;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;

public class ClusterSearchShardsRequestTests extends ESTestCase {

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
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setTransportVersion(version);
                ClusterSearchShardsRequest deserialized = new ClusterSearchShardsRequest(in);
                assertArrayEquals(request.indices(), deserialized.indices());
                // indices options are not equivalent when sent to an older version and re-read due
                // to the addition of hidden indices as expand to hidden indices is always true when
                // read from a prior version
                if (version.onOrAfter(TransportVersions.V_7_7_0) || request.indicesOptions().expandWildcardsHidden()) {
                    assertEquals(request.indicesOptions(), deserialized.indicesOptions());
                }
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
}
