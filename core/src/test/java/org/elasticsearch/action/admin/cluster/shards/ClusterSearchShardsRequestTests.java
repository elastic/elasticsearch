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

package org.elasticsearch.action.admin.cluster.shards;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

public class ClusterSearchShardsRequestTests extends ESTestCase {

    public void testSerialization() throws Exception {
        ClusterSearchShardsRequest request = new ClusterSearchShardsRequest();
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
                    IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean()));
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

        Version version = VersionUtils.randomVersionBetween(random(), Version.V_5_0_0, Version.CURRENT);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(version);
                ClusterSearchShardsRequest deserialized = new ClusterSearchShardsRequest();
                deserialized.readFrom(in);
                assertArrayEquals(request.indices(), deserialized.indices());
                assertSame(request.indicesOptions(), deserialized.indicesOptions());
                assertEquals(request.routing(), deserialized.routing());
                assertEquals(request.preference(), deserialized.preference());
            }
        }
    }

    public void testIndicesMustNotBeNull() {
        ClusterSearchShardsRequest request = new ClusterSearchShardsRequest();
        assertNotNull(request.indices());
        expectThrows(NullPointerException.class, () -> request.indices((String[])null));
        expectThrows(NullPointerException.class, () -> request.indices((String)null));
        expectThrows(NullPointerException.class, () -> request.indices(new String[]{"index1", null, "index3"}));
    }
}
