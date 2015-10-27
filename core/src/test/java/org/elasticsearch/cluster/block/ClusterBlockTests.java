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

package org.elasticsearch.cluster.block;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.hamcrest.CoreMatchers.equalTo;

public class ClusterBlockTests extends ESTestCase {
    public void testSerialization() throws Exception {
        int iterations = randomIntBetween(10, 100);
        for (int i = 0; i < iterations; i++) {
            // Get a random version
            Version version = randomVersion(random());

            // Get a random list of ClusterBlockLevels
            EnumSet<ClusterBlockLevel> levels = EnumSet.noneOf(ClusterBlockLevel.class);
            int nbLevels = randomIntBetween(1, ClusterBlockLevel.values().length);
            for (int j = 0; j < nbLevels; j++) {
                levels.add(randomFrom(ClusterBlockLevel.values()));
            }

            ClusterBlock clusterBlock = new ClusterBlock(randomInt(), "cluster block #" + randomInt(), randomBoolean(),
                    randomBoolean(), randomFrom(RestStatus.values()), levels);

            BytesStreamOutput out = new BytesStreamOutput();
            out.setVersion(version);
            clusterBlock.writeTo(out);

            StreamInput in = StreamInput.wrap(out.bytes());
            in.setVersion(version);
            ClusterBlock result = ClusterBlock.readClusterBlock(in);

            assertThat(result.id(), equalTo(clusterBlock.id()));
            assertThat(result.status(), equalTo(clusterBlock.status()));
            assertThat(result.description(), equalTo(clusterBlock.description()));
            assertThat(result.retryable(), equalTo(clusterBlock.retryable()));
            assertThat(result.disableStatePersistence(), equalTo(clusterBlock.disableStatePersistence()));
            assertArrayEquals(result.levels().toArray(), clusterBlock.levels().toArray());
        }
    }

    public void testToXContent() throws IOException {
        for (ToXContent.Params params : Arrays.asList(null, ToXContent.EMPTY_PARAMS, toParams("keyed", Boolean.TRUE.toString()))) {
            assertEquals("{\n" +
                    "  \"blocks\" : {\n" +
                    "    \"0\" : {\n" +
                    "      \"description\" : \"cluster block #0\",\n" +
                    "      \"retryable\" : true,\n" +
                    "      \"levels\" : [ \"read\", \"write\", \"metadata_read\", \"metadata_write\" ]\n" +
                    "    },\n" +
                    "    \"1\" : {\n" +
                    "      \"description\" : \"cluster block #1\",\n" +
                    "      \"retryable\" : true,\n" +
                    "      \"levels\" : [ \"read\", \"write\", \"metadata_read\", \"metadata_write\" ]\n" +
                    "    }\n" +
                    "  }\n" +
                    "}", buildClusterBlocks(params));
        }
    }

    public void testToXContentWithKeyedSetToFalse() throws IOException {
        // With keyed=false parameter, the blocks ids are moved to a sub field "id"
        assertEquals("{\n" +
                "  \"blocks\" : [ {\n" +
                "    \"id\" : \"0\",\n" +
                "    \"description\" : \"cluster block #0\",\n" +
                "    \"retryable\" : true,\n" +
                "    \"levels\" : [ \"read\", \"write\", \"metadata_read\", \"metadata_write\" ]\n" +
                "  }, {\n" +
                "    \"id\" : \"1\",\n" +
                "    \"description\" : \"cluster block #1\",\n" +
                "    \"retryable\" : true,\n" +
                "    \"levels\" : [ \"read\", \"write\", \"metadata_read\", \"metadata_write\" ]\n" +
                "  } ]\n" +
                "}", buildClusterBlocks(toParams("keyed", Boolean.FALSE.toString())));
    }

    private String buildClusterBlocks(ToXContent.Params params) throws IOException {
        try (XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON).prettyPrint()) {
            builder.startObject();
            builder.start("blocks", params);
            for (int i = 0; i < 2; i++) {
                ClusterBlock clusterBlock = new ClusterBlock(i, "cluster block #" + String.valueOf(i), true, false, RestStatus.OK, ClusterBlockLevel.ALL);
                clusterBlock.toXContent(builder, params);
            }
            builder.end(params);
            builder.endObject();
            return builder.string().trim();
        }
    }

    private ToXContent.Params toParams(String key, String value) {
        Map<String, String> params = new HashMap<>();
        params.put(key, value);
        return new ToXContent.MapParams(params);
    }
}
