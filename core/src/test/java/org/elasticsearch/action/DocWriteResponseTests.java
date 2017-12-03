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

package org.elasticsearch.action;

import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class DocWriteResponseTests extends ESTestCase {
    public void testGetLocation() {
        final DocWriteResponse response =
                new DocWriteResponse(
                        new ShardId("index", "uuid", 0),
                        "type",
                        "id",
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        17,
                        0,
                        Result.CREATED) {};
        assertEquals("/index/type/id", response.getLocation(null));
        assertEquals("/index/type/id?routing=test_routing", response.getLocation("test_routing"));
    }

    public void testGetLocationNonAscii() {
        final DocWriteResponse response =
                new DocWriteResponse(
                        new ShardId("index", "uuid", 0),
                        "type",
                        "❤",
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        17,
                        0,
                        Result.CREATED) {};
        assertEquals("/index/type/%E2%9D%A4", response.getLocation(null));
        assertEquals("/index/type/%E2%9D%A4?routing=%C3%A4", response.getLocation("ä"));
    }

    public void testGetLocationWithSpaces() {
        final DocWriteResponse response =
                new DocWriteResponse(
                        new ShardId("index", "uuid", 0),
                        "type",
                        "a b",
                        SequenceNumbers.UNASSIGNED_SEQ_NO,
                        17,
                        0,
                        Result.CREATED) {};
        assertEquals("/index/type/a+b", response.getLocation(null));
        assertEquals("/index/type/a+b?routing=c+d", response.getLocation("c d"));
    }

    /**
     * Tests that {@link DocWriteResponse#toXContent(XContentBuilder, ToXContent.Params)} doesn't include {@code forced_refresh} unless it
     * is true. We can't assert this in the yaml tests because "not found" is also "false" there....
     */
    public void testToXContentDoesntIncludeForcedRefreshUnlessForced() throws IOException {
        DocWriteResponse response =
            new DocWriteResponse(
                new ShardId("index", "uuid", 0),
                "type",
                "id",
                SequenceNumbers.UNASSIGNED_SEQ_NO,
                17,
                0,
                Result.CREATED) {
                // DocWriteResponse is abstract so we have to sneak a subclass in here to test it.
            };
        response.setShardInfo(new ShardInfo(1, 1));
        response.setForcedRefresh(false);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, builder.bytes())) {
                assertThat(parser.map(), not(hasKey("forced_refresh")));
            }
        }
        response.setForcedRefresh(true);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, builder.bytes())) {
                assertThat(parser.map(), hasEntry("forced_refresh", true));
            }
        }
    }
}
