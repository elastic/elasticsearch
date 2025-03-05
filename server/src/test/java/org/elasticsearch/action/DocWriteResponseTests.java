/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class DocWriteResponseTests extends ESTestCase {
    public void testGetLocation() {
        final DocWriteResponse response = new DocWriteResponse(
            new ShardId("index", "uuid", 0),
            "id",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            17,
            0,
            Result.CREATED
        ) {
        };
        assertEquals("/index/_doc/id", response.getLocation(null));
        assertEquals("/index/_doc/id?routing=test_routing", response.getLocation("test_routing"));
    }

    public void testGetLocationNonAscii() {
        final DocWriteResponse response = new DocWriteResponse(
            new ShardId("index", "uuid", 0),
            "❤",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            17,
            0,
            Result.CREATED
        ) {
        };
        assertEquals("/index/_doc/%E2%9D%A4", response.getLocation(null));
        assertEquals("/index/_doc/%E2%9D%A4?routing=%C3%A4", response.getLocation("ä"));
    }

    public void testGetLocationWithSpaces() {
        final DocWriteResponse response = new DocWriteResponse(
            new ShardId("index", "uuid", 0),
            "a b",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            17,
            0,
            Result.CREATED
        ) {
        };
        assertEquals("/index/_doc/a+b", response.getLocation(null));
        assertEquals("/index/_doc/a+b?routing=c+d", response.getLocation("c d"));
    }

    /**
     * Tests that {@link DocWriteResponse#toXContent(XContentBuilder, ToXContent.Params)} doesn't include {@code forced_refresh} unless it
     * is true. We can't assert this in the yaml tests because "not found" is also "false" there....
     */
    public void testToXContentDoesntIncludeForcedRefreshUnlessForced() throws IOException {
        DocWriteResponse response = new DocWriteResponse(
            new ShardId("index", "uuid", 0),
            "id",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            17,
            0,
            Result.CREATED
        ) {
            // DocWriteResponse is abstract so we have to sneak a subclass in here to test it.
        };
        response.setShardInfo(ShardInfo.allSuccessful(1));
        response.setForcedRefresh(false);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
                assertThat(parser.map(), not(hasKey("forced_refresh")));
            }
        }
        response.setForcedRefresh(true);
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
                assertThat(parser.map(), hasEntry("forced_refresh", true));
            }
        }
    }

    public void testTypeWhenCompatible() throws IOException {
        DocWriteResponse response = new DocWriteResponse(
            new ShardId("index", "uuid", 0),
            "id",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            17,
            0,
            DocWriteResponse.Result.CREATED
        ) {
            // DocWriteResponse is abstract so we have to sneak a subclass in here to test it.
        };

        try (XContentBuilder builder = XContentBuilder.builder(JsonXContent.jsonXContent, RestApiVersion.V_8)) {
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);

            try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
                assertThat(parser.map(), not(hasKey(MapperService.TYPE_FIELD_NAME)));
            }
        }
    }
}
