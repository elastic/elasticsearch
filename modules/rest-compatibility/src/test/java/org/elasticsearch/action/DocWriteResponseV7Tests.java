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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

public class DocWriteResponseV7Tests extends ESTestCase {

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
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.setCompatibleMajorVersion((byte) 7);
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);

            try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
                assertThat(parser.map(), hasEntry(DocWriteResponse.TYPE_FIELD_NAME, MapperService.SINGLE_MAPPING_NAME));
            }
        }

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.setCompatibleMajorVersion((byte) 6);
            response.toXContent(builder, ToXContent.EMPTY_PARAMS);

            try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
                assertThat(parser.map(), not(hasKey(DocWriteResponse.TYPE_FIELD_NAME)));
            }
        }
    }
}
