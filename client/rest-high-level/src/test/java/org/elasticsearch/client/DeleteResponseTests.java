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

package org.elasticsearch.client;

import org.elasticsearch.action.DocWriteResponse.Result;
import org.elasticsearch.common.lucene.uid.Versions;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Collections;

public class DeleteResponseTests extends ESTestCase {

    public void testParseSuccess() throws IOException {
        String responseString = "{\n" +
                "    \"_shards\" : {\n" +
                "        \"total\" : 2,\n" +
                "        \"failed\" : 0,\n" +
                "        \"successful\" : 2\n" +
                "    },\n" +
                "    \"_index\" : \"twitter\",\n" +
                "    \"_id\" : \"1\",\n" +
                "    \"_version\" : 2,\n" +
                "    \"_primary_term\": 1,\n" +
                "    \"_seq_no\": 5,\n" +
                "    \"result\": \"deleted\"\n" +
                "}";
        try (XContentParser parser = XContentType.JSON.xContent().createParser(new NamedXContentRegistry(Collections.emptyList()),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, responseString)) {
            DeleteResponse response = DeleteResponse.PARSER.parse(parser, null);
            assertEquals(2, response.getShardInfo().getTotal());
            assertEquals(2, response.getShardInfo().getSuccessful());
            assertEquals(0, response.getShardInfo().getFailed());
            assertEquals("twitter", response.getIndex());
            assertEquals("1", response.getId());
            assertEquals(2L, response.getVersion());
            assertEquals(1L, response.getPrimaryTerm());
            assertEquals(5L, response.getSeqNo());
            assertEquals(Result.DELETED, response.getResult());
            assertFalse(response.isForcedRefresh());
        }
    }

    public void testParseForcedRefresh() throws IOException {
        String responseString = "{\n" +
                "    \"_shards\" : {\n" +
                "        \"total\" : 2,\n" +
                "        \"failed\" : 0,\n" +
                "        \"successful\" : 2\n" +
                "    },\n" +
                "    \"_index\" : \"twitter\",\n" +
                "    \"_id\" : \"1\",\n" +
                "    \"_version\" : 2,\n" +
                "    \"_primary_term\": 1,\n" +
                "    \"_seq_no\": 5,\n" +
                "    \"result\": \"deleted\",\n" +
                "    \"forced_refresh\": true\n" +
                "}";
        try (XContentParser parser = XContentType.JSON.xContent().createParser(new NamedXContentRegistry(Collections.emptyList()),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, responseString)) {
            DeleteResponse response = DeleteResponse.PARSER.parse(parser, null);
            assertEquals(2, response.getShardInfo().getTotal());
            assertEquals(2, response.getShardInfo().getSuccessful());
            assertEquals(0, response.getShardInfo().getFailed());
            assertEquals("twitter", response.getIndex());
            assertEquals("1", response.getId());
            assertEquals(2L, response.getVersion());
            assertEquals(1L, response.getPrimaryTerm());
            assertEquals(5L, response.getSeqNo());
            assertEquals(Result.DELETED, response.getResult());
            assertTrue(response.isForcedRefresh());
        }
    }

    public void testParseNotFound() throws IOException {
        String responseString = "{\n" +
                "    \"_shards\" : {\n" +
                "        \"total\" : 2,\n" +
                "        \"failed\" : 0,\n" +
                "        \"successful\" : 2\n" +
                "    },\n" +
                "    \"_index\" : \"twitter\",\n" +
                "    \"_id\" : \"1\",\n" +
                "    \"_version\" : -1,\n" +
                "    \"result\": \"not_found\"\n" +
                "}";
        try (XContentParser parser = XContentType.JSON.xContent().createParser(new NamedXContentRegistry(Collections.emptyList()),
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION, responseString)) {
            DeleteResponse response = DeleteResponse.PARSER.parse(parser, null);
            assertEquals(2, response.getShardInfo().getTotal());
            assertEquals(2, response.getShardInfo().getSuccessful());
            assertEquals(0, response.getShardInfo().getFailed());
            assertEquals("twitter", response.getIndex());
            assertEquals("1", response.getId());
            assertEquals(Versions.NOT_FOUND, response.getVersion());
            assertEquals(0L, response.getPrimaryTerm());
            assertEquals(SequenceNumbers.UNASSIGNED_SEQ_NO, response.getSeqNo());
            assertEquals(Result.NOT_FOUND, response.getResult());
            assertFalse(response.isForcedRefresh());
        }
    }

}
