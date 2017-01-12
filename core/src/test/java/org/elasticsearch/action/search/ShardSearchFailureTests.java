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

package org.elasticsearch.action.search;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;

public class ShardSearchFailureTests extends ESTestCase {

    public static ShardSearchFailure createTestItem() {
        String randomMessage = randomAsciiOfLengthBetween(3, 20);
        Exception ex = new ParsingException(0, 0, randomMessage , null);
        String nodeId = randomAsciiOfLengthBetween(5, 10);
        String indexName = randomAsciiOfLengthBetween(5, 10);
        String indexUuid = randomAsciiOfLengthBetween(5, 10);
        int shardId = randomInt();
        return new ShardSearchFailure(ex,
                new SearchShardTarget(nodeId, new ShardId(new Index(indexName, indexUuid), shardId)));
    }

    public void testFromXContent() throws IOException {
        ShardSearchFailure response = createTestItem();
        XContentType xcontentType = randomFrom(XContentType.values());
        XContentBuilder builder = XContentFactory.contentBuilder(xcontentType);
        builder.startObject();
        builder = response.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        XContentParser parser = createParser(builder);
        parser.nextToken(); // jump to first START_OBJECT
        ShardSearchFailure parsed = ShardSearchFailure.fromXContent(parser);

        assertEquals(response.index(), parsed.index());
        assertEquals(response.shard().nodeId(), parsed.shard().nodeId());
        assertEquals(response.shardId(), parsed.shardId());

        // we cannot compare the cause, because it will be wrapped in an outer ElasticSearchException
        // best effort: try to check that the original message appears somewhere in the rendered xContent
        String originalMsg = response.getCause().getMessage();
        assertTrue(parsed.getCause().getMessage().contains(originalMsg));

        assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
        assertNull(parser.nextToken());
    }

    public void testToXContent() throws IOException {
        ShardSearchFailure failure = new ShardSearchFailure(new ParsingException(0, 0, "some message", null),
                new SearchShardTarget("nodeId", new ShardId(new Index("indexName", "indexUuid"), 123)));
        BytesReference xContent = toXContent(failure, XContentType.JSON);
        assertEquals(
                "{\"shard\":123,"
                        + "\"index\":\"indexName\","
                        + "\"node\":\"nodeId\","
                        + "\"reason\":{"
                            + "\"type\":\"parsing_exception\","
                            + "\"reason\":\"some message\","
                            + "\"line\":0,"
                            + "\"col\":0"
                        + "}"
                + "}",
                xContent.utf8ToString());
    }
}
