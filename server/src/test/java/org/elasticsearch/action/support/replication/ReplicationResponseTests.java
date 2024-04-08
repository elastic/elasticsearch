/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support.replication;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.replication.ReplicationResponse.ShardInfo;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;

import static org.elasticsearch.ElasticsearchExceptionTests.assertDeepEquals;
import static org.elasticsearch.common.xcontent.XContentHelper.toXContent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;

public class ReplicationResponseTests extends ESTestCase {

    public void testShardInfoToString() {
        final int total = 5;
        final int successful = randomIntBetween(1, total);
        final ShardInfo shardInfo = ShardInfo.of(total, successful);
        assertEquals(Strings.format("ShardInfo{total=5, successful=%d, failures=[]}", successful), shardInfo.toString());
    }

    public void testShardInfoToXContent() throws IOException {
        {
            ShardInfo shardInfo = ShardInfo.of(5, 3);
            String output = Strings.toString(shardInfo);
            assertEquals("{\"total\":5,\"successful\":3,\"failed\":0}", output);
        }
        {
            ShardInfo shardInfo = ShardInfo.of(
                6,
                4,
                new ShardInfo.Failure[] {
                    new ShardInfo.Failure(
                        new ShardId("index", "_uuid", 3),
                        "_node_id",
                        new IllegalArgumentException("Wrong"),
                        RestStatus.BAD_REQUEST,
                        false
                    ),
                    new ShardInfo.Failure(
                        new ShardId("index", "_uuid", 1),
                        "_node_id",
                        new CircuitBreakingException("Wrong", 12, 21, CircuitBreaker.Durability.PERMANENT),
                        RestStatus.NOT_ACCEPTABLE,
                        true
                    ) }
            );
            String output = Strings.toString(shardInfo);
            assertEquals(XContentHelper.stripWhitespace("""
                {
                  "total": 6,
                  "successful": 4,
                  "failed": 2,
                  "failures": [
                    {
                      "_index": "index",
                      "_shard": 3,
                      "_node": "_node_id",
                      "reason": {
                        "type": "illegal_argument_exception",
                        "reason": "Wrong"
                      },
                      "status": "BAD_REQUEST",
                      "primary": false
                    },
                    {
                      "_index": "index",
                      "_shard": 1,
                      "_node": "_node_id",
                      "reason": {
                        "type": "circuit_breaking_exception",
                        "reason": "Wrong",
                        "bytes_wanted": 12,
                        "bytes_limit": 21,
                        "durability": "PERMANENT"
                      },
                      "status": "NOT_ACCEPTABLE",
                      "primary": true
                    }
                  ]
                }"""), output);
        }
    }

    public void testShardInfoToAndFromXContent() throws IOException {
        final Tuple<ShardInfo, ShardInfo> tuple = RandomObjects.randomShardInfo(random());
        ShardInfo shardInfo = tuple.v1();
        ShardInfo expectedShardInfo = tuple.v2();

        final XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(shardInfo, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);

        // Shuffle the XContent fields
        if (randomBoolean()) {
            try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
                originalBytes = BytesReference.bytes(shuffleXContent(parser, randomBoolean()));
            }
        }

        ShardInfo parsedShardInfo;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            // Move to the first start object
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsedShardInfo = ShardInfo.fromXContent(parser);
            assertNull(parser.nextToken());
        }
        assertShardInfo(expectedShardInfo, parsedShardInfo);

        BytesReference expectedFinalBytes = toXContent(expectedShardInfo, xContentType, humanReadable);
        BytesReference finalBytes = toXContent(parsedShardInfo, xContentType, humanReadable);
        assertToXContentEquivalent(expectedFinalBytes, finalBytes, xContentType);
    }

    public static void assertShardInfo(ShardInfo expected, ShardInfo actual) {
        if (expected == null) {
            assertNull(actual);
        } else {
            assertEquals(expected.getTotal(), actual.getTotal());
            assertEquals(expected.getSuccessful(), actual.getSuccessful());
            assertEquals(expected.getFailed(), actual.getFailed());

            ReplicationResponse.ShardInfo.Failure[] expectedFailures = expected.getFailures();
            ReplicationResponse.ShardInfo.Failure[] actualFailures = actual.getFailures();
            assertEquals(expectedFailures.length, actualFailures.length);

            for (int i = 0; i < expectedFailures.length; i++) {
                ReplicationResponse.ShardInfo.Failure expectedFailure = expectedFailures[i];
                ReplicationResponse.ShardInfo.Failure actualFailure = actualFailures[i];

                assertEquals(expectedFailure.fullShardId(), actualFailure.fullShardId());
                assertEquals(expectedFailure.status(), actualFailure.status());
                assertEquals(expectedFailure.nodeId(), actualFailure.nodeId());
                assertEquals(expectedFailure.primary(), actualFailure.primary());
                Throwable expectedCause = expectedFailure.getCause();
                Throwable actualCause = actualFailure.getCause();
                if (expectedCause instanceof ElasticsearchException) {
                    assertDeepEquals((ElasticsearchException) expectedCause, (ElasticsearchException) actualCause);
                } else {
                    assertEquals(expectedCause.getClass(), actualCause.getClass());
                    assertEquals(expectedCause.getMessage(), actualCause.getMessage());
                }
            }
        }
    }
}
