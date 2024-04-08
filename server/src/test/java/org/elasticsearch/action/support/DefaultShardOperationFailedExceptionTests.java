/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFormatTooNewException;
import org.apache.lucene.index.IndexFormatTooOldException;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.LockObtainFailedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.support.broadcast.BroadcastShardOperationFailedException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;

public class DefaultShardOperationFailedExceptionTests extends ESTestCase {

    public void testToString() {
        {
            DefaultShardOperationFailedException exception = new DefaultShardOperationFailedException(
                new ElasticsearchException("foo", new IllegalArgumentException("bar", new RuntimeException("baz")))
            );
            assertThat(exception.toString(), startsWith("[null][-1] failed, reason [org.elasticsearch.ElasticsearchException: foo"));
        }

        {
            ElasticsearchException elasticsearchException = new ElasticsearchException("foo");
            elasticsearchException.setIndex(new Index("index1", "_na_"));
            elasticsearchException.setShard(new ShardId("index1", "_na_", 1));
            DefaultShardOperationFailedException exception = new DefaultShardOperationFailedException(elasticsearchException);
            assertThat(
                exception.toString(),
                startsWith("[index1][1] failed, reason [[index1][[index1][1]] org.elasticsearch.ElasticsearchException: foo")
            );
        }

        {
            DefaultShardOperationFailedException exception = new DefaultShardOperationFailedException("index2", 2, new Exception("foo"));
            assertThat(exception.toString(), startsWith("[index2][2] failed, reason [java.lang.Exception: foo"));
        }
    }

    public void testToXContent() throws IOException {
        {
            DefaultShardOperationFailedException exception = new DefaultShardOperationFailedException(new ElasticsearchException("foo"));
            assertEquals(
                """
                    {"shard":-1,"index":null,"status":"INTERNAL_SERVER_ERROR","reason":{"type":"exception","reason":"foo"}}""",
                Strings.toString(exception)
            );
        }
        {
            DefaultShardOperationFailedException exception = new DefaultShardOperationFailedException(
                new ElasticsearchException("foo", new IllegalArgumentException("bar"))
            );
            assertEquals(XContentHelper.stripWhitespace("""
                {
                  "shard": -1,
                  "index": null,
                  "status": "INTERNAL_SERVER_ERROR",
                  "reason": {
                    "type": "exception",
                    "reason": "foo",
                    "caused_by": {
                      "type": "illegal_argument_exception",
                      "reason": "bar"
                    }
                  }
                }"""), Strings.toString(exception));
        }
        {
            DefaultShardOperationFailedException exception = new DefaultShardOperationFailedException(
                new BroadcastShardOperationFailedException(new ShardId("test", "_uuid", 2), "foo", new IllegalStateException("bar"))
            );
            assertEquals(XContentHelper.stripWhitespace("""
                {
                  "shard": 2,
                  "index": "test",
                  "status": "INTERNAL_SERVER_ERROR",
                  "reason": {
                    "type": "illegal_state_exception",
                    "reason": "bar"
                  }
                }"""), Strings.toString(exception));
        }
        {
            DefaultShardOperationFailedException exception = new DefaultShardOperationFailedException(
                "test",
                1,
                new IllegalArgumentException("foo")
            );
            assertEquals(XContentHelper.stripWhitespace("""
                {
                  "shard": 1,
                  "index": "test",
                  "status": "BAD_REQUEST",
                  "reason": {
                    "type": "illegal_argument_exception",
                    "reason": "foo"
                  }
                }"""), Strings.toString(exception));
        }
    }

    public void testFromXContent() throws IOException {
        XContent xContent = randomFrom(XContentType.values()).xContent();
        XContentBuilder builder = XContentBuilder.builder(xContent)
            .startObject()
            .field("shard", 1)
            .field("index", "test")
            .field("status", "INTERNAL_SERVER_ERROR")
            .startObject("reason")
            .field("type", "exception")
            .field("reason", "foo")
            .endObject()
            .endObject();
        builder = shuffleXContent(builder);
        DefaultShardOperationFailedException parsed;
        try (XContentParser parser = createParser(xContent, BytesReference.bytes(builder))) {
            assertEquals(XContentParser.Token.START_OBJECT, parser.nextToken());
            parsed = DefaultShardOperationFailedException.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }

        assertNotNull(parsed);
        assertEquals(parsed.shardId(), 1);
        assertEquals(parsed.index(), "test");
        assertEquals(parsed.status(), RestStatus.INTERNAL_SERVER_ERROR);
        assertEquals(parsed.getCause().getMessage(), "Elasticsearch exception [type=exception, reason=foo]");
    }

    public void testSerialization() throws Exception {
        final DefaultShardOperationFailedException exception = randomInstance();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            exception.writeTo(out);

            try (StreamInput in = out.bytes().streamInput()) {
                final DefaultShardOperationFailedException deserializedException = new DefaultShardOperationFailedException(in);
                assertNotSame(exception, deserializedException);
                assertThat(deserializedException.index(), equalTo(exception.index()));
                assertThat(deserializedException.shardId(), equalTo(exception.shardId()));

                // Serialising and deserialising an exception seems to remove the "java.base/" part from the stack trace
                // in the `reason` property, so we don't compare it directly. Instead, check that the first lines match,
                // and that the stack trace has the same number of lines.
                List<String> expectedReasonLines = exception.reason().lines().toList();
                List<String> actualReasonLines = deserializedException.reason().lines().toList();
                assertThat(actualReasonLines.get(0), equalTo(expectedReasonLines.get(0)));
                assertThat("Exceptions have a different number of lines", actualReasonLines, hasSize(expectedReasonLines.size()));

                assertThat(deserializedException.getCause().getMessage(), equalTo(exception.getCause().getMessage()));
                assertThat(deserializedException.getCause().getClass(), equalTo(exception.getCause().getClass()));
                assertArrayEquals(deserializedException.getCause().getStackTrace(), exception.getCause().getStackTrace());
            }
        }
    }

    private static DefaultShardOperationFailedException randomInstance() {
        final Exception cause = randomException();
        if (cause instanceof ElasticsearchException) {
            return new DefaultShardOperationFailedException((ElasticsearchException) cause);
        } else {
            return new DefaultShardOperationFailedException(randomAlphaOfLengthBetween(1, 5), randomIntBetween(0, 10), cause);
        }
    }

    @SuppressWarnings("unchecked")
    private static Exception randomException() {
        Supplier<Exception> supplier = randomFrom(
            () -> new CorruptIndexException(randomAlphaOfLengthBetween(1, 5), randomAlphaOfLengthBetween(1, 5), randomExceptionOrNull()),
            () -> new NullPointerException(randomAlphaOfLengthBetween(1, 5)),
            () -> new NumberFormatException(randomAlphaOfLengthBetween(1, 5)),
            () -> new IllegalArgumentException(randomAlphaOfLengthBetween(1, 5), randomExceptionOrNull()),
            () -> new AlreadyClosedException(randomAlphaOfLengthBetween(1, 5), randomExceptionOrNull()),
            () -> new EOFException(randomAlphaOfLengthBetween(1, 5)),
            () -> new SecurityException(randomAlphaOfLengthBetween(1, 5), randomExceptionOrNull()),
            () -> new StringIndexOutOfBoundsException(randomAlphaOfLengthBetween(1, 5)),
            () -> new ArrayIndexOutOfBoundsException(randomAlphaOfLengthBetween(1, 5)),
            () -> new StringIndexOutOfBoundsException(randomAlphaOfLengthBetween(1, 5)),
            () -> new FileNotFoundException(randomAlphaOfLengthBetween(1, 5)),
            () -> new IllegalStateException(randomAlphaOfLengthBetween(1, 5), randomExceptionOrNull()),
            () -> new LockObtainFailedException(randomAlphaOfLengthBetween(1, 5), randomExceptionOrNull()),
            () -> new InterruptedException(randomAlphaOfLengthBetween(1, 5)),
            () -> new IOException(randomAlphaOfLengthBetween(1, 5), randomExceptionOrNull()),
            () -> new EsRejectedExecutionException(randomAlphaOfLengthBetween(1, 5), randomBoolean()),
            () -> new IndexFormatTooNewException(randomAlphaOfLengthBetween(1, 10), randomInt(), randomInt(), randomInt()),
            () -> new IndexFormatTooOldException(randomAlphaOfLengthBetween(1, 5), randomAlphaOfLengthBetween(1, 5))
        );
        return supplier.get();
    }

    private static Exception randomExceptionOrNull() {
        return randomBoolean() ? randomException() : null;
    }
}
