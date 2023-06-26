/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.hamcrest;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BaseBroadcastResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertBlocked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.containsString;

public class ElasticsearchAssertionsTests extends ESTestCase {

    public void testAssertXContentEquivalent() throws IOException {
        try (XContentBuilder original = JsonXContent.contentBuilder()) {
            original.startObject();
            for (Object value : RandomObjects.randomStoredFieldValues(random(), original.contentType()).v1()) {
                original.field(randomAlphaOfLength(10), value);
            }
            {
                original.startObject(randomAlphaOfLength(10));
                for (Object value : RandomObjects.randomStoredFieldValues(random(), original.contentType()).v1()) {
                    original.field(randomAlphaOfLength(10), value);
                }
                original.endObject();
            }
            {
                original.startArray(randomAlphaOfLength(10));
                for (Object value : RandomObjects.randomStoredFieldValues(random(), original.contentType()).v1()) {
                    original.value(value);
                }
                original.endArray();
            }
            original.endObject();

            try (
                XContentBuilder copy = JsonXContent.contentBuilder();
                XContentParser parser = createParser(original.contentType().xContent(), BytesReference.bytes(original))
            ) {
                parser.nextToken();
                copy.generator().copyCurrentStructure(parser);
                try (XContentBuilder copyShuffled = shuffleXContent(copy)) {
                    assertToXContentEquivalent(BytesReference.bytes(original), BytesReference.bytes(copyShuffled), original.contentType());
                }
            }
        }
    }

    public void testAssertXContentEquivalentErrors() throws IOException {
        {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            {
                builder.startObject("foo");
                {
                    builder.field("f1", "value1");
                    builder.field("f2", "value2");
                }
                builder.endObject();
            }
            builder.endObject();

            XContentBuilder otherBuilder = JsonXContent.contentBuilder();
            otherBuilder.startObject();
            {
                otherBuilder.startObject("foo");
                {
                    otherBuilder.field("f1", "value1");
                }
                otherBuilder.endObject();
            }
            otherBuilder.endObject();
            AssertionError error = expectThrows(
                AssertionError.class,
                () -> assertToXContentEquivalent(BytesReference.bytes(builder), BytesReference.bytes(otherBuilder), builder.contentType())
            );
            assertThat(error.getMessage(), containsString("f2: expected [value2] but not found"));
        }
        {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            {
                builder.startObject("foo");
                {
                    builder.field("f1", "value1");
                    builder.field("f2", "value2");
                }
                builder.endObject();
            }
            builder.endObject();

            XContentBuilder otherBuilder = JsonXContent.contentBuilder();
            otherBuilder.startObject();
            {
                otherBuilder.startObject("foo");
                {
                    otherBuilder.field("f1", "value1");
                    otherBuilder.field("f2", "differentValue2");
                }
                otherBuilder.endObject();
            }
            otherBuilder.endObject();
            AssertionError error = expectThrows(
                AssertionError.class,
                () -> assertToXContentEquivalent(BytesReference.bytes(builder), BytesReference.bytes(otherBuilder), builder.contentType())
            );
            assertThat(error.getMessage(), containsString("f2: expected String [value2] but was String [differentValue2]"));
        }
        {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            {
                builder.startArray("foo");
                {
                    builder.value("one");
                    builder.value("two");
                    builder.value("three");
                }
                builder.endArray();
            }
            builder.field("f1", "value");
            builder.endObject();

            XContentBuilder otherBuilder = JsonXContent.contentBuilder();
            otherBuilder.startObject();
            {
                otherBuilder.startArray("foo");
                {
                    otherBuilder.value("one");
                    otherBuilder.value("two");
                    otherBuilder.value("four");
                }
                otherBuilder.endArray();
            }
            otherBuilder.field("f1", "value");
            otherBuilder.endObject();
            AssertionError error = expectThrows(
                AssertionError.class,
                () -> assertToXContentEquivalent(BytesReference.bytes(builder), BytesReference.bytes(otherBuilder), builder.contentType())
            );
            assertThat(error.getMessage(), containsString("2: expected String [three] but was String [four]"));
        }
        {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.startObject();
            {
                builder.startArray("foo");
                {
                    builder.value("one");
                    builder.value("two");
                    builder.value("three");
                }
                builder.endArray();
            }
            builder.endObject();

            XContentBuilder otherBuilder = JsonXContent.contentBuilder();
            otherBuilder.startObject();
            {
                otherBuilder.startArray("foo");
                {
                    otherBuilder.value("one");
                    otherBuilder.value("two");
                }
                otherBuilder.endArray();
            }
            otherBuilder.endObject();
            AssertionError error = expectThrows(
                AssertionError.class,
                () -> assertToXContentEquivalent(BytesReference.bytes(builder), BytesReference.bytes(otherBuilder), builder.contentType())
            );
            assertThat(error.getMessage(), containsString("expected [1] more entries"));
        }
    }

    public void testAssertBlocked() {
        Map<String, Set<ClusterBlock>> indexLevelBlocks = new HashMap<>();

        indexLevelBlocks.put("test", Set.of(IndexMetadata.INDEX_READ_ONLY_BLOCK));
        assertBlocked(
            new BaseBroadcastResponse(
                1,
                0,
                1,
                List.of(new DefaultShardOperationFailedException("test", 0, new ClusterBlockException(indexLevelBlocks)))
            )
        );

        indexLevelBlocks.put("test", Set.of(IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK));
        assertBlocked(
            new BaseBroadcastResponse(
                1,
                0,
                1,
                List.of(new DefaultShardOperationFailedException("test", 0, new ClusterBlockException(indexLevelBlocks)))
            )
        );

        indexLevelBlocks.put("test", Set.of(IndexMetadata.INDEX_READ_BLOCK, IndexMetadata.INDEX_METADATA_BLOCK));
        assertBlocked(
            new BaseBroadcastResponse(
                1,
                0,
                1,
                List.of(new DefaultShardOperationFailedException("test", 0, new ClusterBlockException(indexLevelBlocks)))
            )
        );

        indexLevelBlocks.put("test", Set.of(IndexMetadata.INDEX_READ_ONLY_BLOCK, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK));
        assertBlocked(
            new BaseBroadcastResponse(
                1,
                0,
                1,
                List.of(new DefaultShardOperationFailedException("test", 0, new ClusterBlockException(indexLevelBlocks)))
            )
        );
    }
}
