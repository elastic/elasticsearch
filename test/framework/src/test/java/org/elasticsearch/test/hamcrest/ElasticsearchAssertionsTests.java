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

package org.elasticsearch.test.hamcrest;

import org.elasticsearch.action.support.DefaultShardOperationFailedException;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.block.ClusterBlock;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.RandomObjects;

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

            try (XContentBuilder copy = JsonXContent.contentBuilder();
                    XContentParser parser = createParser(original.contentType().xContent(), BytesReference.bytes(original))) {
                parser.nextToken();
                copy.generator().copyCurrentStructure(parser);
                try (XContentBuilder copyShuffled = shuffleXContent(copy) ) {
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
            AssertionError error = expectThrows(AssertionError.class,
                    () -> assertToXContentEquivalent(BytesReference.bytes(builder), BytesReference.bytes(otherBuilder),
                            builder.contentType()));
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
            AssertionError error = expectThrows(AssertionError.class,
                    () -> assertToXContentEquivalent(BytesReference.bytes(builder), BytesReference.bytes(otherBuilder),
                            builder.contentType()));
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
            AssertionError error = expectThrows(AssertionError.class,
                    () -> assertToXContentEquivalent(BytesReference.bytes(builder), BytesReference.bytes(otherBuilder),
                            builder.contentType()));
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
            AssertionError error = expectThrows(AssertionError.class,
                    () -> assertToXContentEquivalent(BytesReference.bytes(builder), BytesReference.bytes(otherBuilder),
                            builder.contentType()));
            assertThat(error.getMessage(), containsString("expected [1] more entries"));
        }
    }

    public void testAssertBlocked() {
        Map<String, Set<ClusterBlock>> indexLevelBlocks = new HashMap<>();

        indexLevelBlocks.put("test", Set.of(IndexMetadata.INDEX_READ_ONLY_BLOCK));
        assertBlocked(new BroadcastResponse(1, 0, 1, List.of(new DefaultShardOperationFailedException("test", 0,
            new ClusterBlockException(indexLevelBlocks)))));

        indexLevelBlocks.put("test", Set.of(IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK));
        assertBlocked(new BroadcastResponse(1, 0, 1, List.of(new DefaultShardOperationFailedException("test", 0,
            new ClusterBlockException(indexLevelBlocks)))));

        indexLevelBlocks.put("test", Set.of(IndexMetadata.INDEX_READ_BLOCK, IndexMetadata.INDEX_METADATA_BLOCK));
        assertBlocked(new BroadcastResponse(1, 0, 1, List.of(new DefaultShardOperationFailedException("test", 0,
            new ClusterBlockException(indexLevelBlocks)))));

        indexLevelBlocks.put("test", Set.of(IndexMetadata.INDEX_READ_ONLY_BLOCK, IndexMetadata.INDEX_READ_ONLY_ALLOW_DELETE_BLOCK));
        assertBlocked(new BroadcastResponse(1, 0, 1, List.of(new DefaultShardOperationFailedException("test", 0,
            new ClusterBlockException(indexLevelBlocks)))));
    }
}
