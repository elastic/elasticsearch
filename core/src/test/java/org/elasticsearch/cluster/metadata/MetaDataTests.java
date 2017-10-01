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

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class MetaDataTests extends ESTestCase {

    public void testIndexAndAliasWithSameName() {
        IndexMetaData.Builder builder = IndexMetaData.builder("index")
                .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putAlias(AliasMetaData.builder("index").build());
        try {
            MetaData.builder().put(builder).build();
            fail("exception should have been thrown");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), equalTo("index and alias names need to be unique, but the following duplicates were found [index (alias of [index])]"));
        }
    }

    public void testAliasCollidingWithAnExistingIndex() {
        int indexCount = randomIntBetween(10, 100);
        Set<String> indices = new HashSet<>(indexCount);
        for (int i = 0; i < indexCount; i++) {
            indices.add(randomAlphaOfLength(10));
        }
        Map<String, Set<String>> aliasToIndices = new HashMap<>();
        for (String alias: randomSubsetOf(randomIntBetween(1, 10), indices)) {
            aliasToIndices.put(alias, new HashSet<>(randomSubsetOf(randomIntBetween(1, 3), indices)));
        }
        int properAliases = randomIntBetween(0, 3);
        for (int i = 0; i < properAliases; i++) {
            aliasToIndices.put(randomAlphaOfLength(5), new HashSet<>(randomSubsetOf(randomIntBetween(1, 3), indices)));
        }
        MetaData.Builder metaDataBuilder = MetaData.builder();
        for (String index : indices) {
            IndexMetaData.Builder indexBuilder = IndexMetaData.builder(index)
                .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0);
            aliasToIndices.forEach((key, value) -> {
                if (value.contains(index)) {
                    indexBuilder.putAlias(AliasMetaData.builder(key).build());
                }
            });
            metaDataBuilder.put(indexBuilder);
        }
        try {
            metaDataBuilder.build();
            fail("exception should have been thrown");
        } catch (IllegalStateException e) {
            assertThat(e.getMessage(), startsWith("index and alias names need to be unique"));
        }
    }

    public void testResolveIndexRouting() {
        IndexMetaData.Builder builder = IndexMetaData.builder("index")
                .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putAlias(AliasMetaData.builder("alias0").build())
                .putAlias(AliasMetaData.builder("alias1").routing("1").build())
                .putAlias(AliasMetaData.builder("alias2").routing("1,2").build());
        MetaData metaData = MetaData.builder().put(builder).build();

        // no alias, no index
        assertEquals(metaData.resolveIndexRouting(null, null, null), null);
        assertEquals(metaData.resolveIndexRouting(null, "0", null), "0");
        assertEquals(metaData.resolveIndexRouting("32", "0", null), "0");
        assertEquals(metaData.resolveIndexRouting("32", null, null), "32");

        // index, no alias
        assertEquals(metaData.resolveIndexRouting("32", "0", "index"), "0");
        assertEquals(metaData.resolveIndexRouting("32", null, "index"), "32");
        assertEquals(metaData.resolveIndexRouting(null, null, "index"), null);
        assertEquals(metaData.resolveIndexRouting(null, "0", "index"), "0");

        // alias with no index routing
        assertEquals(metaData.resolveIndexRouting(null, null, "alias0"), null);
        assertEquals(metaData.resolveIndexRouting(null, "0", "alias0"), "0");
        assertEquals(metaData.resolveIndexRouting("32", null, "alias0"), "32");
        assertEquals(metaData.resolveIndexRouting("32", "0", "alias0"), "0");

        // alias with index routing.
        assertEquals(metaData.resolveIndexRouting(null, null, "alias1"), "1");
        assertEquals(metaData.resolveIndexRouting("32", null, "alias1"), "1");
        assertEquals(metaData.resolveIndexRouting("32", "1", "alias1"), "1");
        try {
            metaData.resolveIndexRouting(null, "0", "alias1");
            fail("should fail");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Alias [alias1] has index routing associated with it [1], and was provided with routing value [0], rejecting operation"));
        }

        try {
            metaData.resolveIndexRouting("32", "0", "alias1");
            fail("should fail");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Alias [alias1] has index routing associated with it [1], and was provided with routing value [0], rejecting operation"));
        }

        // alias with invalid index routing.
        try {
            metaData.resolveIndexRouting(null, null, "alias2");
            fail("should fail");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("index/alias [alias2] provided with routing value [1,2] that resolved to several routing values, rejecting operation"));
        }

        try {
            metaData.resolveIndexRouting(null, "1", "alias2");
            fail("should fail");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("index/alias [alias2] provided with routing value [1,2] that resolved to several routing values, rejecting operation"));
        }

        try {
            metaData.resolveIndexRouting("32", null, "alias2");
            fail("should fail");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("index/alias [alias2] provided with routing value [1,2] that resolved to several routing values, rejecting operation"));
        }
    }

    public void testUnknownFieldClusterMetaData() throws IOException {
        BytesReference metadata = JsonXContent.contentBuilder()
            .startObject()
                .startObject("meta-data")
                    .field("random", "value")
                .endObject()
            .endObject().bytes();
        XContentParser parser = createParser(JsonXContent.jsonXContent, metadata);
        try {
            MetaData.Builder.fromXContent(parser);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Unexpected field [random]", e.getMessage());
        }
    }

    public void testUnknownFieldIndexMetaData() throws IOException {
        BytesReference metadata = JsonXContent.contentBuilder()
            .startObject()
                .startObject("index_name")
                    .field("random", "value")
                .endObject()
            .endObject().bytes();
        XContentParser parser = createParser(JsonXContent.jsonXContent, metadata);
        try {
            IndexMetaData.Builder.fromXContent(parser);
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Unexpected field [random]", e.getMessage());
        }
    }

    public void testMetaDataGlobalStateChangesOnIndexDeletions() {
        IndexGraveyard.Builder builder = IndexGraveyard.builder();
        builder.addTombstone(new Index("idx1", UUIDs.randomBase64UUID()));
        final MetaData metaData1 = MetaData.builder().indexGraveyard(builder.build()).build();
        builder = IndexGraveyard.builder(metaData1.indexGraveyard());
        builder.addTombstone(new Index("idx2", UUIDs.randomBase64UUID()));
        final MetaData metaData2 = MetaData.builder(metaData1).indexGraveyard(builder.build()).build();
        assertFalse("metadata not equal after adding index deletions", MetaData.isGlobalStateEquals(metaData1, metaData2));
        final MetaData metaData3 = MetaData.builder(metaData2).build();
        assertTrue("metadata equal when not adding index deletions", MetaData.isGlobalStateEquals(metaData2, metaData3));
    }

    public void testXContentWithIndexGraveyard() throws IOException {
        final IndexGraveyard graveyard = IndexGraveyardTests.createRandom();
        final MetaData originalMeta = MetaData.builder().indexGraveyard(graveyard).build();
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        originalMeta.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, builder.bytes());
        final MetaData fromXContentMeta = MetaData.fromXContent(parser);
        assertThat(fromXContentMeta.indexGraveyard(), equalTo(originalMeta.indexGraveyard()));
    }

    public void testSerializationWithIndexGraveyard() throws IOException {
        final IndexGraveyard graveyard = IndexGraveyardTests.createRandom();
        final MetaData originalMeta = MetaData.builder().indexGraveyard(graveyard).build();
        final BytesStreamOutput out = new BytesStreamOutput();
        originalMeta.writeTo(out);
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        final MetaData fromStreamMeta = MetaData.readFrom(
            new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)
        );
        assertThat(fromStreamMeta.indexGraveyard(), equalTo(fromStreamMeta.indexGraveyard()));
    }
}
