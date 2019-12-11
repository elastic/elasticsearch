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
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.coordination.CoordinationMetaData;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfigExclusion;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.Index;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.startsWith;

public class MetaDataTests extends ESTestCase {

    public void testFindAliases() {
        MetaData metaData = MetaData.builder().put(IndexMetaData.builder("index")
            .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetaData.builder("alias1").build())
            .putAlias(AliasMetaData.builder("alias2").build())).build();

        {
            ImmutableOpenMap<String, List<AliasMetaData>> aliases = metaData.findAliases(new GetAliasesRequest(), Strings.EMPTY_ARRAY);
            assertThat(aliases.size(), equalTo(0));
        }
        {
            final GetAliasesRequest request;
            if (randomBoolean()) {
                request = new GetAliasesRequest();
            } else {
                request = new GetAliasesRequest(randomFrom("alias1", "alias2"));
                // replacing with empty aliases behaves as if aliases were unspecified at request building
                request.replaceAliases(Strings.EMPTY_ARRAY);
            }
            ImmutableOpenMap<String, List<AliasMetaData>> aliases = metaData.findAliases(new GetAliasesRequest(), new String[]{"index"});
            assertThat(aliases.size(), equalTo(1));
            List<AliasMetaData> aliasMetaDataList = aliases.get("index");
            assertThat(aliasMetaDataList.size(), equalTo(2));
            assertThat(aliasMetaDataList.get(0).alias(), equalTo("alias1"));
            assertThat(aliasMetaDataList.get(1).alias(), equalTo("alias2"));
        }
        {
            ImmutableOpenMap<String, List<AliasMetaData>> aliases =
                metaData.findAliases(new GetAliasesRequest("alias*"), new String[]{"index"});
            assertThat(aliases.size(), equalTo(1));
            List<AliasMetaData> aliasMetaDataList = aliases.get("index");
            assertThat(aliasMetaDataList.size(), equalTo(2));
            assertThat(aliasMetaDataList.get(0).alias(), equalTo("alias1"));
            assertThat(aliasMetaDataList.get(1).alias(), equalTo("alias2"));
        }
        {
            ImmutableOpenMap<String, List<AliasMetaData>> aliases =
                metaData.findAliases(new GetAliasesRequest("alias1"), new String[]{"index"});
            assertThat(aliases.size(), equalTo(1));
            List<AliasMetaData> aliasMetaDataList = aliases.get("index");
            assertThat(aliasMetaDataList.size(), equalTo(1));
            assertThat(aliasMetaDataList.get(0).alias(), equalTo("alias1"));
        }
        {
            ImmutableOpenMap<String, List<AliasMetaData>> aliases = metaData.findAllAliases(new String[]{"index"});
            assertThat(aliases.size(), equalTo(1));
            List<AliasMetaData> aliasMetaDataList = aliases.get("index");
            assertThat(aliasMetaDataList.size(), equalTo(2));
            assertThat(aliasMetaDataList.get(0).alias(), equalTo("alias1"));
            assertThat(aliasMetaDataList.get(1).alias(), equalTo("alias2"));
        }
        {
            ImmutableOpenMap<String, List<AliasMetaData>> aliases = metaData.findAllAliases(Strings.EMPTY_ARRAY);
            assertThat(aliases.size(), equalTo(0));
        }
    }

    public void testFindAliasWithExclusion() {
        MetaData metaData = MetaData.builder().put(
            IndexMetaData.builder("index")
                .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putAlias(AliasMetaData.builder("alias1").build())
                .putAlias(AliasMetaData.builder("alias2").build())
        ).build();
        List<AliasMetaData> aliases =
            metaData.findAliases(new GetAliasesRequest().aliases("*", "-alias1"), new String[] {"index"}).get("index");
        assertThat(aliases.size(), equalTo(1));
        assertThat(aliases.get(0).alias(), equalTo("alias2"));
    }

    public void testFindAliasWithExclusionAndOverride() {
        MetaData metaData = MetaData.builder().put(
            IndexMetaData.builder("index")
                .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .putAlias(AliasMetaData.builder("aa").build())
                .putAlias(AliasMetaData.builder("ab").build())
                .putAlias(AliasMetaData.builder("bb").build())
        ).build();
        List<AliasMetaData> aliases =
            metaData.findAliases(new GetAliasesRequest().aliases("a*", "-*b", "b*"), new String[] {"index"}).get("index");
        assertThat(aliases.size(), equalTo(2));
        assertThat(aliases.get(0).alias(), equalTo("aa"));
        assertThat(aliases.get(1).alias(), equalTo("bb"));
    }

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
            assertThat(e.getMessage(),
                equalTo("index and alias names need to be unique, but the following duplicates were found [index (alias of [index])]"));
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

    public void testValidateAliasWriteOnly() {
        String alias = randomAlphaOfLength(5);
        String indexA = randomAlphaOfLength(6);
        String indexB = randomAlphaOfLength(7);
        Boolean aWriteIndex = randomBoolean() ? null : randomBoolean();
        Boolean bWriteIndex;
        if (Boolean.TRUE.equals(aWriteIndex)) {
            bWriteIndex = randomFrom(Boolean.FALSE, null);
        } else {
            bWriteIndex = randomFrom(Boolean.TRUE, Boolean.FALSE, null);
        }
        // when only one index/alias pair exist
        MetaData metaData = MetaData.builder().put(buildIndexMetaData(indexA, alias, aWriteIndex)).build();

        // when alias points to two indices, but valid
        // one of the following combinations: [(null, null), (null, true), (null, false), (false, false)]
        MetaData.builder(metaData).put(buildIndexMetaData(indexB, alias, bWriteIndex)).build();

        // when too many write indices
        Exception exception = expectThrows(IllegalStateException.class,
            () -> {
                IndexMetaData.Builder metaA = buildIndexMetaData(indexA, alias, true);
                IndexMetaData.Builder metaB = buildIndexMetaData(indexB, alias, true);
                MetaData.builder().put(metaA).put(metaB).build();
            });
        assertThat(exception.getMessage(), startsWith("alias [" + alias + "] has more than one write index ["));
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
        assertEquals(metaData.resolveIndexRouting(null, null), null);
        assertEquals(metaData.resolveIndexRouting("0", null), "0");

        // index, no alias
        assertEquals(metaData.resolveIndexRouting(null, "index"), null);
        assertEquals(metaData.resolveIndexRouting("0", "index"), "0");

        // alias with no index routing
        assertEquals(metaData.resolveIndexRouting(null, "alias0"), null);
        assertEquals(metaData.resolveIndexRouting("0", "alias0"), "0");

        // alias with index routing.
        assertEquals(metaData.resolveIndexRouting(null, "alias1"), "1");
        try {
            metaData.resolveIndexRouting("0", "alias1");
            fail("should fail");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("Alias [alias1] has index routing associated with it [1], " +
                "and was provided with routing value [0], rejecting operation"));
        }

        // alias with invalid index routing.
        try {
            metaData.resolveIndexRouting(null, "alias2");
            fail("should fail");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("index/alias [alias2] provided with routing value [1,2] that" +
                " resolved to several routing values, rejecting operation"));
        }

        try {
            metaData.resolveIndexRouting("1", "alias2");
            fail("should fail");
        } catch (IllegalArgumentException ex) {
            assertThat(ex.getMessage(), is("index/alias [alias2] provided with routing value [1,2] that" +
                " resolved to several routing values, rejecting operation"));
        }

        IndexMetaData.Builder builder2 = IndexMetaData.builder("index2")
            .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetaData.builder("alias0").build());
        MetaData metaDataTwoIndices = MetaData.builder(metaData).put(builder2).build();

        // alias with multiple indices
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class,
            () -> metaDataTwoIndices.resolveIndexRouting("1", "alias0"));
        assertThat(exception.getMessage(), startsWith("Alias [alias0] has more than one index associated with it"));
    }

    public void testResolveWriteIndexRouting() {
        AliasMetaData.Builder aliasZeroBuilder = AliasMetaData.builder("alias0");
        if (randomBoolean()) {
            aliasZeroBuilder.writeIndex(true);
        }
        IndexMetaData.Builder builder = IndexMetaData.builder("index")
            .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(aliasZeroBuilder.build())
            .putAlias(AliasMetaData.builder("alias1").routing("1").build())
            .putAlias(AliasMetaData.builder("alias2").routing("1,2").build())
            .putAlias(AliasMetaData.builder("alias3").writeIndex(false).build())
            .putAlias(AliasMetaData.builder("alias4").routing("1,2").writeIndex(true).build());
        MetaData metaData = MetaData.builder().put(builder).build();

        // no alias, no index
        assertEquals(metaData.resolveWriteIndexRouting(null, null), null);
        assertEquals(metaData.resolveWriteIndexRouting("0", null), "0");

        // index, no alias
        assertEquals(metaData.resolveWriteIndexRouting(null, "index"), null);
        assertEquals(metaData.resolveWriteIndexRouting("0", "index"), "0");

        // alias with no index routing
        assertEquals(metaData.resolveWriteIndexRouting(null, "alias0"), null);
        assertEquals(metaData.resolveWriteIndexRouting("0", "alias0"), "0");

        // alias with index routing.
        assertEquals(metaData.resolveWriteIndexRouting(null, "alias1"), "1");
        Exception exception = expectThrows(IllegalArgumentException.class, () -> metaData.resolveWriteIndexRouting("0", "alias1"));
        assertThat(exception.getMessage(),
            is("Alias [alias1] has index routing associated with it [1], and was provided with routing value [0], rejecting operation"));

        // alias with invalid index routing.
        exception = expectThrows(IllegalArgumentException.class, () -> metaData.resolveWriteIndexRouting(null, "alias2"));
            assertThat(exception.getMessage(),
                is("index/alias [alias2] provided with routing value [1,2] that resolved to several routing values, rejecting operation"));
        exception = expectThrows(IllegalArgumentException.class, () -> metaData.resolveWriteIndexRouting("1", "alias2"));
        assertThat(exception.getMessage(),
            is("index/alias [alias2] provided with routing value [1,2] that resolved to several routing values, rejecting operation"));
        exception = expectThrows(IllegalArgumentException.class, () -> metaData.resolveWriteIndexRouting(randomFrom("1", null), "alias4"));
        assertThat(exception.getMessage(),
            is("index/alias [alias4] provided with routing value [1,2] that resolved to several routing values, rejecting operation"));

        // alias with no write index
        exception = expectThrows(IllegalArgumentException.class, () -> metaData.resolveWriteIndexRouting("1", "alias3"));
        assertThat(exception.getMessage(),
            is("alias [alias3] does not have a write index"));


        // aliases with multiple indices
        AliasMetaData.Builder aliasZeroBuilderTwo = AliasMetaData.builder("alias0");
        if (randomBoolean()) {
            aliasZeroBuilder.writeIndex(false);
        }
        IndexMetaData.Builder builder2 = IndexMetaData.builder("index2")
            .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(aliasZeroBuilderTwo.build())
            .putAlias(AliasMetaData.builder("alias1").routing("0").writeIndex(true).build())
            .putAlias(AliasMetaData.builder("alias2").writeIndex(true).build());
        MetaData metaDataTwoIndices = MetaData.builder(metaData).put(builder2).build();

        // verify that new write index is used
        assertThat("0", equalTo(metaDataTwoIndices.resolveWriteIndexRouting("0", "alias1")));
    }

    public void testUnknownFieldClusterMetaData() throws IOException {
        BytesReference metadata = BytesReference.bytes(JsonXContent.contentBuilder()
            .startObject()
                .startObject("meta-data")
                    .field("random", "value")
                .endObject()
            .endObject());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, metadata)) {
            MetaData.Builder.fromXContent(parser, randomBoolean());
            fail();
        } catch (IllegalArgumentException e) {
            assertEquals("Unexpected field [random]", e.getMessage());
        }
    }

    public void testUnknownFieldIndexMetaData() throws IOException {
        BytesReference metadata = BytesReference.bytes(JsonXContent.contentBuilder()
            .startObject()
                .startObject("index_name")
                    .field("random", "value")
                .endObject()
            .endObject());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, metadata)) {
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
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final MetaData fromXContentMeta = MetaData.fromXContent(parser);
            assertThat(fromXContentMeta.indexGraveyard(), equalTo(originalMeta.indexGraveyard()));
        }
    }

    public void testXContentClusterUUID() throws IOException {
        final MetaData originalMeta = MetaData.builder().clusterUUID(UUIDs.randomBase64UUID())
            .clusterUUIDCommitted(randomBoolean()).build();
        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        originalMeta.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final MetaData fromXContentMeta = MetaData.fromXContent(parser);
            assertThat(fromXContentMeta.clusterUUID(), equalTo(originalMeta.clusterUUID()));
            assertThat(fromXContentMeta.clusterUUIDCommitted(), equalTo(originalMeta.clusterUUIDCommitted()));
        }
    }

    public void testSerializationClusterUUID() throws IOException {
        final MetaData originalMeta = MetaData.builder().clusterUUID(UUIDs.randomBase64UUID())
            .clusterUUIDCommitted(randomBoolean()).build();
        final BytesStreamOutput out = new BytesStreamOutput();
        originalMeta.writeTo(out);
        NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(ClusterModule.getNamedWriteables());
        final MetaData fromStreamMeta = MetaData.readFrom(
            new NamedWriteableAwareStreamInput(out.bytes().streamInput(), namedWriteableRegistry)
        );
        assertThat(fromStreamMeta.clusterUUID(), equalTo(originalMeta.clusterUUID()));
        assertThat(fromStreamMeta.clusterUUIDCommitted(), equalTo(originalMeta.clusterUUIDCommitted()));
    }

    public void testMetaDataGlobalStateChangesOnClusterUUIDChanges() {
        final MetaData metaData1 = MetaData.builder().clusterUUID(UUIDs.randomBase64UUID()).clusterUUIDCommitted(randomBoolean()).build();
        final MetaData metaData2 = MetaData.builder(metaData1).clusterUUID(UUIDs.randomBase64UUID()).build();
        final MetaData metaData3 = MetaData.builder(metaData1).clusterUUIDCommitted(!metaData1.clusterUUIDCommitted()).build();
        assertFalse(MetaData.isGlobalStateEquals(metaData1, metaData2));
        assertFalse(MetaData.isGlobalStateEquals(metaData1, metaData3));
        final MetaData metaData4 = MetaData.builder(metaData2).clusterUUID(metaData1.clusterUUID()).build();
        assertTrue(MetaData.isGlobalStateEquals(metaData1, metaData4));
    }

    private static CoordinationMetaData.VotingConfiguration randomVotingConfig() {
        return new CoordinationMetaData.VotingConfiguration(Sets.newHashSet(generateRandomStringArray(randomInt(10), 20, false)));
    }

    private Set<VotingConfigExclusion> randomVotingConfigExclusions() {
        final int size = randomIntBetween(0, 10);
        final Set<VotingConfigExclusion> nodes = new HashSet<>(size);
        while (nodes.size() < size) {
            assertTrue(nodes.add(new VotingConfigExclusion(randomAlphaOfLength(10), randomAlphaOfLength(10))));
        }
        return nodes;
    }

    public void testXContentWithCoordinationMetaData() throws IOException {
        CoordinationMetaData originalMeta = new CoordinationMetaData(randomNonNegativeLong(), randomVotingConfig(), randomVotingConfig(),
                randomVotingConfigExclusions());

        MetaData metaData = MetaData.builder().coordinationMetaData(originalMeta).build();

        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        metaData.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            final CoordinationMetaData fromXContentMeta = MetaData.fromXContent(parser).coordinationMetaData();
            assertThat(fromXContentMeta, equalTo(originalMeta));
        }
    }

    public void testGlobalStateEqualsCoordinationMetaData() {
        CoordinationMetaData coordinationMetaData1 = new CoordinationMetaData(randomNonNegativeLong(), randomVotingConfig(),
                randomVotingConfig(), randomVotingConfigExclusions());
        MetaData metaData1 = MetaData.builder().coordinationMetaData(coordinationMetaData1).build();
        CoordinationMetaData coordinationMetaData2 = new CoordinationMetaData(randomNonNegativeLong(), randomVotingConfig(),
                randomVotingConfig(), randomVotingConfigExclusions());
        MetaData metaData2 = MetaData.builder().coordinationMetaData(coordinationMetaData2).build();

        assertTrue(MetaData.isGlobalStateEquals(metaData1, metaData1));
        assertFalse(MetaData.isGlobalStateEquals(metaData1, metaData2));
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

    public void testFindMappings() throws IOException {
        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("index1")
                    .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0))
                    .putMapping(FIND_MAPPINGS_TEST_ITEM))
                .put(IndexMetaData.builder("index2")
                    .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0))
                    .putMapping(FIND_MAPPINGS_TEST_ITEM)).build();

        {
            ImmutableOpenMap<String, MappingMetaData> mappings = metaData.findMappings(Strings.EMPTY_ARRAY,
                    MapperPlugin.NOOP_FIELD_FILTER);
            assertEquals(0, mappings.size());
        }
        {
            ImmutableOpenMap<String, MappingMetaData> mappings = metaData.findMappings(new String[]{"index1"},
                    MapperPlugin.NOOP_FIELD_FILTER);
            assertEquals(1, mappings.size());
            assertIndexMappingsNotFiltered(mappings, "index1");
        }
        {
            ImmutableOpenMap<String, MappingMetaData> mappings = metaData.findMappings(
                    new String[]{"index1", "index2"},
                    MapperPlugin.NOOP_FIELD_FILTER);
            assertEquals(2, mappings.size());
            assertIndexMappingsNotFiltered(mappings, "index1");
            assertIndexMappingsNotFiltered(mappings, "index2");
        }
    }

    public void testFindMappingsNoOpFilters() throws IOException {
        MappingMetaData originalMappingMetaData = new MappingMetaData("_doc",
                XContentHelper.convertToMap(JsonXContent.jsonXContent, FIND_MAPPINGS_TEST_ITEM, true));

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("index1")
                        .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0))
                        .putMapping(originalMappingMetaData)).build();

        {
            ImmutableOpenMap<String, MappingMetaData> mappings = metaData.findMappings(new String[]{"index1"},
                    MapperPlugin.NOOP_FIELD_FILTER);
            MappingMetaData mappingMetaData = mappings.get("index1");
            assertSame(originalMappingMetaData, mappingMetaData);
        }
        {
            ImmutableOpenMap<String, MappingMetaData> mappings = metaData.findMappings(new String[]{"index1"},
                    index -> field -> randomBoolean());
            MappingMetaData mappingMetaData = mappings.get("index1");
            assertNotSame(originalMappingMetaData, mappingMetaData);
        }
    }

    @SuppressWarnings("unchecked")
    public void testFindMappingsWithFilters() throws IOException {
        String mapping = FIND_MAPPINGS_TEST_ITEM;
        if (randomBoolean()) {
            Map<String, Object> stringObjectMap = XContentHelper.convertToMap(JsonXContent.jsonXContent, FIND_MAPPINGS_TEST_ITEM, false);
            Map<String, Object> doc = (Map<String, Object>)stringObjectMap.get("_doc");
            try (XContentBuilder builder = JsonXContent.contentBuilder()) {
                builder.map(doc);
                mapping = Strings.toString(builder);
            }
        }

        MetaData metaData = MetaData.builder()
                .put(IndexMetaData.builder("index1")
                        .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0))
                .putMapping(mapping))
                .put(IndexMetaData.builder("index2")
                        .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0))
                        .putMapping(mapping))
                .put(IndexMetaData.builder("index3")
                        .settings(Settings.builder().put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                                .put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0))
                        .putMapping(mapping)).build();

        {
            ImmutableOpenMap<String, MappingMetaData> mappings = metaData.findMappings(
                    new String[]{"index1", "index2", "index3"},
                    index -> {
                        if (index.equals("index1")) {
                            return field -> field.startsWith("name.") == false && field.startsWith("properties.key.") == false
                                    && field.equals("age") == false && field.equals("address.location") == false;
                        }
                        if (index.equals("index2")) {
                            return field -> false;
                        }
                        return MapperPlugin.NOOP_FIELD_PREDICATE;
                    });



            assertIndexMappingsNoFields(mappings, "index2");
            assertIndexMappingsNotFiltered(mappings, "index3");

            MappingMetaData docMapping = mappings.get("index1");
            assertNotNull(docMapping);

            Map<String, Object> sourceAsMap = docMapping.getSourceAsMap();
            assertEquals(3, sourceAsMap.size());
            assertTrue(sourceAsMap.containsKey("_routing"));
            assertTrue(sourceAsMap.containsKey("_source"));

            Map<String, Object> typeProperties = (Map<String, Object>) sourceAsMap.get("properties");
            assertEquals(6, typeProperties.size());
            assertTrue(typeProperties.containsKey("birth"));
            assertTrue(typeProperties.containsKey("ip"));
            assertTrue(typeProperties.containsKey("suggest"));

            Map<String, Object> name = (Map<String, Object>) typeProperties.get("name");
            assertNotNull(name);
            assertEquals(1, name.size());
            Map<String, Object> nameProperties = (Map<String, Object>) name.get("properties");
            assertNotNull(nameProperties);
            assertEquals(0, nameProperties.size());

            Map<String, Object> address = (Map<String, Object>) typeProperties.get("address");
            assertNotNull(address);
            assertEquals(2, address.size());
            assertTrue(address.containsKey("type"));
            Map<String, Object> addressProperties = (Map<String, Object>) address.get("properties");
            assertNotNull(addressProperties);
            assertEquals(2, addressProperties.size());
            assertLeafs(addressProperties, "street", "area");

            Map<String, Object> properties = (Map<String, Object>) typeProperties.get("properties");
            assertNotNull(properties);
            assertEquals(2, properties.size());
            assertTrue(properties.containsKey("type"));
            Map<String, Object> propertiesProperties = (Map<String, Object>) properties.get("properties");
            assertNotNull(propertiesProperties);
            assertEquals(2, propertiesProperties.size());
            assertLeafs(propertiesProperties, "key");
            assertMultiField(propertiesProperties, "value", "keyword");
        }

        {
            ImmutableOpenMap<String, MappingMetaData> mappings = metaData.findMappings(
                    new String[]{"index1", "index2" , "index3"},
                    index -> field -> (index.equals("index3") && field.endsWith("keyword")));

            assertIndexMappingsNoFields(mappings, "index1");
            assertIndexMappingsNoFields(mappings, "index2");
            MappingMetaData mappingMetaData = mappings.get("index3");
            Map<String, Object> sourceAsMap = mappingMetaData.getSourceAsMap();
            assertEquals(3, sourceAsMap.size());
            assertTrue(sourceAsMap.containsKey("_routing"));
            assertTrue(sourceAsMap.containsKey("_source"));
            Map<String, Object> typeProperties = (Map<String, Object>) sourceAsMap.get("properties");
            assertNotNull(typeProperties);
            assertEquals(1, typeProperties.size());
            Map<String, Object> properties = (Map<String, Object>) typeProperties.get("properties");
            assertNotNull(properties);
            assertEquals(2, properties.size());
            assertTrue(properties.containsKey("type"));
            Map<String, Object> propertiesProperties = (Map<String, Object>) properties.get("properties");
            assertNotNull(propertiesProperties);
            assertEquals(2, propertiesProperties.size());
            Map<String, Object> key = (Map<String, Object>) propertiesProperties.get("key");
            assertEquals(1, key.size());
            Map<String, Object> keyProperties = (Map<String, Object>) key.get("properties");
            assertEquals(1, keyProperties.size());
            assertLeafs(keyProperties, "keyword");
            Map<String, Object> value = (Map<String, Object>) propertiesProperties.get("value");
            assertEquals(1, value.size());
            Map<String, Object> valueProperties = (Map<String, Object>) value.get("properties");
            assertEquals(1, valueProperties.size());
            assertLeafs(valueProperties, "keyword");
        }

        {
            ImmutableOpenMap<String, MappingMetaData> mappings = metaData.findMappings(
                    new String[]{"index1", "index2" , "index3"},
                    index -> field -> (index.equals("index2")));

            assertIndexMappingsNoFields(mappings, "index1");
            assertIndexMappingsNoFields(mappings, "index3");
            assertIndexMappingsNotFiltered(mappings, "index2");
        }
    }

    private IndexMetaData.Builder buildIndexMetaData(String name, String alias, Boolean writeIndex) {
        return IndexMetaData.builder(name)
            .settings(settings(Version.CURRENT)).creationDate(randomNonNegativeLong())
            .putAlias(AliasMetaData.builder(alias).writeIndex(writeIndex))
            .numberOfShards(1).numberOfReplicas(0);
    }

    @SuppressWarnings("unchecked")
    private static void assertIndexMappingsNoFields(ImmutableOpenMap<String, MappingMetaData> mappings,
                                                    String index) {
        MappingMetaData docMapping = mappings.get(index);
        assertNotNull(docMapping);
        Map<String, Object> sourceAsMap = docMapping.getSourceAsMap();
        assertEquals(3, sourceAsMap.size());
        assertTrue(sourceAsMap.containsKey("_routing"));
        assertTrue(sourceAsMap.containsKey("_source"));
        Map<String, Object> typeProperties = (Map<String, Object>) sourceAsMap.get("properties");
        assertEquals(0, typeProperties.size());
    }

    @SuppressWarnings("unchecked")
    private static void assertIndexMappingsNotFiltered(ImmutableOpenMap<String, MappingMetaData> mappings,
                                                       String index) {
        MappingMetaData docMapping = mappings.get(index);
        assertNotNull(docMapping);

        Map<String, Object> sourceAsMap = docMapping.getSourceAsMap();
        assertEquals(3, sourceAsMap.size());
        assertTrue(sourceAsMap.containsKey("_routing"));
        assertTrue(sourceAsMap.containsKey("_source"));

        Map<String, Object> typeProperties = (Map<String, Object>) sourceAsMap.get("properties");
        assertEquals(7, typeProperties.size());
        assertTrue(typeProperties.containsKey("birth"));
        assertTrue(typeProperties.containsKey("age"));
        assertTrue(typeProperties.containsKey("ip"));
        assertTrue(typeProperties.containsKey("suggest"));

        Map<String, Object> name = (Map<String, Object>) typeProperties.get("name");
        assertNotNull(name);
        assertEquals(1, name.size());
        Map<String, Object> nameProperties = (Map<String, Object>) name.get("properties");
        assertNotNull(nameProperties);
        assertEquals(2, nameProperties.size());
        assertLeafs(nameProperties, "first", "last");

        Map<String, Object> address = (Map<String, Object>) typeProperties.get("address");
        assertNotNull(address);
        assertEquals(2, address.size());
        assertTrue(address.containsKey("type"));
        Map<String, Object> addressProperties = (Map<String, Object>) address.get("properties");
        assertNotNull(addressProperties);
        assertEquals(3, addressProperties.size());
        assertLeafs(addressProperties, "street", "location", "area");

        Map<String, Object> properties = (Map<String, Object>) typeProperties.get("properties");
        assertNotNull(properties);
        assertEquals(2, properties.size());
        assertTrue(properties.containsKey("type"));
        Map<String, Object> propertiesProperties = (Map<String, Object>) properties.get("properties");
        assertNotNull(propertiesProperties);
        assertEquals(2, propertiesProperties.size());
        assertMultiField(propertiesProperties, "key", "keyword");
        assertMultiField(propertiesProperties, "value", "keyword");
    }

    @SuppressWarnings("unchecked")
    public static void assertLeafs(Map<String, Object> properties, String... fields) {
        for (String field : fields) {
            assertTrue(properties.containsKey(field));
            Map<String, Object> fieldProp = (Map<String, Object>)properties.get(field);
            assertNotNull(fieldProp);
            assertFalse(fieldProp.containsKey("properties"));
            assertFalse(fieldProp.containsKey("fields"));
        }
    }

    public static void assertMultiField(Map<String, Object> properties, String field, String... subFields) {
        assertTrue(properties.containsKey(field));
        @SuppressWarnings("unchecked")
        Map<String, Object> fieldProp = (Map<String, Object>)properties.get(field);
        assertNotNull(fieldProp);
        assertTrue(fieldProp.containsKey("fields"));
        @SuppressWarnings("unchecked")
        Map<String, Object> subFieldsDef = (Map<String, Object>) fieldProp.get("fields");
        assertLeafs(subFieldsDef, subFields);
    }

    private static final String FIND_MAPPINGS_TEST_ITEM = "{\n" +
            "  \"_doc\": {\n" +
            "      \"_routing\": {\n" +
            "        \"required\":true\n" +
            "      }," +
            "      \"_source\": {\n" +
            "        \"enabled\":false\n" +
            "      }," +
            "      \"properties\": {\n" +
            "        \"name\": {\n" +
            "          \"properties\": {\n" +
            "            \"first\": {\n" +
            "              \"type\": \"keyword\"\n" +
            "            },\n" +
            "            \"last\": {\n" +
            "              \"type\": \"keyword\"\n" +
            "            }\n" +
            "          }\n" +
            "        },\n" +
            "        \"birth\": {\n" +
            "          \"type\": \"date\"\n" +
            "        },\n" +
            "        \"age\": {\n" +
            "          \"type\": \"integer\"\n" +
            "        },\n" +
            "        \"ip\": {\n" +
            "          \"type\": \"ip\"\n" +
            "        },\n" +
            "        \"suggest\" : {\n" +
            "          \"type\": \"completion\"\n" +
            "        },\n" +
            "        \"address\": {\n" +
            "          \"type\": \"object\",\n" +
            "          \"properties\": {\n" +
            "            \"street\": {\n" +
            "              \"type\": \"keyword\"\n" +
            "            },\n" +
            "            \"location\": {\n" +
            "              \"type\": \"geo_point\"\n" +
            "            },\n" +
            "            \"area\": {\n" +
            "              \"type\": \"geo_shape\",  \n" +
            "              \"tree\": \"quadtree\",\n" +
            "              \"precision\": \"1m\"\n" +
            "            }\n" +
            "          }\n" +
            "        },\n" +
            "        \"properties\": {\n" +
            "          \"type\": \"nested\",\n" +
            "          \"properties\": {\n" +
            "            \"key\" : {\n" +
            "              \"type\": \"text\",\n" +
            "              \"fields\": {\n" +
            "                \"keyword\" : {\n" +
            "                  \"type\" : \"keyword\"\n" +
            "                }\n" +
            "              }\n" +
            "            },\n" +
            "            \"value\" : {\n" +
            "              \"type\": \"text\",\n" +
            "              \"fields\": {\n" +
            "                \"keyword\" : {\n" +
            "                  \"type\" : \"keyword\"\n" +
            "                }\n" +
            "              }\n" +
            "            }\n" +
            "          }\n" +
            "        }\n" +
            "      }\n" +
            "    }\n" +
            "  }\n" +
            "}";

    public void testTransientSettingsOverridePersistentSettings() {
        final Setting setting = Setting.simpleString("key");
        final MetaData metaData = MetaData.builder()
            .persistentSettings(Settings.builder().put(setting.getKey(), "persistent-value").build())
            .transientSettings(Settings.builder().put(setting.getKey(), "transient-value").build()).build();
        assertThat(setting.get(metaData.settings()), equalTo("transient-value"));
    }

    public void testBuilderRejectsNullCustom() {
        final MetaData.Builder builder = MetaData.builder();
        final String key = randomAlphaOfLength(10);
        assertThat(expectThrows(NullPointerException.class, () -> builder.putCustom(key, null)).getMessage(), containsString(key));
    }

    public void testBuilderRejectsNullInCustoms() {
        final MetaData.Builder builder = MetaData.builder();
        final String key = randomAlphaOfLength(10);
        final ImmutableOpenMap.Builder<String, MetaData.Custom> mapBuilder = ImmutableOpenMap.builder();
        mapBuilder.put(key, null);
        final ImmutableOpenMap<String, MetaData.Custom> map = mapBuilder.build();
        assertThat(expectThrows(NullPointerException.class, () -> builder.customs(map)).getMessage(), containsString(key));
    }
}
