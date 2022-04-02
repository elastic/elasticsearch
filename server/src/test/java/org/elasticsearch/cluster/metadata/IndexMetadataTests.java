/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxPrimaryShardDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxPrimaryShardSizeCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxSizeCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesModule;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_HIDDEN_SETTING;
import static org.elasticsearch.cluster.metadata.IndexMetadata.parseIndexNameCounter;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class IndexMetadataTests extends ESTestCase {

    @Before
    public void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        return new NamedWriteableRegistry(IndicesModule.getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(IndicesModule.getNamedXContents());
    }

    public void testIndexMetadataSerialization() throws IOException {
        Integer numShard = randomFrom(1, 2, 4, 8, 16);
        int numberOfReplicas = randomIntBetween(0, 10);
        final boolean system = randomBoolean();
        Map<String, String> customMap = new HashMap<>();
        customMap.put(randomAlphaOfLength(5), randomAlphaOfLength(10));
        customMap.put(randomAlphaOfLength(10), randomAlphaOfLength(15));
        IndexMetadata metadata = IndexMetadata.builder("foo")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1)
                    .put("index.number_of_shards", numShard)
                    .put("index.number_of_replicas", numberOfReplicas)
                    .build()
            )
            .creationDate(randomLong())
            .primaryTerm(0, 2)
            .setRoutingNumShards(32)
            .system(system)
            .putCustom("my_custom", customMap)
            .putRolloverInfo(
                new RolloverInfo(
                    randomAlphaOfLength(5),
                    List.of(
                        new MaxAgeCondition(TimeValue.timeValueMillis(randomNonNegativeLong())),
                        new MaxDocsCondition(randomNonNegativeLong()),
                        new MaxSizeCondition(new ByteSizeValue(randomNonNegativeLong())),
                        new MaxPrimaryShardSizeCondition(new ByteSizeValue(randomNonNegativeLong())),
                        new MaxPrimaryShardDocsCondition(randomNonNegativeLong())
                    ),
                    randomNonNegativeLong()
                )
            )
            .build();
        assertEquals(system, metadata.isSystem());

        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        IndexMetadata.FORMAT.toXContent(builder, metadata);
        builder.endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        final IndexMetadata fromXContentMeta = IndexMetadata.fromXContent(parser);
        assertEquals(
            "expected: " + Strings.toString(metadata) + "\nactual  : " + Strings.toString(fromXContentMeta),
            metadata,
            fromXContentMeta
        );
        assertEquals(metadata.hashCode(), fromXContentMeta.hashCode());

        assertEquals(metadata.getNumberOfReplicas(), fromXContentMeta.getNumberOfReplicas());
        assertEquals(metadata.getNumberOfShards(), fromXContentMeta.getNumberOfShards());
        assertEquals(metadata.getCreationVersion(), fromXContentMeta.getCreationVersion());
        assertEquals(metadata.getCompatibilityVersion(), fromXContentMeta.getCompatibilityVersion());
        assertEquals(metadata.getRoutingNumShards(), fromXContentMeta.getRoutingNumShards());
        assertEquals(metadata.getCreationDate(), fromXContentMeta.getCreationDate());
        assertEquals(metadata.getRoutingFactor(), fromXContentMeta.getRoutingFactor());
        assertEquals(metadata.primaryTerm(0), fromXContentMeta.primaryTerm(0));
        assertEquals(metadata.isSystem(), fromXContentMeta.isSystem());
        ImmutableOpenMap.Builder<String, DiffableStringMap> expectedCustomBuilder = ImmutableOpenMap.builder();
        expectedCustomBuilder.put("my_custom", new DiffableStringMap(customMap));
        ImmutableOpenMap<String, DiffableStringMap> expectedCustom = expectedCustomBuilder.build();
        assertEquals(metadata.getCustomData(), expectedCustom);
        assertEquals(metadata.getCustomData(), fromXContentMeta.getCustomData());

        final BytesStreamOutput out = new BytesStreamOutput();
        metadata.writeTo(out);
        try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), writableRegistry())) {
            IndexMetadata deserialized = IndexMetadata.readFrom(in);
            assertEquals(metadata, deserialized);
            assertEquals(metadata.hashCode(), deserialized.hashCode());

            assertEquals(metadata.getNumberOfReplicas(), deserialized.getNumberOfReplicas());
            assertEquals(metadata.getNumberOfShards(), deserialized.getNumberOfShards());
            assertEquals(metadata.getCreationVersion(), deserialized.getCreationVersion());
            assertEquals(metadata.getCompatibilityVersion(), deserialized.getCompatibilityVersion());
            assertEquals(metadata.getRoutingNumShards(), deserialized.getRoutingNumShards());
            assertEquals(metadata.getCreationDate(), deserialized.getCreationDate());
            assertEquals(metadata.getRoutingFactor(), deserialized.getRoutingFactor());
            assertEquals(metadata.primaryTerm(0), deserialized.primaryTerm(0));
            assertEquals(metadata.getRolloverInfos(), deserialized.getRolloverInfos());
            assertEquals(deserialized.getCustomData(), expectedCustom);
            assertEquals(metadata.getCustomData(), deserialized.getCustomData());
            assertEquals(metadata.isSystem(), deserialized.isSystem());
        }
    }

    public void testGetRoutingFactor() {
        Integer numShard = randomFrom(1, 2, 4, 8, 16);
        int routingFactor = IndexMetadata.getRoutingFactor(32, numShard);
        assertEquals(routingFactor * numShard, 32);

        Integer brokenNumShards = randomFrom(3, 5, 9, 12, 29, 42);
        expectThrows(IllegalArgumentException.class, () -> IndexMetadata.getRoutingFactor(32, brokenNumShards));
    }

    public void testSelectShrinkShards() {
        int numberOfReplicas = randomIntBetween(0, 10);
        IndexMetadata metadata = IndexMetadata.builder("foo")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1)
                    .put("index.number_of_shards", 32)
                    .put("index.number_of_replicas", numberOfReplicas)
                    .build()
            )
            .creationDate(randomLong())
            .build();
        Set<ShardId> shardIds = IndexMetadata.selectShrinkShards(0, metadata, 8);
        assertEquals(
            shardIds,
            Sets.newHashSet(
                new ShardId(metadata.getIndex(), 0),
                new ShardId(metadata.getIndex(), 1),
                new ShardId(metadata.getIndex(), 2),
                new ShardId(metadata.getIndex(), 3)
            )
        );
        shardIds = IndexMetadata.selectShrinkShards(1, metadata, 8);
        assertEquals(
            shardIds,
            Sets.newHashSet(
                new ShardId(metadata.getIndex(), 4),
                new ShardId(metadata.getIndex(), 5),
                new ShardId(metadata.getIndex(), 6),
                new ShardId(metadata.getIndex(), 7)
            )
        );
        shardIds = IndexMetadata.selectShrinkShards(7, metadata, 8);
        assertEquals(
            shardIds,
            Sets.newHashSet(
                new ShardId(metadata.getIndex(), 28),
                new ShardId(metadata.getIndex(), 29),
                new ShardId(metadata.getIndex(), 30),
                new ShardId(metadata.getIndex(), 31)
            )
        );

        assertEquals(
            "the number of target shards (8) must be greater than the shard id: 8",
            expectThrows(IllegalArgumentException.class, () -> IndexMetadata.selectShrinkShards(8, metadata, 8)).getMessage()
        );
    }

    public void testSelectResizeShards() {
        int numTargetShards = randomFrom(4, 6, 8, 12);

        IndexMetadata split = IndexMetadata.builder("foo")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1)
                    .put("index.number_of_shards", 2)
                    .put("index.number_of_replicas", 0)
                    .build()
            )
            .creationDate(randomLong())
            .setRoutingNumShards(numTargetShards * 2)
            .build();

        IndexMetadata shrink = IndexMetadata.builder("foo")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1)
                    .put("index.number_of_shards", 32)
                    .put("index.number_of_replicas", 0)
                    .build()
            )
            .creationDate(randomLong())
            .build();
        int shard = randomIntBetween(0, numTargetShards - 1);
        assertEquals(
            Collections.singleton(IndexMetadata.selectSplitShard(shard, split, numTargetShards)),
            IndexMetadata.selectRecoverFromShards(shard, split, numTargetShards)
        );

        numTargetShards = randomFrom(1, 2, 4, 8, 16);
        shard = randomIntBetween(0, numTargetShards - 1);
        assertEquals(
            IndexMetadata.selectShrinkShards(shard, shrink, numTargetShards),
            IndexMetadata.selectRecoverFromShards(shard, shrink, numTargetShards)
        );

        IndexMetadata.selectRecoverFromShards(0, shrink, 32);
    }

    public void testSelectSplitShard() {
        IndexMetadata metadata = IndexMetadata.builder("foo")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1)
                    .put("index.number_of_shards", 2)
                    .put("index.number_of_replicas", 0)
                    .build()
            )
            .creationDate(randomLong())
            .setRoutingNumShards(4)
            .build();
        ShardId shardId = IndexMetadata.selectSplitShard(0, metadata, 4);
        assertEquals(0, shardId.getId());
        shardId = IndexMetadata.selectSplitShard(1, metadata, 4);
        assertEquals(0, shardId.getId());
        shardId = IndexMetadata.selectSplitShard(2, metadata, 4);
        assertEquals(1, shardId.getId());
        shardId = IndexMetadata.selectSplitShard(3, metadata, 4);
        assertEquals(1, shardId.getId());

        assertEquals(
            "the number of target shards (0) must be greater than the shard id: 0",
            expectThrows(IllegalArgumentException.class, () -> IndexMetadata.selectSplitShard(0, metadata, 0)).getMessage()
        );

        assertEquals(
            "the number of source shards [2] must be a factor of [3]",
            expectThrows(IllegalArgumentException.class, () -> IndexMetadata.selectSplitShard(0, metadata, 3)).getMessage()
        );

        assertEquals(
            "the number of routing shards [4] must be a multiple of the target shards [8]",
            expectThrows(IllegalStateException.class, () -> IndexMetadata.selectSplitShard(0, metadata, 8)).getMessage()
        );
    }

    public void testIndexFormat() {
        Settings defaultSettings = Settings.builder()
            .put("index.version.created", 1)
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 1)
            .build();

        // matching version
        {
            IndexMetadata metadata = IndexMetadata.builder("foo")
                .settings(
                    Settings.builder()
                        .put(defaultSettings)
                        // intentionally not using the constant, so upgrading requires you to look at this test
                        // where you have to update this part and the next one
                        .put("index.format", 6)
                        .build()
                )
                .build();

            assertThat(metadata.getSettings().getAsInt(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), 0), is(6));
        }

        // no setting configured
        {
            IndexMetadata metadata = IndexMetadata.builder("foo").settings(Settings.builder().put(defaultSettings).build()).build();
            assertThat(metadata.getSettings().getAsInt(IndexMetadata.INDEX_FORMAT_SETTING.getKey(), 0), is(0));
        }
    }

    public void testNumberOfRoutingShards() {
        Settings build = Settings.builder().put("index.number_of_shards", 5).put("index.number_of_routing_shards", 10).build();
        assertEquals(10, IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(build).intValue());

        build = Settings.builder().put("index.number_of_shards", 5).put("index.number_of_routing_shards", 5).build();
        assertEquals(5, IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(build).intValue());

        int numShards = randomIntBetween(1, 10);
        build = Settings.builder().put("index.number_of_shards", numShards).build();
        assertEquals(numShards, IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(build).intValue());

        Settings lessThanSettings = Settings.builder().put("index.number_of_shards", 8).put("index.number_of_routing_shards", 4).build();
        IllegalArgumentException iae = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(lessThanSettings)
        );
        assertEquals("index.number_of_routing_shards [4] must be >= index.number_of_shards [8]", iae.getMessage());

        Settings notAFactorySettings = Settings.builder().put("index.number_of_shards", 2).put("index.number_of_routing_shards", 3).build();
        iae = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.INDEX_NUMBER_OF_ROUTING_SHARDS_SETTING.get(notAFactorySettings)
        );
        assertEquals("the number of source shards [2] must be a factor of [3]", iae.getMessage());
    }

    public void testMissingNumberOfShards() {
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> IndexMetadata.builder("test").build());
        assertThat(e.getMessage(), containsString("must specify number of shards for index [test]"));
    }

    public void testNumberOfShardsIsNotZero() {
        runTestNumberOfShardsIsPositive(0);
    }

    public void testNumberOfShardsIsNotNegative() {
        runTestNumberOfShardsIsPositive(-randomIntBetween(1, Integer.MAX_VALUE));
    }

    private void runTestNumberOfShardsIsPositive(final int numberOfShards) {
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numberOfShards).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.builder("test").settings(settings).build()
        );
        assertThat(
            e.getMessage(),
            equalTo("Failed to parse value [" + numberOfShards + "] for setting [index.number_of_shards] must be >= 1")
        );
    }

    public void testMissingCreatedVersion() {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
            .build();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.builder("test").settings(settings).build()
        );
        assertThat(e.getMessage(), containsString("[index.version.created] is not present"));
    }

    public void testMissingNumberOfReplicas() {
        final Settings settings = Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 8)).build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.builder("test").settings(settings).build()
        );
        assertThat(e.getMessage(), containsString("must specify number of replicas for index [test]"));
    }

    public void testNumberOfReplicasIsNonNegative() {
        final int numberOfReplicas = -randomIntBetween(1, Integer.MAX_VALUE);
        final Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 8))
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas)
            .build();
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> IndexMetadata.builder("test").settings(settings).build()
        );
        assertThat(
            e.getMessage(),
            equalTo("Failed to parse value [" + numberOfReplicas + "] for setting [index.number_of_replicas] must be >= 0")
        );
    }

    public void testParseIndexNameReturnsCounter() {
        assertThat(parseIndexNameCounter(".ds-logs-000003"), is(3));
        assertThat(parseIndexNameCounter("shrink-logs-000003"), is(3));
    }

    public void testParseIndexNameSupportsDateMathPattern() {
        assertThat(parseIndexNameCounter("<logs-{now/d}-1>"), is(1));
    }

    public void testParseIndexNameThrowExceptionWhenNoSeparatorIsPresent() {
        try {
            parseIndexNameCounter("testIndexNameWithoutDash");
            fail("expected to fail as the index name contains no - separator");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("no - separator found in index name [testIndexNameWithoutDash]"));
        }
    }

    public void testParseIndexNameCannotFormatNumber() {
        try {
            parseIndexNameCounter("testIndexName-000a2");
            fail("expected to fail as the index name doesn't end with digits");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), is("unable to parse the index name [testIndexName-000a2] to extract the counter"));
        }
    }

    public void testIsHidden() {
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 8))
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);
        IndexMetadata indexMetadata = IndexMetadata.builder("test").settings(settings).build();
        assertFalse(indexMetadata.isHidden());

        settings.put(INDEX_HIDDEN_SETTING.getKey(), "false");
        indexMetadata = IndexMetadata.builder(indexMetadata).settings(settings).build();
        assertFalse(indexMetadata.isHidden());

        settings.put(INDEX_HIDDEN_SETTING.getKey(), "true");
        indexMetadata = IndexMetadata.builder(indexMetadata).settings(settings).build();
        assertTrue(indexMetadata.isHidden());

        indexMetadata = IndexMetadata.builder(indexMetadata).build();
        assertTrue(indexMetadata.isHidden()); // preserved if settings unchanged
    }

    public void testGetTierPreference() {
        final Settings indexSettings = indexSettingsWithDataTier("data_warm,data_cold");
        final IndexMetadata indexMetadata = IndexMetadata.builder("myindex").settings(indexSettings).build();
        assertThat(indexMetadata.getTierPreference(), is(DataTier.parseTierList(DataTier.TIER_PREFERENCE_SETTING.get(indexSettings))));
        assertThat(indexMetadata.getTierPreference(), is(List.of(DataTier.DATA_WARM, DataTier.DATA_COLD)));

    }

    public void testBuildsWithBrokenTierPreference() {
        final Settings indexSettings = indexSettingsWithDataTier("broken_tier");
        final IndexMetadata indexMetadata = IndexMetadata.builder("myindex").settings(indexSettings).build();
        expectThrows(IllegalArgumentException.class, indexMetadata::getTierPreference);
    }

    public void testLifeCyclePolicyName() {
        Settings.Builder settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, randomIntBetween(1, 8))
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT);

        IndexMetadata idxMeta1 = IndexMetadata.builder("test").settings(settings).build();

        // null means no policy
        assertNull(idxMeta1.getLifecyclePolicyName());

        IndexMetadata idxMeta2 = IndexMetadata.builder(idxMeta1)
            .settings(settings.put(IndexMetadata.LIFECYCLE_NAME, "some_policy").build())
            .build();

        assertThat(idxMeta2.getLifecyclePolicyName(), equalTo("some_policy"));
    }

    private static Settings indexSettingsWithDataTier(String dataTier) {
        return Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(DataTier.TIER_PREFERENCE, dataTier)
            .build();
    }
}
