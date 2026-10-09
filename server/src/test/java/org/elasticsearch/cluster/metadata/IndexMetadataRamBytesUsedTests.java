/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxSizeCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.AbstractAccountableFieldsTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Structural tripwire and behavioural assertions for {@link IndexMetadata#ramBytesUsed()}.
 */
public class IndexMetadataRamBytesUsedTests extends AbstractAccountableFieldsTestCase {

    @Override
    protected Class<? extends Accountable> classUnderTest() {
        return IndexMetadata.class;
    }

    @Override
    protected Set<String> fieldsAccountedForInRamBytesUsed() {
        return Set.of(
            "index",
            "settings",
            "mapping",
            "primaryTerms",
            "inSyncAllocationIds",
            "aliases",
            "customData",
            "inferenceFields",
            "rolloverInfos",
            "transportVersion",
            "routingPaths",
            "timeSeriesDimensions",
            "requireFilters",
            "includeFilters",
            "excludeFilters",
            "initialRecoveryFilters",
            "indexCreatedVersion",
            "mappingsUpdatedVersion",
            "indexCompatibilityVersion",
            "waitForActiveShards",
            "timestampRange",
            "eventIngestedRange",
            "tierPreference",
            "lifecyclePolicyName",
            "lifecycleExecutionState",
            "autoExpandReplicas",
            "timeSeriesStart",
            "timeSeriesEnd",
            "stats",
            "writeLoadForecast",
            "shardSizeInBytesForecast",
            "reshardingMetadata"
        );
    }

    @Override
    protected Set<String> fieldsExcludedFromRamBytesUsed() {
        // Shared enum singletons; only the field references are counted in BASE_RAM_BYTES_USED.
        return Set.of("state", "indexMode");
    }

    @Override
    protected boolean assertsAgainstRamUsageTester() {
        // Settings.estimatedRamBytesUsed() deliberately omits interned keys/values; a full-graph RamUsageTester walk would count them
        // and fail estimate >= actual by design. Structural + behavioural checks below cover this class instead.
        return false;
    }

    @Override
    protected Accountable createRandomTestInstance() {
        int shards = randomIntBetween(1, 8);
        return IndexMetadata.builder(randomAlphaOfLengthBetween(3, 12))
            .settings(indexSettings(shards, 0).put("index.version.created", IndexVersion.current().id()))
            .build();
    }

    public void testRamBytesUsedIncreasesWithMapping() throws IOException {
        Settings settings = indexSettings(1, 0).put("index.version.created", IndexVersion.current().id()).build();
        IndexMetadata withoutMapping = IndexMetadata.builder("test").settings(settings).build();
        long withoutMappingBytes = withoutMapping.ramBytesUsed();

        String mappingJson = """
            {
              "_doc": {
                "properties": {
                  "field": { "type": "keyword" }
                }
              }
            }
            """;
        IndexMetadata withMapping = IndexMetadata.builder("test")
            .settings(settings)
            .putMapping(new MappingMetadata(CompressedXContent.fromJSON(mappingJson)))
            .build();
        long withMappingBytes = withMapping.ramBytesUsed();

        assertThat(withMappingBytes, greaterThan(withoutMappingBytes));
        assertThat(
            withMappingBytes - withoutMappingBytes,
            greaterThanOrEqualTo((long) withMapping.mapping().source().compressedReference().length())
        );
    }

    public void testRamBytesUsedScalesWithShardCount() {
        Settings settings = indexSettings(1, 0).put("index.version.created", IndexVersion.current().id()).build();
        IndexMetadata oneShard = IndexMetadata.builder("test").settings(settings).build();
        Settings manyShardSettings = indexSettings(32, 0).put("index.version.created", IndexVersion.current().id()).build();
        IndexMetadata manyShards = IndexMetadata.builder("test").settings(manyShardSettings).build();

        assertThat(manyShards.ramBytesUsed(), greaterThan(oneShard.ramBytesUsed()));
        assertThat(manyShards.ramBytesUsed() - oneShard.ramBytesUsed(), greaterThanOrEqualTo(31L * RamUsageEstimator.NUM_BYTES_OBJECT_REF));
    }

    @SuppressForbidden(reason = "reflectively inspects the private memoization field to verify caching")
    public void testRamBytesUsedIsMemoized() throws Exception {
        Settings settings = indexSettings(1, 0).put("index.version.created", IndexVersion.current().id()).build();
        IndexMetadata metadata = IndexMetadata.builder("test")
            .settings(settings)
            .putMapping(new MappingMetadata(CompressedXContent.fromJSON("""
                { "_doc": { "properties": { "field": { "type": "keyword" } } } }
                """)))
            .build();

        var memoField = IndexMetadata.class.getDeclaredField("ramBytesUsed");
        memoField.setAccessible(true);
        assertThat(memoField.getLong(metadata), equalTo(-1L));

        long computed = metadata.ramBytesUsed();
        assertThat(computed, greaterThan(0L));
        assertThat(memoField.getLong(metadata), equalTo(computed));

        assertThat(metadata.ramBytesUsed(), equalTo(computed));
        assertThat(memoField.getLong(metadata), equalTo(computed));
    }

    public void testRamBytesUsedIncludesRoutingPaths() {
        Settings base = indexSettings(1, 0).put("index.version.created", IndexVersion.current().id()).build();
        IndexMetadata withoutPaths = IndexMetadata.builder("test").settings(base).build();
        Settings withPathSettings = Settings.builder().put(base).putList("index.routing_path", "dim1", "dim2", "dim3").build();
        IndexMetadata withPaths = IndexMetadata.builder("test").settings(withPathSettings).build();

        assertThat(withPaths.ramBytesUsed(), greaterThan(withoutPaths.ramBytesUsed()));
    }

    public void testRamBytesUsedIncludesAliasMetadata() {
        Settings settings = indexSettings(1, 0).put("index.version.created", IndexVersion.current().id()).build();
        IndexMetadata withoutAlias = IndexMetadata.builder("test").settings(settings).build();
        IndexMetadata withAlias = IndexMetadata.builder("test")
            .settings(settings)
            .putAlias(
                AliasMetadata.builder("alias")
                    .filter("{\"term\":{\"field\":\"value\"}}")
                    .indexRouting("routing")
                    .searchRouting("sr1,sr2")
                    .writeIndex(true)
                    .build()
            )
            .build();

        assertThat(withAlias.ramBytesUsed(), greaterThan(withoutAlias.ramBytesUsed()));
    }

    public void testRamBytesUsedIncludesRolloverMetConditions() {
        Settings settings = indexSettings(1, 0).put("index.version.created", IndexVersion.current().id()).build();
        IndexMetadata withoutRollover = IndexMetadata.builder("test").settings(settings).build();
        IndexMetadata withRollover = IndexMetadata.builder("test")
            .settings(settings)
            .putRolloverInfo(
                new RolloverInfo(
                    "alias",
                    List.of(
                        new MaxDocsCondition(1_000L),
                        new MaxAgeCondition(TimeValue.timeValueDays(1)),
                        new MaxSizeCondition(ByteSizeValue.ofMb(1))
                    ),
                    System.currentTimeMillis()
                )
            )
            .build();

        assertThat(withRollover.ramBytesUsed(), greaterThan(withoutRollover.ramBytesUsed()));
    }

    public void testRamBytesUsedIncludesDiscoveryNodeFilters() {
        Settings base = indexSettings(1, 0).put("index.version.created", IndexVersion.current().id()).build();
        IndexMetadata withoutFilters = IndexMetadata.builder("test").settings(base).build();
        Settings withFilterSettings = Settings.builder()
            .put(base)
            .put("index.routing.allocation.require._id", "node-1,node-2")
            .put("index.routing.allocation.require.rack", "r1")
            .build();
        IndexMetadata withFilters = IndexMetadata.builder("test").settings(withFilterSettings).build();

        assertThat(withFilters.ramBytesUsed(), greaterThan(withoutFilters.ramBytesUsed()));
    }

    /**
     * {@code customData} is nested ({@code Map<String, DiffableStringMap>}); lengthening an inner string value must increase the estimate.
     */
    public void testRamBytesUsedIncludesDeepCustomData() {
        Settings settings = indexSettings(1, 0).put("index.version.created", IndexVersion.current().id()).build();
        IndexMetadata withoutCustom = IndexMetadata.builder("test").settings(settings).build();
        IndexMetadata withShortCustom = IndexMetadata.builder("test")
            .settings(settings)
            .putCustom("my_custom", Map.of("key", "short"))
            .build();
        IndexMetadata withLongCustom = IndexMetadata.builder("test")
            .settings(settings)
            .putCustom("my_custom", Map.of("key", "x".repeat(256)))
            .build();

        assertThat(withShortCustom.ramBytesUsed(), greaterThan(withoutCustom.ramBytesUsed()));
        assertThat(withLongCustom.ramBytesUsed(), greaterThan(withShortCustom.ramBytesUsed()));
    }

    /**
     * {@code inSyncAllocationIds} is nested ({@code Map<Integer, Set<String>>}); lengthening a set value must increase the estimate.
     */
    public void testRamBytesUsedIncludesInSyncAllocationIdValues() {
        Settings settings = indexSettings(1, 0).put("index.version.created", IndexVersion.current().id()).build();
        IndexMetadata withShortIds = IndexMetadata.builder("test").settings(settings).putInSyncAllocationIds(0, Set.of("short")).build();
        IndexMetadata withLongIds = IndexMetadata.builder("test")
            .settings(settings)
            .putInSyncAllocationIds(0, Set.of("x".repeat(256)))
            .build();

        assertThat(withLongIds.ramBytesUsed(), greaterThan(withShortIds.ramBytesUsed()));
    }

    /**
     * Non-tautology check for a fixed minimal {@link IndexMetadata}: the estimate must exceed an independent lower bound derived from
     * literal string lengths, {@link Settings#estimatedRamBytesUsed()}, and primary-term array length — without replicating
     * {@link IndexMetadata#computeRamBytesUsed()}. A settings-key delta verifies that index-level accounting tracks settings growth.
     */
    public void testRamBytesUsedMinimalIndexMetadataHandComputed() {
        Settings settings = indexSettings(1, 0).put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current().id())
            .put(IndexMetadata.SETTING_INDEX_UUID, "00000000-0000-0000-0000-000000000001")
            .build();
        IndexMetadata metadata = IndexMetadata.builder("idx").settings(settings).build();
        long actual = metadata.ramBytesUsed();

        long settingsBytes = settings.estimatedRamBytesUsed();
        long independentLowerBound = RamUsageEstimator.shallowSizeOfInstance(IndexMetadata.class) + metadata.getIndex().ramBytesUsed()
            + settingsBytes + RamUsageEstimator.sizeOf(new long[1]);
        assertThat(actual, greaterThanOrEqualTo(independentLowerBound));

        Settings withExtraSetting = Settings.builder().put(settings).put("index.refresh_interval", "1s").build();
        IndexMetadata metadataWithExtraSetting = IndexMetadata.builder("idx").settings(withExtraSetting).build();
        long settingsDelta = withExtraSetting.estimatedRamBytesUsed() - settingsBytes;
        assertThat(settingsDelta, greaterThan(0L));
        assertThat(metadataWithExtraSetting.ramBytesUsed() - actual, equalTo(settingsDelta));
    }
}
