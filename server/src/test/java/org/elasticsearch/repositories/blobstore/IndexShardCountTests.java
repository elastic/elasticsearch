/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.blobstore;

import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxPrimaryShardDocsCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxPrimaryShardSizeCondition;
import org.elasticsearch.action.admin.indices.rollover.MaxSizeCondition;
import org.elasticsearch.action.admin.indices.rollover.OptimalShardCountCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadataStats;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexWriteLoad;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadata;
import org.elasticsearch.cluster.metadata.InferenceFieldMetadataTests;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.shard.IndexLongFieldRange;
import org.elasticsearch.index.shard.ShardLongFieldRange;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.IndexMetadata.KEY_SETTINGS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;

public class IndexShardCountTests extends ESTestCase {
    public void testFromIndexMetaDataWithValidIndexMetaDataObject() throws IOException {
        int numberOfShards = randomFrom(1, 2, 4, 8, 16);
        IndexMetadata indexMetadata = randomIndexMetadata(numberOfShards);

        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        IndexMetadata.FORMAT.toXContent(builder, indexMetadata);
        builder.endObject();
        XContentParser parser = createParser(builder);

        IndexShardCount count = IndexShardCount.fromIndexMetadata(parser);
        assertEquals(numberOfShards, count.count());
    }

    public void testFromIndexMetaDataWithValidIndexMetaDataObjectWithoutEventIngestedField() throws IOException {
        int numberOfShards = randomFrom(1, 2, 4, 8, 16);
        IndexMetadata indexMetadata = randomIndexMetadata(numberOfShards);

        final XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        IndexMetadata.FORMAT.toXContent(builder, indexMetadata);
        builder.endObject();
        XContentParser parser = createParser(builder);

        // convert XContent to a map and remove the IndexMetadata.KEY_EVENT_INGESTED_RANGE entry
        // to simulate IndexMetadata from an older cluster version (before TransportVersions.EVENT_INGESTED_RANGE_IN_CLUSTER_STATE)
        Map<String, Object> indexMetadataMap = XContentHelper.convertToMap(BytesReference.bytes(builder), true, XContentType.JSON).v2();
        removeEventIngestedField(indexMetadataMap);

        IndexShardCount count = IndexShardCount.fromIndexMetadata(parser);
        assertEquals(numberOfShards, count.count());
    }

    public void testFromIndexMetaDataWithoutNumberOfShardsSettingReturnsNegativeOne() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .field("my_index")
            .startObject()
            .startObject(KEY_SETTINGS)
            // no shard count
            .endObject()
            .endObject()
            .endObject();
        XContentParser parser = createParser(builder);

        IndexShardCount count = IndexShardCount.fromIndexMetadata(parser);
        assertEquals(-1, count.count());
    }

    public void testFromIndexMetaDataWithNestedSettingsThrowsIllegalArgumentException() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
            .startObject()
            .field("my_index")
            .startObject()
            .startObject(KEY_SETTINGS)
            .startObject("index")
            .field("number_of_shards", randomNonNegativeInt())
            .endObject()
            .endObject()
            .endObject()
            .endObject();
        XContentParser parser = createParser(builder);

        assertThrows(IllegalArgumentException.class, () -> IndexShardCount.fromIndexMetadata(parser));
    }

    // IndexMetadata specifies two parsing methods legacyFromXContent and fromXContent to be used depending
    // on the IndexVersion. Since we are only reading the shard count, we should succeed in either case
    public void testFromIndexMetaDataWithOldVersionSucceeds() throws IOException {
        int numberOfShards = randomFrom(1, 2, 4, 8, 16);
        XContentBuilder indexMetadataBuilder = buildLegacyIndexMetadata(
            numberOfShards,
            IndexVersion.getMinimumCompatibleIndexVersion(1_000_000)
        );
        XContentParser parser = createParser(indexMetadataBuilder);
        IndexShardCount count = IndexShardCount.fromIndexMetadata(parser);
        assertEquals(numberOfShards, count.count());
    }

    private XContentBuilder buildLegacyIndexMetadata(int numberOfShards, IndexVersion compatibilityVersion) throws IOException {
        return XContentFactory.jsonBuilder()
            .startObject()
            .field("my_index")
            .startObject()
            .startObject(KEY_SETTINGS)
            .field(SETTING_NUMBER_OF_SHARDS, numberOfShards)
            .field("index.version.compatibility", compatibilityVersion)
            .endObject()
            .endObject()
            .endObject();
    }

    private Map<String, String> randomCustomMap() {
        Map<String, String> customMap = new HashMap<>();
        customMap.put(randomAlphaOfLength(5), randomAlphaOfLength(10));
        customMap.put(randomAlphaOfLength(10), randomAlphaOfLength(15));
        return customMap;
    }

    private IndexMetadata randomIndexMetadata(int numberOfShards) {
        final boolean system = randomBoolean();
        Map<String, String> customMap = randomCustomMap();
        return randomIndexMetadata(numberOfShards, system, customMap);
    }

    private IndexMetadata randomIndexMetadata(int numberOfShards, boolean system, Map<String, String> customMap) {
        int numberOfReplicas = randomIntBetween(0, 10);
        IndexVersion mappingsUpdatedVersion = IndexVersionUtils.randomVersion();
        IndexMetadataStats indexStats = randomBoolean() ? randomIndexStats(numberOfShards) : null;
        Double indexWriteLoadForecast = randomBoolean() ? randomDoubleBetween(0.0, 128, true) : null;
        Long shardSizeInBytesForecast = randomBoolean() ? randomLongBetween(1024, 10240) : null;
        Map<String, InferenceFieldMetadata> inferenceFields = randomInferenceFields();
        IndexReshardingMetadata reshardingMetadata = randomBoolean() ? randomIndexReshardingMetadata(numberOfShards) : null;

        return IndexMetadata.builder("foo")
            .settings(randomSettings(numberOfShards, numberOfReplicas))
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
                        new MaxSizeCondition(ByteSizeValue.ofBytes(randomNonNegativeLong())),
                        new MaxPrimaryShardSizeCondition(ByteSizeValue.ofBytes(randomNonNegativeLong())),
                        new MaxPrimaryShardDocsCondition(randomNonNegativeLong()),
                        new OptimalShardCountCondition(3)
                    ),
                    randomNonNegativeLong()
                )
            )
            .mappingsUpdatedVersion(mappingsUpdatedVersion)
            .stats(indexStats)
            .indexWriteLoadForecast(indexWriteLoadForecast)
            .shardSizeInBytesForecast(shardSizeInBytesForecast)
            .putInferenceFields(inferenceFields)
            .eventIngestedRange(
                randomFrom(
                    IndexLongFieldRange.UNKNOWN,
                    IndexLongFieldRange.EMPTY,
                    IndexLongFieldRange.NO_SHARDS,
                    IndexLongFieldRange.NO_SHARDS.extendWithShardRange(0, 1, ShardLongFieldRange.of(5000000, 5500000))
                )
            )
            .reshardingMetadata(reshardingMetadata)
            .build();
    }

    private Settings.Builder randomSettings(int numberOfShards, int numberOfReplicas) {
        return indexSettings(numberOfShards, numberOfReplicas).put("index.version.created", 1)
            .putList("index.query.default_field", "title", "description", "tags");
    }

    private void removeEventIngestedField(Map<String, Object> indexMetadataMap) {
        // convert XContent to a map and remove the IndexMetadata.KEY_EVENT_INGESTED_RANGE entry
        // to simulate IndexMetadata from an older cluster version (before TransportVersions.EVENT_INGESTED_RANGE_IN_CLUSTER_STATE)
        // Map<String, Object> indexMetadataMap = XContentHelper.convertToMap(BytesReference.bytes(builder), true, XContentType.JSON).v2();

        @SuppressWarnings("unchecked")
        Map<String, Object> inner = (Map<String, Object>) indexMetadataMap.get("foo");
        assertTrue(inner.containsKey(IndexMetadata.KEY_EVENT_INGESTED_RANGE));
        inner.remove(IndexMetadata.KEY_EVENT_INGESTED_RANGE);
        // validate that the IndexMetadata.KEY_EVENT_INGESTED_RANGE has been removed before calling fromXContent
        assertFalse(inner.containsKey(IndexMetadata.KEY_EVENT_INGESTED_RANGE));
    }

    public static Map<String, InferenceFieldMetadata> randomInferenceFields() {
        Map<String, InferenceFieldMetadata> map = new HashMap<>();
        int numFields = randomIntBetween(0, 5);
        for (int i = 0; i < numFields; i++) {
            String field = randomAlphaOfLengthBetween(5, 10);
            map.put(field, randomInferenceFieldMetadata(field));
        }
        return map;
    }

    private static InferenceFieldMetadata randomInferenceFieldMetadata(String name) {
        return new InferenceFieldMetadata(
            name,
            randomIdentifier(),
            randomIdentifier(),
            randomSet(1, 5, ESTestCase::randomIdentifier).toArray(String[]::new),
            InferenceFieldMetadataTests.generateRandomChunkingSettings()
        );
    }

    private IndexMetadataStats randomIndexStats(int numberOfShards) {
        IndexWriteLoad.Builder indexWriteLoadBuilder = IndexWriteLoad.builder(numberOfShards);
        int numberOfPopulatedWriteLoads = randomIntBetween(0, numberOfShards);
        for (int i = 0; i < numberOfPopulatedWriteLoads; i++) {
            indexWriteLoadBuilder.withShardWriteLoad(
                i,
                randomDoubleBetween(0.0, 128.0, true),
                randomDoubleBetween(0.0, 128.0, true),
                randomDoubleBetween(0.0, 128.0, true),
                randomNonNegativeLong()
            );
        }
        return new IndexMetadataStats(indexWriteLoadBuilder.build(), randomLongBetween(100, 1024), randomIntBetween(1, 2));
    }

    private IndexReshardingMetadata randomIndexReshardingMetadata(int oldShards) {
        return IndexReshardingMetadata.newSplitByMultiple(oldShards, randomIntBetween(2, 5));
    }
}
