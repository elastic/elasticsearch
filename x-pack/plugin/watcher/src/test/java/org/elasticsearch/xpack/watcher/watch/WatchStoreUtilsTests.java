/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.watch;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.DataStreamMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WatchStoreUtilsTests extends ESTestCase {

    public void testGetConcreteIndexForDataStream() {
        String dataStreamName = randomAlphaOfLength(20);
        Metadata.Builder metadataBuilder = Metadata.builder();
        ImmutableOpenMap.Builder<String, Metadata.Custom> customsBuilder = ImmutableOpenMap.builder();
        Map<String, DataStream> dataStreams = new HashMap<>();
        ImmutableOpenMap.Builder<String, IndexMetadata> indexMetadataMapBuilder = ImmutableOpenMap.builder();
        List<String> indexNames = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(2, 10); i++) {
            String indexName = dataStreamName + "_" + i;
            indexNames.add(indexName);
            IndexMetadata.Builder indexMetadataBuilder = new IndexMetadata.Builder(indexName);
            Settings settings = Settings.builder()
                .put(IndexMetadata.SETTING_PRIORITY, 5)
                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
                .build();
            indexMetadataBuilder.settings(settings);
            indexMetadataMapBuilder.put(indexName, indexMetadataBuilder.build());
        }
        metadataBuilder.indices(indexMetadataMapBuilder.build());
        dataStreams.put(
            dataStreamName,
            new DataStream(
                dataStreamName,
                indexNames.stream().map(indexName -> new Index(indexName, IndexMetadata.INDEX_UUID_NA_VALUE)).collect(Collectors.toList()),
                randomInt(),
                Collections.emptyMap(),
                true,
                randomBoolean(),
                true,
                randomBoolean(),
                IndexMode.TIME_SERIES
            )
        );
        Map<String, DataStreamAlias> dataStreamAliases = Collections.emptyMap();
        DataStreamMetadata dataStreamMetadata = new DataStreamMetadata(dataStreams, dataStreamAliases);
        customsBuilder.put(DataStreamMetadata.TYPE, dataStreamMetadata);
        metadataBuilder.customs(customsBuilder.build());
        IndexMetadata concreteIndex = WatchStoreUtils.getConcreteIndex(dataStreamName, metadataBuilder.build());
        assertNotNull(concreteIndex);
        assertEquals(indexNames.get(indexNames.size() - 1), concreteIndex.getIndex().getName());
    }

    public void testGetConcreteIndexForAliasWithMultipleNonWritableIndices() {
        String aliasName = randomAlphaOfLength(20);
        Metadata.Builder metadataBuilder = Metadata.builder();
        AliasMetadata.Builder aliasMetadataBuilder = new AliasMetadata.Builder(aliasName);
        aliasMetadataBuilder.writeIndex(false);
        AliasMetadata aliasMetadata = aliasMetadataBuilder.build();
        ImmutableOpenMap.Builder<String, IndexMetadata> indexMetadataMapBuilder = ImmutableOpenMap.builder();
        for (int i = 0; i < randomIntBetween(2, 10); i++) {
            String indexName = aliasName + "_" + i;
            IndexMetadata.Builder indexMetadataBuilder = new IndexMetadata.Builder(indexName);
            Settings settings = Settings.builder()
                .put(IndexMetadata.SETTING_PRIORITY, 5)
                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
                .build();
            indexMetadataBuilder.settings(settings);
            indexMetadataBuilder.putAlias(aliasMetadata);
            indexMetadataMapBuilder.put(indexName, indexMetadataBuilder.build());
        }
        metadataBuilder.indices(indexMetadataMapBuilder.build());
        expectThrows(IllegalStateException.class, () -> WatchStoreUtils.getConcreteIndex(aliasName, metadataBuilder.build()));
    }

    public void testGetConcreteIndexForAliasWithMultipleIndicesWithWritable() {
        String aliasName = randomAlphaOfLength(20);
        Metadata.Builder metadataBuilder = Metadata.builder();
        AliasMetadata.Builder aliasMetadataBuilder = new AliasMetadata.Builder(aliasName);
        aliasMetadataBuilder.writeIndex(false);
        AliasMetadata aliasMetadata = aliasMetadataBuilder.build();
        AliasMetadata.Builder writableAliasMetadataBuilder = new AliasMetadata.Builder(aliasName);
        writableAliasMetadataBuilder.writeIndex(true);
        AliasMetadata writableAliasMetadata = writableAliasMetadataBuilder.build();
        ImmutableOpenMap.Builder<String, IndexMetadata> indexMetadataMapBuilder = ImmutableOpenMap.builder();
        List<String> indexNames = new ArrayList<>();
        int indexCount = randomIntBetween(2, 10);
        int writableIndexIndex = randomIntBetween(0, indexCount - 1);
        for (int i = 0; i < indexCount; i++) {
            String indexName = aliasName + "_" + i;
            indexNames.add(indexName);
            IndexMetadata.Builder indexMetadataBuilder = new IndexMetadata.Builder(indexName);
            Settings settings = Settings.builder()
                .put(IndexMetadata.SETTING_PRIORITY, 5)
                .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
                .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
                .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
                .build();
            indexMetadataBuilder.settings(settings);
            if (i == writableIndexIndex) {
                indexMetadataBuilder.putAlias(writableAliasMetadata);
            } else {
                indexMetadataBuilder.putAlias(aliasMetadata);
            }
            indexMetadataMapBuilder.put(indexName, indexMetadataBuilder.build());
        }
        metadataBuilder.indices(indexMetadataMapBuilder.build());
        IndexMetadata concreteIndex = WatchStoreUtils.getConcreteIndex(aliasName, metadataBuilder.build());
        assertNotNull(concreteIndex);
        assertEquals(indexNames.get(writableIndexIndex), concreteIndex.getIndex().getName());
    }

    public void testGetConcreteIndexForAliasWithOneNonWritableIndex() {
        String aliasName = randomAlphaOfLength(20);
        Metadata.Builder metadataBuilder = Metadata.builder();
        AliasMetadata.Builder aliasMetadataBuilder = new AliasMetadata.Builder(aliasName);
        aliasMetadataBuilder.writeIndex(false);
        AliasMetadata aliasMetadata = aliasMetadataBuilder.build();
        ImmutableOpenMap.Builder<String, IndexMetadata> indexMetadataMapBuilder = ImmutableOpenMap.builder();
        String indexName = aliasName + "_" + 0;
        IndexMetadata.Builder indexMetadataBuilder = new IndexMetadata.Builder(indexName);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_PRIORITY, 5)
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            .build();
        indexMetadataBuilder.settings(settings);
        indexMetadataBuilder.putAlias(aliasMetadata);
        indexMetadataMapBuilder.put(indexName, indexMetadataBuilder.build());
        metadataBuilder.indices(indexMetadataMapBuilder.build());
        IndexMetadata concreteIndex = WatchStoreUtils.getConcreteIndex(aliasName, metadataBuilder.build());
        assertNotNull(concreteIndex);
        assertEquals(indexName, concreteIndex.getIndex().getName());
    }

    public void testGetConcreteIndexForConcreteIndex() {
        String indexName = randomAlphaOfLength(20);
        Metadata.Builder metadataBuilder = Metadata.builder();
        ImmutableOpenMap.Builder<String, IndexMetadata> indexMetadataMapBuilder = ImmutableOpenMap.builder();
        IndexMetadata.Builder indexMetadataBuilder = new IndexMetadata.Builder(indexName);
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_PRIORITY, 5)
            .put(IndexMetadata.INDEX_NUMBER_OF_SHARDS_SETTING.getKey(), 1)
            .put(IndexMetadata.INDEX_NUMBER_OF_REPLICAS_SETTING.getKey(), 1)
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            .build();
        indexMetadataBuilder.settings(settings);
        indexMetadataMapBuilder.put(indexName, indexMetadataBuilder.build());
        metadataBuilder.indices(indexMetadataMapBuilder.build());
        IndexMetadata concreteIndex = WatchStoreUtils.getConcreteIndex(indexName, metadataBuilder.build());
        assertNotNull(concreteIndex);
        assertEquals(indexName, concreteIndex.getIndex().getName());
    }
}
