/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.watcher.watch;

import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamAlias;
import org.elasticsearch.cluster.metadata.DataStreamMetadata;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class WatchStoreUtilsTests extends ESTestCase {

    public void testGetConcreteIndexForDataStream() {
        String dataStreamName = randomAlphaOfLength(20);
        Metadata.Builder metadataBuilder = Metadata.builder();
        Map<String, Metadata.ProjectCustom> customsBuilder = new HashMap<>();
        Map<String, DataStream> dataStreams = new HashMap<>();
        Map<String, IndexMetadata> indexMetadataMapBuilder = new HashMap<>();
        List<String> indexNames = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(2, 10); i++) {
            String indexName = dataStreamName + "_" + i;
            indexNames.add(indexName);
            indexMetadataMapBuilder.put(indexName, createIndexMetaData(indexName, null));
        }
        metadataBuilder.indices(indexMetadataMapBuilder);
        dataStreams.put(
            dataStreamName,
            DataStreamTestHelper.newInstance(
                dataStreamName,
                indexNames.stream().map(indexName -> new Index(indexName, IndexMetadata.INDEX_UUID_NA_VALUE)).collect(Collectors.toList())
            )
        );
        ImmutableOpenMap<String, DataStreamAlias> dataStreamAliases = ImmutableOpenMap.of();
        DataStreamMetadata dataStreamMetadata = new DataStreamMetadata(
            ImmutableOpenMap.<String, DataStream>builder().putAllFromMap(dataStreams).build(),
            dataStreamAliases
        );
        customsBuilder.put(DataStreamMetadata.TYPE, dataStreamMetadata);
        metadataBuilder.projectCustoms(customsBuilder);
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
        Map<String, IndexMetadata> indexMetadataMapBuilder = new HashMap<>();
        for (int i = 0; i < randomIntBetween(2, 10); i++) {
            String indexName = aliasName + "_" + i;
            indexMetadataMapBuilder.put(indexName, createIndexMetaData(indexName, aliasMetadata));
        }
        metadataBuilder.indices(indexMetadataMapBuilder);
        expectThrows(IllegalStateException.class, () -> WatchStoreUtils.getConcreteIndex(aliasName, metadataBuilder.build()));
    }

    public void testGetConcreteIndexForAliasWithMultipleIndicesWithWritable() {
        String aliasName = randomAlphaOfLength(20);
        Metadata.Builder metadataBuilder = Metadata.builder();
        AliasMetadata.Builder aliasMetadataBuilder = new AliasMetadata.Builder(aliasName);
        aliasMetadataBuilder.writeIndex(false);
        AliasMetadata nonWritableAliasMetadata = aliasMetadataBuilder.build();
        AliasMetadata.Builder writableAliasMetadataBuilder = new AliasMetadata.Builder(aliasName);
        writableAliasMetadataBuilder.writeIndex(true);
        AliasMetadata writableAliasMetadata = writableAliasMetadataBuilder.build();
        Map<String, IndexMetadata> indexMetadataMapBuilder = new HashMap<>();
        List<String> indexNames = new ArrayList<>();
        int indexCount = randomIntBetween(2, 10);
        int writableIndexIndex = randomIntBetween(0, indexCount - 1);
        for (int i = 0; i < indexCount; i++) {
            String indexName = aliasName + "_" + i;
            indexNames.add(indexName);
            final AliasMetadata aliasMetadata;
            if (i == writableIndexIndex) {
                aliasMetadata = writableAliasMetadata;
            } else {
                aliasMetadata = nonWritableAliasMetadata;
            }
            indexMetadataMapBuilder.put(indexName, createIndexMetaData(indexName, aliasMetadata));
        }
        metadataBuilder.indices(indexMetadataMapBuilder);
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
        Map<String, IndexMetadata> indexMetadataMapBuilder = new HashMap<>();
        String indexName = aliasName + "_" + 0;
        indexMetadataMapBuilder.put(indexName, createIndexMetaData(indexName, aliasMetadata));
        metadataBuilder.indices(indexMetadataMapBuilder);
        IndexMetadata concreteIndex = WatchStoreUtils.getConcreteIndex(aliasName, metadataBuilder.build());
        assertNotNull(concreteIndex);
        assertEquals(indexName, concreteIndex.getIndex().getName());
    }

    public void testGetConcreteIndexForConcreteIndex() {
        String indexName = randomAlphaOfLength(20);
        Metadata.Builder metadataBuilder = Metadata.builder();
        Map<String, IndexMetadata> indexMetadataMapBuilder = new HashMap<>();
        indexMetadataMapBuilder.put(indexName, createIndexMetaData(indexName, null));
        metadataBuilder.indices(indexMetadataMapBuilder);
        IndexMetadata concreteIndex = WatchStoreUtils.getConcreteIndex(indexName, metadataBuilder.build());
        assertNotNull(concreteIndex);
        assertEquals(indexName, concreteIndex.getIndex().getName());
    }

    private IndexMetadata createIndexMetaData(String indexName, AliasMetadata aliasMetadata) {
        IndexMetadata.Builder indexMetadataBuilder = new IndexMetadata.Builder(indexName);
        Settings settings = indexSettings(1, 1).put(IndexMetadata.SETTING_PRIORITY, 5)
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), IndexVersion.current())
            .build();
        indexMetadataBuilder.settings(settings);
        if (aliasMetadata != null) {
            indexMetadataBuilder.putAlias(aliasMetadata);
        }
        return indexMetadataBuilder.build();
    }
}
