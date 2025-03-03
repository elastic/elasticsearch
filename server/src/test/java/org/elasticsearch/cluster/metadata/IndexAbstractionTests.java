/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class IndexAbstractionTests extends ESTestCase {

    public void testIndexAbstractionsDoNotSupportFailureIndices() {
        AliasMetadata aliasMetadata = AliasMetadata.builder("my-alias").build();
        IndexMetadata standaloneIndexMetadata = newIndexMetadata("my-index", aliasMetadata);
        IndexMetadata backingIndexMetadata = newIndexMetadata(".ds-my-ds", null);
        IndexMetadata failureIndexMetadata = newIndexMetadata(".fs-my-ds", null);
        DataStream dataStreamWithFs = newDataStreamInstance(
            List.of(backingIndexMetadata.getIndex()),
            List.of(failureIndexMetadata.getIndex())
        );
        var metadata = Metadata.builder()
            .put(standaloneIndexMetadata, false)
            .put(backingIndexMetadata, false)
            .put(failureIndexMetadata, false)
            .dataStreams(Map.of(dataStreamWithFs.getName(), dataStreamWithFs), Map.of())
            .build()
            .getProject();

        // Concrete indices do not support failure store

        IndexAbstraction standaloneIndex = new IndexAbstraction.ConcreteIndex(standaloneIndexMetadata);
        assertThat(standaloneIndex.getWriteIndex(), equalTo(standaloneIndexMetadata.getIndex()));
        assertThat(standaloneIndex.getWriteFailureIndex(metadata), nullValue());
        assertThat(standaloneIndex.getFailureIndices(metadata), empty());

        // Even if they belong to a data stream
        IndexAbstraction backingIndex = new IndexAbstraction.ConcreteIndex(backingIndexMetadata, dataStreamWithFs);
        assertThat(backingIndex.getWriteIndex(), equalTo(backingIndexMetadata.getIndex()));
        assertThat(backingIndex.getWriteFailureIndex(metadata), nullValue());
        assertThat(backingIndex.getFailureIndices(metadata), empty());

        IndexAbstraction failureIndex = new IndexAbstraction.ConcreteIndex(failureIndexMetadata, dataStreamWithFs);
        assertThat(failureIndex.getWriteIndex(), equalTo(failureIndexMetadata.getIndex()));
        assertThat(failureIndex.getWriteFailureIndex(metadata), nullValue());
        assertThat(failureIndex.getFailureIndices(metadata), empty());

        // Aliases of standalone indices also do not support the failure store
        List<IndexMetadata> referenceIndices = List.of(standaloneIndexMetadata);
        IndexAbstraction alias = new IndexAbstraction.Alias(aliasMetadata, referenceIndices);
        assertThat(alias.getIndices(), containsInAnyOrder(referenceIndices.stream().map(IndexMetadata::getIndex).toArray()));
        assertThat(alias.getWriteFailureIndex(metadata), nullValue());
        assertThat(alias.getFailureIndices(metadata), empty());
    }

    public void testIndexAbstractionsWithFailureIndices() {
        IndexMetadata backingIndexMetadata = newIndexMetadata(".ds-my-fs", null);
        IndexMetadata failureIndexMetadata = newIndexMetadata(".fs-my-fs", null);
        IndexMetadata otherBackingIndexMetadata = newIndexMetadata(".ds-my-ds", null);
        DataStream dsWithFailureStore = newDataStreamInstance(
            List.of(backingIndexMetadata.getIndex()),
            List.of(failureIndexMetadata.getIndex())
        );
        DataStream dsWithoutFailureStore = newDataStreamInstance(List.of(otherBackingIndexMetadata.getIndex()), List.of());
        DataStreamAlias aliasWithoutFailureStore = new DataStreamAlias(
            "no-fs-alias",
            List.of(dsWithoutFailureStore.getName()),
            dsWithoutFailureStore.getName(),
            Map.of()
        );
        DataStreamAlias aliasWithFailureStore = new DataStreamAlias(
            "with-fs-alias",
            List.of(dsWithoutFailureStore.getName(), dsWithFailureStore.getName()),
            dsWithFailureStore.getName(),
            Map.of()
        );
        DataStreamAlias aliasWithoutWriteDataStream = new DataStreamAlias(
            "no-write-alias",
            List.of(dsWithoutFailureStore.getName(), dsWithFailureStore.getName()),
            null,
            Map.of()
        );
        DataStreamAlias aliasWithoutWriteFailureStoreDataStream = new DataStreamAlias(
            "no-write-failure-stote-alias",
            List.of(dsWithoutFailureStore.getName(), dsWithFailureStore.getName()),
            dsWithoutFailureStore.getName(),
            Map.of()
        );
        var metadata = Metadata.builder()
            .put(otherBackingIndexMetadata, false)
            .put(backingIndexMetadata, false)
            .put(failureIndexMetadata, false)
            .dataStreams(
                Map.of(dsWithFailureStore.getName(), dsWithFailureStore, dsWithoutFailureStore.getName(), dsWithoutFailureStore),
                Map.of(
                    aliasWithoutFailureStore.getAlias(),
                    aliasWithoutFailureStore,
                    aliasWithFailureStore.getAlias(),
                    aliasWithFailureStore,
                    aliasWithoutWriteDataStream.getAlias(),
                    aliasWithoutWriteDataStream
                )
            )
            .build()
            .getProject();

        // Data stream with no failure store
        assertThat(dsWithoutFailureStore.getWriteIndex(), equalTo(otherBackingIndexMetadata.getIndex()));
        assertThat(dsWithoutFailureStore.getIndices(), contains(otherBackingIndexMetadata.getIndex()));
        assertThat(dsWithoutFailureStore.getWriteFailureIndex(), nullValue());
        assertThat(dsWithoutFailureStore.getFailureIndices(), empty());

        // Data stream with failure store
        assertThat(dsWithFailureStore.getWriteIndex(), equalTo(backingIndexMetadata.getIndex()));
        assertThat(dsWithFailureStore.getIndices(), contains(backingIndexMetadata.getIndex()));
        assertThat(dsWithFailureStore.getWriteFailureIndex(), equalTo(failureIndexMetadata.getIndex()));
        assertThat(dsWithFailureStore.getFailureIndices(), contains(failureIndexMetadata.getIndex()));

        // Alias with no write data stream
        List<Index> referenceIndices = Stream.concat(dsWithFailureStore.getIndices().stream(), dsWithoutFailureStore.getIndices().stream())
            .toList();
        IndexAbstraction aliasWithNoWriteDs = new IndexAbstraction.Alias(
            aliasWithoutWriteDataStream,
            referenceIndices,
            null,
            List.of(dsWithFailureStore.getName(), dsWithoutFailureStore.getName())
        );
        assertThat(aliasWithNoWriteDs.getWriteIndex(), nullValue());
        assertThat(aliasWithNoWriteDs.getIndices(), containsInAnyOrder(referenceIndices.toArray()));
        assertThat(aliasWithNoWriteDs.getWriteFailureIndex(metadata), nullValue());
        assertThat(aliasWithNoWriteDs.getFailureIndices(metadata), contains(failureIndexMetadata.getIndex()));

        // Alias with no failure store
        IndexAbstraction aliasWithNoFs = new IndexAbstraction.Alias(
            aliasWithoutFailureStore,
            dsWithoutFailureStore.getIndices(),
            dsWithoutFailureStore.getWriteIndex(),
            List.of(dsWithoutFailureStore.getName())
        );
        assertThat(aliasWithNoFs.getWriteIndex(), equalTo(dsWithoutFailureStore.getWriteIndex()));
        assertThat(aliasWithNoFs.getIndices(), contains(otherBackingIndexMetadata.getIndex()));
        assertThat(aliasWithNoFs.getWriteFailureIndex(metadata), nullValue());
        assertThat(aliasWithNoFs.getFailureIndices(metadata), empty());

        // Alias with failure store and write ds with failure store
        IndexAbstraction aliasWithWriteFs = new IndexAbstraction.Alias(
            aliasWithoutWriteDataStream,
            referenceIndices,
            dsWithFailureStore.getWriteIndex(),
            List.of(dsWithFailureStore.getName(), dsWithoutFailureStore.getName())
        );
        assertThat(aliasWithWriteFs.getWriteIndex(), equalTo(backingIndexMetadata.getIndex()));
        assertThat(aliasWithWriteFs.getIndices(), containsInAnyOrder(referenceIndices.toArray()));
        assertThat(aliasWithWriteFs.getWriteFailureIndex(metadata), equalTo(failureIndexMetadata.getIndex()));
        assertThat(aliasWithWriteFs.getFailureIndices(metadata), contains(failureIndexMetadata.getIndex()));

        // Alias with failure store and write ds without failure store
        IndexAbstraction aliasWithWithoutWriteFs = new IndexAbstraction.Alias(
            aliasWithoutWriteDataStream,
            referenceIndices,
            dsWithoutFailureStore.getWriteIndex(),
            List.of(dsWithFailureStore.getName(), dsWithoutFailureStore.getName())
        );
        assertThat(aliasWithWithoutWriteFs.getWriteIndex(), equalTo(otherBackingIndexMetadata.getIndex()));
        assertThat(aliasWithWithoutWriteFs.getIndices(), containsInAnyOrder(referenceIndices.toArray()));
        assertThat(aliasWithWithoutWriteFs.getWriteFailureIndex(metadata), nullValue());
        assertThat(aliasWithWithoutWriteFs.getFailureIndices(metadata), contains(failureIndexMetadata.getIndex()));
    }

    private IndexMetadata newIndexMetadata(String indexName, AliasMetadata aliasMetadata) {
        Settings dummyIndexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();

        IndexMetadata.Builder builder = new IndexMetadata.Builder(indexName).settings(dummyIndexSettings);
        if (aliasMetadata != null) {
            builder.putAlias(aliasMetadata);
        }
        return builder.build();
    }

    private static DataStream newDataStreamInstance(List<Index> backingIndices, List<Index> failureStoreIndices) {
        boolean isSystem = randomBoolean();
        return DataStream.builder(randomAlphaOfLength(50), backingIndices)
            .setFailureIndices(DataStream.DataStreamIndices.failureIndicesBuilder(failureStoreIndices).build())
            .setGeneration(randomLongBetween(1, 1000))
            .setMetadata(Map.of())
            .setSystem(isSystem)
            .setHidden(isSystem || randomBoolean())
            .setReplicated(randomBoolean())
            .build();
    }
}
