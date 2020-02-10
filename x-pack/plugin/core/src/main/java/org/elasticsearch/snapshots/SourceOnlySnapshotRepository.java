/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.snapshots;

import org.apache.lucene.codecs.blocktree.BlockTreeTermsReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.repositories.FilterRepository;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.ShardGenerations;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * <p>
 * This is a filter snapshot repository that only snapshots the minimal required information
 * that is needed to recreate the index. In other words instead of snapshotting the entire shard
 * with all it's lucene indexed fields, doc values, points etc. it only snapshots the stored
 * fields including _source and _routing as well as the live docs in oder to distinguish between
 * live and deleted docs.
 * </p>
 * <p>
 * The repository can wrap any other repository delegating the source only snapshot to it to and read
 * from it. For instance a file repository of type <i>fs</i> by passing <i>settings.delegate_type=fs</i>
 * at repository creation time.
 * </p>
 * Snapshots restored from source only snapshots are minimal indices that are read-only and only allow
 * match_all scroll searches in order to reindex the data.
 */
public final class SourceOnlySnapshotRepository extends FilterRepository {
    private static final Setting<String> DELEGATE_TYPE = new Setting<>("delegate_type", "", Function.identity(), Setting.Property
        .NodeScope);
    public static final Setting<Boolean> SOURCE_ONLY = Setting.boolSetting("index.source_only", false, Setting
        .Property.IndexScope, Setting.Property.Final, Setting.Property.PrivateIndex);

    private static final String SNAPSHOT_DIR_NAME = "_snapshot";

    SourceOnlySnapshotRepository(Repository in) {
        super(in);
    }

    @Override
    public void finalizeSnapshot(SnapshotId snapshotId, ShardGenerations shardGenerations, long startTime, String failure,
                                 int totalShards, List<SnapshotShardFailure> shardFailures, long repositoryStateId,
                                 boolean includeGlobalState, MetaData metaData, Map<String, Object> userMetadata,
                                 boolean writeShardGens, ActionListener<SnapshotInfo> listener) {
        // we process the index metadata at snapshot time. This means if somebody tries to restore
        // a _source only snapshot with a plain repository it will be just fine since we already set the
        // required engine, that the index is read-only and the mapping to a default mapping
        try {
            super.finalizeSnapshot(snapshotId, shardGenerations, startTime, failure, totalShards, shardFailures, repositoryStateId,
                includeGlobalState, metadataToSnapshot(shardGenerations.indices(), metaData), userMetadata, writeShardGens, listener);
        } catch (IOException ex) {
            listener.onFailure(ex);
        }
    }

    private static MetaData metadataToSnapshot(Collection<IndexId> indices, MetaData metaData) throws IOException {
        MetaData.Builder builder = MetaData.builder(metaData);
        for (IndexId indexId : indices) {
            IndexMetaData index = metaData.index(indexId.getName());
            IndexMetaData.Builder indexMetadataBuilder = IndexMetaData.builder(index);
            // for a minimal restore we basically disable indexing on all fields and only create an index
            // that is valid from an operational perspective. ie. it will have all metadata fields like version/
            // seqID etc. and an indexed ID field such that we can potentially perform updates on them or delete documents.
            MappingMetaData mmd = index.mapping();
            if (mmd != null) {
                // we don't need to obey any routing here stuff is read-only anyway and get is disabled
                final String mapping = "{ \"_doc\" : { \"enabled\": false, \"_meta\": " + mmd.source().string() + " } }";
                indexMetadataBuilder.putMapping(mapping);
            }
            indexMetadataBuilder.settings(Settings.builder().put(index.getSettings())
                .put(SOURCE_ONLY.getKey(), true)
                .put("index.blocks.write", true)); // read-only!
            indexMetadataBuilder.settingsVersion(1 + indexMetadataBuilder.settingsVersion());
            builder.put(indexMetadataBuilder);
        }
        return builder.build();
    }


    @Override
    public void snapshotShard(Store store, MapperService mapperService, SnapshotId snapshotId, IndexId indexId,
                              IndexCommit snapshotIndexCommit, IndexShardSnapshotStatus snapshotStatus, boolean writeShardGens,
                              Map<String, Object> userMetadata, ActionListener<String> listener) {
        if (mapperService.documentMapper() != null // if there is no mapping this is null
            && mapperService.documentMapper().sourceMapper().isComplete() == false) {
            listener.onFailure(
                new IllegalStateException("Can't snapshot _source only on an index that has incomplete source ie. has _source disabled " +
                    "or filters the source"));
            return;
        }
        Directory unwrap = FilterDirectory.unwrap(store.directory());
        if (unwrap instanceof FSDirectory == false) {
            throw new AssertionError("expected FSDirectory but got " + unwrap.toString());
        }
        Path dataPath = ((FSDirectory) unwrap).getDirectory().getParent();
        // TODO should we have a snapshot tmp directory per shard that is maintained by the system?
        Path snapPath = dataPath.resolve(SNAPSHOT_DIR_NAME);
        final List<Closeable> toClose = new ArrayList<>(3);
        try {
            FSDirectory directory = new SimpleFSDirectory(snapPath);
            toClose.add(directory);
            Store tempStore = new Store(store.shardId(), store.indexSettings(), directory, new ShardLock(store.shardId()) {
                @Override
                protected void closeInternal() {
                    // do nothing;
                }
            }, Store.OnClose.EMPTY);
            Supplier<Query> querySupplier = mapperService.hasNested() ? Queries::newNestedFilter : null;
            // SourceOnlySnapshot will take care of soft- and hard-deletes no special casing needed here
            SourceOnlySnapshot snapshot = new SourceOnlySnapshot(tempStore.directory(), querySupplier);
            snapshot.syncSnapshot(snapshotIndexCommit);
            // we will use the lucene doc ID as the seq ID so we set the local checkpoint to maxDoc with a new index UUID
            SegmentInfos segmentInfos = tempStore.readLastCommittedSegmentsInfo();
            final long maxDoc = segmentInfos.totalMaxDoc();
            tempStore.bootstrapNewHistory(maxDoc, maxDoc);
            store.incRef();
            toClose.add(store::decRef);
            DirectoryReader reader = DirectoryReader.open(tempStore.directory(),
                Collections.singletonMap(BlockTreeTermsReader.FST_MODE_KEY, BlockTreeTermsReader.FSTLoadMode.OFF_HEAP.name()));
            toClose.add(reader);
            IndexCommit indexCommit = reader.getIndexCommit();
            super.snapshotShard(tempStore, mapperService, snapshotId, indexId, indexCommit, snapshotStatus, writeShardGens,
                userMetadata, ActionListener.runBefore(listener, () -> IOUtils.close(toClose)));
        } catch (IOException e) {
            try {
                IOUtils.close(toClose);
            } catch (IOException ex) {
                e.addSuppressed(ex);
            }
            listener.onFailure(e);
        }
    }

    /**
     * Returns an {@link EngineFactory} for the source only snapshots.
     */
    public static EngineFactory getEngineFactory() {
        return config -> new ReadOnlyEngine(config, null, new TranslogStats(0, 0, 0, 0, 0), true,
            reader -> {
                try {
                    return SeqIdGeneratingFilterReader.wrap(reader, config.getPrimaryTermSupplier().getAsLong());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
    }

    /**
     * Returns a new source only repository factory
     */
    public static Repository.Factory newRepositoryFactory() {
        return new Repository.Factory() {

            @Override
            public Repository create(RepositoryMetaData metadata) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Repository create(RepositoryMetaData metaData, Function<String, Repository.Factory> typeLookup) throws Exception {
                String delegateType = DELEGATE_TYPE.get(metaData.settings());
                if (Strings.hasLength(delegateType) == false) {
                    throw new IllegalArgumentException(DELEGATE_TYPE.getKey() + " must be set");
                }
                Repository.Factory factory = typeLookup.apply(delegateType);
                return new SourceOnlySnapshotRepository(factory.create(new RepositoryMetaData(metaData.name(),
                    delegateType, metaData.settings()), typeLookup));
            }
        };
    }
}
