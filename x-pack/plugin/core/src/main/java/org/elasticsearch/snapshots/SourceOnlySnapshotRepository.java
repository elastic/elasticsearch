/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.snapshots;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.SimpleFSDirectory;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MappingMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.RepositoryMetaData;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardPath;
import org.elasticsearch.index.snapshots.IndexShardSnapshotStatus;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.repositories.FilterRepository;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * <p>
 * This is a filter snapshot repository that only snapshots the minimal required information
 * that is needed to recreate the index. In other words instead of snapshotting the entire shard
 * with all it's lucene indexed fields, doc values, points etc. it only snapshots the the stored
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
    public void initializeSnapshot(SnapshotId snapshotId, List<IndexId> indices, MetaData metaData) {
        // we process the index metadata at snapshot time. This means if somebody tries to restore
        // a _source only snapshot with a plain repository it will be just fine since we already set the
        // required engine, that the index is read-only and the mapping to a default mapping
        try {
            MetaData.Builder builder = MetaData.builder(metaData);
            for (IndexId indexId : indices) {
                IndexMetaData index = metaData.index(indexId.getName());
                IndexMetaData.Builder indexMetadataBuilder = IndexMetaData.builder(index);
                // for a minimal restore we basically disable indexing on all fields and only create an index
                // that is valid from an operational perspective. ie. it will have all metadata fields like version/
                // seqID etc. and an indexed ID field such that we can potentially perform updates on them or delete documents.
                ImmutableOpenMap<String, MappingMetaData> mappings = index.getMappings();
                Iterator<ObjectObjectCursor<String, MappingMetaData>> iterator = mappings.iterator();
                while (iterator.hasNext()) {
                    ObjectObjectCursor<String, MappingMetaData> next = iterator.next();
                    // we don't need to obey any routing here stuff is read-only anyway and get is disabled
                    final String mapping = "{ \"" + next.key + "\": { \"enabled\": false, \"_meta\": " + next.value.source().string()
                        + " } }";
                    indexMetadataBuilder.putMapping(next.key, mapping);
                }
                indexMetadataBuilder.settings(Settings.builder().put(index.getSettings())
                    .put(SOURCE_ONLY.getKey(), true)
                    .put("index.blocks.write", true)); // read-only!
                indexMetadataBuilder.settingsVersion(1 + indexMetadataBuilder.settingsVersion());
                builder.put(indexMetadataBuilder);
            }
            super.initializeSnapshot(snapshotId, indices, builder.build());
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public void snapshotShard(IndexShard shard, Store store, SnapshotId snapshotId, IndexId indexId, IndexCommit snapshotIndexCommit,
                              IndexShardSnapshotStatus snapshotStatus) {
        if (shard.mapperService().documentMapper() != null // if there is no mapping this is null
            && shard.mapperService().documentMapper().sourceMapper().isComplete() == false) {
            throw new IllegalStateException("Can't snapshot _source only on an index that has incomplete source ie. has _source disabled " +
                "or filters the source");
        }
        ShardPath shardPath = shard.shardPath();
        Path dataPath = shardPath.getDataPath();
        // TODO should we have a snapshot tmp directory per shard that is maintained by the system?
        Path snapPath = dataPath.resolve(SNAPSHOT_DIR_NAME);
        try (FSDirectory directory = new SimpleFSDirectory(snapPath)) {
            Store tempStore = new Store(store.shardId(), store.indexSettings(), directory, new ShardLock(store.shardId()) {
                @Override
                protected void closeInternal() {
                    // do nothing;
                }
            }, Store.OnClose.EMPTY);
            Supplier<Query> querySupplier = shard.mapperService().hasNested() ? Queries::newNestedFilter : null;
            // SourceOnlySnapshot will take care of soft- and hard-deletes no special casing needed here
            SourceOnlySnapshot snapshot = new SourceOnlySnapshot(tempStore.directory(), querySupplier);
            snapshot.syncSnapshot(snapshotIndexCommit);
            // we will use the lucene doc ID as the seq ID so we set the local checkpoint to maxDoc with a new index UUID
            SegmentInfos segmentInfos = tempStore.readLastCommittedSegmentsInfo();
            tempStore.bootstrapNewHistory(segmentInfos.totalMaxDoc());
            store.incRef();
            try (DirectoryReader reader = DirectoryReader.open(tempStore.directory())) {
                IndexCommit indexCommit = reader.getIndexCommit();
                super.snapshotShard(shard, tempStore, snapshotId, indexId, indexCommit, snapshotStatus);
            } finally {
                store.decRef();
            }
        } catch (IOException e) {
            // why on earth does this super method not declare IOException
            throw new UncheckedIOException(e);
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
