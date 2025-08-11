/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.snapshots.sourceonly;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.Query;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RepositoryMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.env.ShardLock;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.ReadOnlyEngine;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.index.translog.TranslogStats;
import org.elasticsearch.repositories.FilterRepository;
import org.elasticsearch.repositories.FinalizeSnapshotContext;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.repositories.Repository;
import org.elasticsearch.repositories.SnapshotShardContext;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
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
    private static final Setting<String> DELEGATE_TYPE = new Setting<>(
        "delegate_type",
        "",
        Function.identity(),
        Setting.Property.NodeScope
    );
    public static final Setting<Boolean> SOURCE_ONLY = Setting.boolSetting(
        "index.source_only",
        false,
        Setting.Property.IndexScope,
        Setting.Property.Final,
        Setting.Property.PrivateIndex
    );

    private static final Logger logger = LogManager.getLogger(SourceOnlySnapshotRepository.class);

    private static final String SNAPSHOT_DIR_NAME = "_snapshot";

    SourceOnlySnapshotRepository(Repository in) {
        super(in);
    }

    @Override
    public void initializeSnapshot(SnapshotId snapshotId, List<IndexId> indices, Metadata metadata) {
        // we process the index metadata at snapshot time. This means if somebody tries to restore
        // a _source only snapshot with a plain repository it will be just fine since we already set the
        // required engine, that the index is read-only and the mapping to a default mapping
        try {
            super.initializeSnapshot(snapshotId, indices, metadataToSnapshot(indices, metadata));
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    @Override
    public void finalizeSnapshot(FinalizeSnapshotContext finalizeSnapshotContext) {
        // we process the index metadata at snapshot time. This means if somebody tries to restore
        // a _source only snapshot with a plain repository it will be just fine since we already set the
        // required engine, that the index is read-only and the mapping to a default mapping
        try {
            super.finalizeSnapshot(
                new FinalizeSnapshotContext(
                    finalizeSnapshotContext.updatedShardGenerations(),
                    finalizeSnapshotContext.repositoryStateId(),
                    metadataToSnapshot(
                        finalizeSnapshotContext.updatedShardGenerations().indices(),
                        finalizeSnapshotContext.clusterMetadata()
                    ),
                    finalizeSnapshotContext.snapshotInfo(),
                    finalizeSnapshotContext.repositoryMetaVersion(),
                    finalizeSnapshotContext
                )
            );
        } catch (IOException e) {
            finalizeSnapshotContext.onFailure(e);
        }
    }

    private static Metadata metadataToSnapshot(Collection<IndexId> indices, Metadata metadata) throws IOException {
        Metadata.Builder builder = Metadata.builder(metadata);
        for (IndexId indexId : indices) {
            IndexMetadata index = metadata.index(indexId.getName());
            IndexMetadata.Builder indexMetadataBuilder = IndexMetadata.builder(index);
            // for a minimal restore we basically disable indexing on all fields and only create an index
            // that is valid from an operational perspective. ie. it will have all metadata fields like version/
            // seqID etc. and an indexed ID field such that we can potentially perform updates on them or delete documents.
            ImmutableOpenMap<String, MappingMetadata> mappings = index.getMappings();
            Iterator<ObjectObjectCursor<String, MappingMetadata>> iterator = mappings.iterator();
            while (iterator.hasNext()) {
                ObjectObjectCursor<String, MappingMetadata> next = iterator.next();
                // we don't need to obey any routing here stuff is read-only anyway and get is disabled
                final String mapping = "{ \"" + next.key + "\": { \"enabled\": false, \"_meta\": " + next.value.source().string() + " } }";
                indexMetadataBuilder.putMapping(next.key, mapping);
            }
            indexMetadataBuilder.settings(
                Settings.builder().put(index.getSettings()).put(SOURCE_ONLY.getKey(), true).put("index.blocks.write", true)
            ); // read-only!
            indexMetadataBuilder.settingsVersion(1 + indexMetadataBuilder.settingsVersion());
            builder.put(indexMetadataBuilder);
        }
        return builder.build();
    }

    @Override
    public void snapshotShard(SnapshotShardContext context) {
        final MapperService mapperService = context.mapperService();
        if (mapperService.documentMapper() != null // if there is no mapping this is null
            && mapperService.documentMapper().sourceMapper().isComplete() == false) {
            context.onFailure(
                new IllegalStateException(
                    "Can't snapshot _source only on an index that has incomplete source ie. has _source disabled " + "or filters the source"
                )
            );
            return;
        }
        final Store store = context.store();
        Directory unwrap = FilterDirectory.unwrap(store.directory());
        if (unwrap instanceof FSDirectory == false) {
            context.onFailure(
                new IllegalStateException(
                    context.indexCommit()
                        + " is not a regular index, but ["
                        + unwrap.toString()
                        + "]  and cannot be snapshotted into a source-only repository"
                )
            );
            return;
        }
        Path dataPath = ((FSDirectory) unwrap).getDirectory().getParent();
        // TODO should we have a snapshot tmp directory per shard that is maintained by the system?
        Path snapPath = dataPath.resolve(SNAPSHOT_DIR_NAME);
        final List<Closeable> toClose = new ArrayList<>(3);
        try {
            SourceOnlySnapshot.LinkedFilesDirectory overlayDir = new SourceOnlySnapshot.LinkedFilesDirectory(new NIOFSDirectory(snapPath));
            toClose.add(overlayDir);
            Store tempStore = new Store(store.shardId(), store.indexSettings(), overlayDir, new ShardLock(store.shardId()) {
                @Override
                protected void closeInternal() {
                    // do nothing;
                }
            }, Store.OnClose.EMPTY);
            Supplier<Query> querySupplier = mapperService.hasNested() ? Queries::newNestedFilter : null;
            // SourceOnlySnapshot will take care of soft- and hard-deletes no special casing needed here
            SourceOnlySnapshot snapshot;
            snapshot = new SourceOnlySnapshot(overlayDir, querySupplier);
            final IndexCommit snapshotIndexCommit = context.indexCommit();
            try {
                snapshot.syncSnapshot(snapshotIndexCommit);
            } catch (NoSuchFileException | CorruptIndexException | FileAlreadyExistsException e) {
                logger.warn(
                    () -> new ParameterizedMessage(
                        "Existing staging directory [{}] appears corrupted and will be pruned and recreated.",
                        snapPath
                    ),
                    e
                );
                Lucene.cleanLuceneIndex(overlayDir);
                snapshot.syncSnapshot(snapshotIndexCommit);
            }
            // we will use the lucene doc ID as the seq ID so we set the local checkpoint to maxDoc with a new index UUID
            SegmentInfos segmentInfos = tempStore.readLastCommittedSegmentsInfo();
            final long maxDoc = segmentInfos.totalMaxDoc();
            tempStore.bootstrapNewHistory(maxDoc, maxDoc);
            store.incRef();
            toClose.add(store::decRef);
            DirectoryReader reader = DirectoryReader.open(tempStore.directory());
            toClose.add(reader);
            IndexCommit indexCommit = reader.getIndexCommit();
            super.snapshotShard(
                new SnapshotShardContext(
                    tempStore,
                    mapperService,
                    context.snapshotId(),
                    context.indexId(),
                    new Engine.IndexCommitRef(indexCommit, () -> IOUtils.close(toClose)),
                    context.stateIdentifier(),
                    context.status(),
                    context.getRepositoryMetaVersion(),
                    context.userMetadata(),
                    context
                )
            );
        } catch (IOException e) {
            try {
                IOUtils.close(toClose);
            } catch (IOException ex) {
                e.addSuppressed(ex);
            }
            context.onFailure(e);
        }
    }

    /**
     * Returns an {@link EngineFactory} for the source only snapshots.
     */
    public static EngineFactory getEngineFactory() {
        return config -> new ReadOnlyEngine(config, null, new TranslogStats(0, 0, 0, 0, 0), true, readerWrapper(config), true, false);
    }

    public static Function<DirectoryReader, DirectoryReader> readerWrapper(EngineConfig engineConfig) {
        return reader -> {
            try {
                return SeqIdGeneratingFilterReader.wrap(reader, engineConfig.getPrimaryTermSupplier().getAsLong());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    /**
     * Returns a new source only repository factory
     */
    public static Repository.Factory newRepositoryFactory() {
        return new Repository.Factory() {

            @Override
            public Repository create(RepositoryMetadata metadata) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Repository create(RepositoryMetadata metadata, Function<String, Repository.Factory> typeLookup) throws Exception {
                String delegateType = DELEGATE_TYPE.get(metadata.settings());
                if (Strings.hasLength(delegateType) == false) {
                    throw new IllegalArgumentException(DELEGATE_TYPE.getKey() + " must be set");
                }
                Repository.Factory factory = typeLookup.apply(delegateType);
                return new SourceOnlySnapshotRepository(
                    factory.create(new RepositoryMetadata(metadata.name(), delegateType, metadata.settings()), typeLookup)
                );
            }
        };
    }
}
