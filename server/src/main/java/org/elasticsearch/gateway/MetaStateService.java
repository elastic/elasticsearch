/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Handles writing and loading {@link Manifest}, {@link Metadata} and {@link IndexMetadata} as used for cluster state persistence in
 * versions prior to {@link Version#V_7_6_0}, used to read this older format during an upgrade from these versions.
 */
public class MetaStateService {
    private static final Logger logger = LogManager.getLogger(MetaStateService.class);

    public final NodeEnvironment nodeEnv;
    public final NamedXContentRegistry namedXContentRegistry;

    public MetaStateService(NodeEnvironment nodeEnv, NamedXContentRegistry namedXContentRegistry) {
        this.nodeEnv = nodeEnv;
        this.namedXContentRegistry = namedXContentRegistry;
    }

    /**
     * Loads the full state, which includes both the global state and all the indices meta data. <br>
     * When loading, manifest file is consulted (represented by {@link Manifest} class), to load proper generations. <br>
     * If there is no manifest file on disk, this method fallbacks to BWC mode, where latest generation of global and indices
     * metadata is loaded. Please note that currently there is no way to distinguish between manifest file being removed and manifest
     * file was not yet created. It means that this method always fallbacks to BWC mode, if there is no manifest file.
     *
     * @return tuple of {@link Manifest} and {@link Metadata} with global metadata and indices metadata. If there is no state on disk,
     * meta state with globalGeneration -1 and empty meta data is returned.
     * @throws IOException if some IOException when loading files occurs or there is no metadata referenced by manifest file.
     */
    public Tuple<Manifest, Metadata> loadFullState() throws IOException {
        final Manifest manifest = Manifest.FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.nodeDataPaths());
        if (manifest == null) {
            return loadFullStateBWC();
        }

        final Metadata.Builder metadataBuilder;
        if (manifest.isGlobalGenerationMissing()) {
            metadataBuilder = Metadata.builder();
        } else {
            final Metadata globalMetadata = Metadata.FORMAT.loadGeneration(
                logger,
                namedXContentRegistry,
                manifest.globalGeneration(),
                nodeEnv.nodeDataPaths()
            );
            if (globalMetadata != null) {
                metadataBuilder = Metadata.builder(globalMetadata);
            } else {
                throw new IOException("failed to find global metadata [generation: " + manifest.globalGeneration() + "]");
            }
        }

        for (Map.Entry<Index, Long> entry : manifest.indexGenerations().entrySet()) {
            final Index index = entry.getKey();
            final long generation = entry.getValue();
            final String indexFolderName = index.getUUID();
            final IndexMetadata indexMetadata = IndexMetadata.FORMAT.loadGeneration(
                logger,
                namedXContentRegistry,
                generation,
                nodeEnv.resolveIndexFolder(indexFolderName)
            );
            if (indexMetadata != null) {
                metadataBuilder.put(indexMetadata, false);
            } else {
                throw new IOException(
                    "failed to find metadata for existing index "
                        + index.getName()
                        + " [location: "
                        + indexFolderName
                        + ", generation: "
                        + generation
                        + "]"
                );
            }
        }

        return new Tuple<>(manifest, metadataBuilder.build());
    }

    /**
     * "Manifest-less" BWC version of loading metadata from disk. See also {@link #loadFullState()}
     */
    private Tuple<Manifest, Metadata> loadFullStateBWC() throws IOException {
        Map<Index, Long> indices = new HashMap<>();
        Metadata.Builder metadataBuilder;

        Tuple<Metadata, Long> metadataAndGeneration = Metadata.FORMAT.loadLatestStateWithGeneration(
            logger,
            namedXContentRegistry,
            nodeEnv.nodeDataPaths()
        );
        Metadata globalMetadata = metadataAndGeneration.v1();
        long globalStateGeneration = metadataAndGeneration.v2();

        final IndexGraveyard indexGraveyard;
        if (globalMetadata != null) {
            metadataBuilder = Metadata.builder(globalMetadata);
            indexGraveyard = globalMetadata.custom(IndexGraveyard.TYPE);
        } else {
            metadataBuilder = Metadata.builder();
            indexGraveyard = IndexGraveyard.builder().build();
        }

        for (String indexFolderName : nodeEnv.availableIndexFolders()) {
            Tuple<IndexMetadata, Long> indexMetadataAndGeneration = IndexMetadata.FORMAT.loadLatestStateWithGeneration(
                logger,
                namedXContentRegistry,
                nodeEnv.resolveIndexFolder(indexFolderName)
            );
            IndexMetadata indexMetadata = indexMetadataAndGeneration.v1();
            long generation = indexMetadataAndGeneration.v2();
            if (indexMetadata != null) {
                if (indexGraveyard.containsIndex(indexMetadata.getIndex())) {
                    logger.debug("[{}] found metadata for deleted index [{}]", indexFolderName, indexMetadata.getIndex());
                    // this index folder is cleared up when state is recovered
                } else {
                    indices.put(indexMetadata.getIndex(), generation);
                    metadataBuilder.put(indexMetadata, false);
                }
            } else {
                logger.debug("[{}] failed to find metadata for existing index location", indexFolderName);
            }
        }

        Manifest manifest = Manifest.unknownCurrentTermAndVersion(globalStateGeneration, indices);
        return new Tuple<>(manifest, metadataBuilder.build());
    }

    /**
     * Loads the index state for the provided index name, returning null if doesn't exists.
     */
    @Nullable
    public IndexMetadata loadIndexState(Index index) throws IOException {
        return IndexMetadata.FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.indexPaths(index));
    }

    /**
     * Loads all indices states available on disk
     */
    List<IndexMetadata> loadIndicesStates(Predicate<String> excludeIndexPathIdsPredicate) throws IOException {
        List<IndexMetadata> indexMetadataList = new ArrayList<>();
        for (String indexFolderName : nodeEnv.availableIndexFolders(excludeIndexPathIdsPredicate)) {
            assert excludeIndexPathIdsPredicate.test(indexFolderName) == false
                : "unexpected folder " + indexFolderName + " which should have been excluded";
            IndexMetadata indexMetadata = IndexMetadata.FORMAT.loadLatestState(
                logger,
                namedXContentRegistry,
                nodeEnv.resolveIndexFolder(indexFolderName)
            );
            if (indexMetadata != null) {
                final String indexPathId = indexMetadata.getIndex().getUUID();
                if (indexFolderName.equals(indexPathId)) {
                    indexMetadataList.add(indexMetadata);
                } else {
                    throw new IllegalStateException("[" + indexFolderName + "] invalid index folder name, rename to [" + indexPathId + "]");
                }
            } else {
                logger.debug("[{}] failed to find metadata for existing index location", indexFolderName);
            }
        }
        return indexMetadataList;
    }

    /**
     * Loads the global state, *without* index state, see {@link #loadFullState()} for that.
     */
    Metadata loadGlobalState() throws IOException {
        return Metadata.FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.nodeDataPaths());
    }

    /**
     * Creates empty cluster state file on disk, deleting global metadata and unreferencing all index metadata
     * (only used for dangling indices at that point).
     */
    public void unreferenceAll() throws IOException {
        boolean useFsync = IndexModule.NODE_STORE_USE_FSYNC.get(nodeEnv.settings());
        // write empty file so that indices become unreferenced
        Manifest.FORMAT.writeAndCleanup(Manifest.empty(), useFsync, nodeEnv.nodeDataPaths());
        Metadata.FORMAT.cleanupOldFiles(Long.MAX_VALUE, useFsync, nodeEnv.nodeDataPaths());
    }

    /**
     * Removes manifest file, global metadata and all index metadata
     */
    public void deleteAll() throws IOException {
        // To ensure that the metadata is never reimported by loadFullStateBWC in case where the deletions here fail mid-way through,
        // we first write an empty manifest file so that the indices become unreferenced, then clean up the indices, and only then delete
        // the manifest file.
        unreferenceAll();
        for (String indexFolderName : nodeEnv.availableIndexFolders()) {
            // delete meta state directories of indices
            MetadataStateFormat.deleteMetaState(nodeEnv.resolveIndexFolder(indexFolderName));
        }
        // finally delete manifest
        Manifest.FORMAT.cleanupOldFiles(Long.MAX_VALUE, IndexModule.NODE_STORE_USE_FSYNC.get(nodeEnv.settings()), nodeEnv.nodeDataPaths());
    }
}
