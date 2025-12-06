/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.xcontent.NamedXContentRegistry;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import static org.elasticsearch.cluster.metadata.Metadata.GLOBAL_STATE_FILE_PREFIX;

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
     * Creates empty cluster state file on disk, deleting global metadata and unreferencing all index metadata
     * (only used for dangling indices at that point).
     */
    public void unreferenceAll() throws IOException {
        Manifest.FORMAT.writeAndCleanup(Manifest.empty(), nodeEnv.nodeDataPaths()); // write empty file so that indices become unreferenced
        cleanUpGlobalStateFiles();
    }

    private void cleanUpGlobalStateFiles() throws IOException {
        for (Path location : nodeEnv.nodeDataPaths()) {
            logger.trace("cleanupOldFiles: cleaning up {} for global state files", location);
            final Path stateLocation = location.resolve(MetadataStateFormat.STATE_DIR_NAME);
            try (var paths = Files.list(stateLocation)) {
                paths.filter(file -> file.getFileName().toString().startsWith(GLOBAL_STATE_FILE_PREFIX)).forEach(file -> {
                    try {
                        Files.deleteIfExists(file);
                    } catch (IOException e) {
                        logger.trace("failed to delete global state file: {}", file);
                    }
                });
            }
        }
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
        Manifest.FORMAT.cleanupOldFiles(Long.MAX_VALUE, nodeEnv.nodeDataPaths()); // finally delete manifest
    }
}
