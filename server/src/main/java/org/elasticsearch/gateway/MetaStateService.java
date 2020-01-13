/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gateway;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.Manifest;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Handles writing and loading {@link Manifest}, {@link MetaData} and {@link IndexMetaData}
 */
public class MetaStateService {
    private static final Logger logger = LogManager.getLogger(MetaStateService.class);

    private final NodeEnvironment nodeEnv;
    private final NamedXContentRegistry namedXContentRegistry;

    // we allow subclasses in tests to redefine formats, e.g. to inject failures
    protected MetaDataStateFormat<MetaData> META_DATA_FORMAT = MetaData.FORMAT;
    protected MetaDataStateFormat<IndexMetaData> INDEX_META_DATA_FORMAT = IndexMetaData.FORMAT;
    protected MetaDataStateFormat<Manifest> MANIFEST_FORMAT = Manifest.FORMAT;

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
     * @return tuple of {@link Manifest} and {@link MetaData} with global metadata and indices metadata. If there is no state on disk,
     * meta state with globalGeneration -1 and empty meta data is returned.
     * @throws IOException if some IOException when loading files occurs or there is no metadata referenced by manifest file.
     */
    public Tuple<Manifest, MetaData> loadFullState() throws IOException {
        final Manifest manifest = MANIFEST_FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.nodeDataPaths());
        if (manifest == null) {
            return loadFullStateBWC();
        }

        final MetaData.Builder metaDataBuilder;
        if (manifest.isGlobalGenerationMissing()) {
            metaDataBuilder = MetaData.builder();
        } else {
            final MetaData globalMetaData = META_DATA_FORMAT.loadGeneration(logger, namedXContentRegistry, manifest.getGlobalGeneration(),
                    nodeEnv.nodeDataPaths());
            if (globalMetaData != null) {
                metaDataBuilder = MetaData.builder(globalMetaData);
            } else {
                throw new IOException("failed to find global metadata [generation: " + manifest.getGlobalGeneration() + "]");
            }
        }

        for (Map.Entry<Index, Long> entry : manifest.getIndexGenerations().entrySet()) {
            final Index index = entry.getKey();
            final long generation = entry.getValue();
            final String indexFolderName = index.getUUID();
            final IndexMetaData indexMetaData = INDEX_META_DATA_FORMAT.loadGeneration(logger, namedXContentRegistry, generation,
                    nodeEnv.resolveIndexFolder(indexFolderName));
            if (indexMetaData != null) {
                metaDataBuilder.put(indexMetaData, false);
            } else {
                throw new IOException("failed to find metadata for existing index " + index.getName() + " [location: " + indexFolderName +
                        ", generation: " + generation + "]");
            }
        }

        return new Tuple<>(manifest, metaDataBuilder.build());
    }

    /**
     * "Manifest-less" BWC version of loading metadata from disk. See also {@link #loadFullState()}
     */
    private Tuple<Manifest, MetaData> loadFullStateBWC() throws IOException {
        Map<Index, Long> indices = new HashMap<>();
        MetaData.Builder metaDataBuilder;

        Tuple<MetaData, Long> metaDataAndGeneration =
                META_DATA_FORMAT.loadLatestStateWithGeneration(logger, namedXContentRegistry, nodeEnv.nodeDataPaths());
        MetaData globalMetaData = metaDataAndGeneration.v1();
        long globalStateGeneration = metaDataAndGeneration.v2();

        final IndexGraveyard indexGraveyard;
        if (globalMetaData != null) {
            metaDataBuilder = MetaData.builder(globalMetaData);
            indexGraveyard = globalMetaData.custom(IndexGraveyard.TYPE);
            // TODO https://github.com/elastic/elasticsearch/issues/38556
            // assert Version.CURRENT.major < 8 : "failed to find manifest file, which is mandatory staring with Elasticsearch version 8.0";
        } else {
            metaDataBuilder = MetaData.builder();
            indexGraveyard = IndexGraveyard.builder().build();
        }

        for (String indexFolderName : nodeEnv.availableIndexFolders()) {
            Tuple<IndexMetaData, Long> indexMetaDataAndGeneration =
                    INDEX_META_DATA_FORMAT.loadLatestStateWithGeneration(logger, namedXContentRegistry,
                            nodeEnv.resolveIndexFolder(indexFolderName));
            // TODO https://github.com/elastic/elasticsearch/issues/38556
            // assert Version.CURRENT.major < 8 : "failed to find manifest file, which is mandatory staring with Elasticsearch version 8.0";
            IndexMetaData indexMetaData = indexMetaDataAndGeneration.v1();
            long generation = indexMetaDataAndGeneration.v2();
            if (indexMetaData != null) {
                if (indexGraveyard.containsIndex(indexMetaData.getIndex())) {
                    logger.debug("[{}] found metadata for deleted index [{}]", indexFolderName, indexMetaData.getIndex());
                    // this index folder is cleared up when state is recovered
                } else {
                    indices.put(indexMetaData.getIndex(), generation);
                    metaDataBuilder.put(indexMetaData, false);
                }
            } else {
                logger.debug("[{}] failed to find metadata for existing index location", indexFolderName);
            }
        }

        Manifest manifest = Manifest.unknownCurrentTermAndVersion(globalStateGeneration, indices);
        return new Tuple<>(manifest, metaDataBuilder.build());
    }

    /**
     * Loads the index state for the provided index name, returning null if doesn't exists.
     */
    @Nullable
    public IndexMetaData loadIndexState(Index index) throws IOException {
        return INDEX_META_DATA_FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.indexPaths(index));
    }

    /**
     * Loads all indices states available on disk
     */
    List<IndexMetaData> loadIndicesStates(Predicate<String> excludeIndexPathIdsPredicate) throws IOException {
        List<IndexMetaData> indexMetaDataList = new ArrayList<>();
        for (String indexFolderName : nodeEnv.availableIndexFolders(excludeIndexPathIdsPredicate)) {
            assert excludeIndexPathIdsPredicate.test(indexFolderName) == false :
                    "unexpected folder " + indexFolderName + " which should have been excluded";
            IndexMetaData indexMetaData = INDEX_META_DATA_FORMAT.loadLatestState(logger, namedXContentRegistry,
                    nodeEnv.resolveIndexFolder(indexFolderName));
            if (indexMetaData != null) {
                final String indexPathId = indexMetaData.getIndex().getUUID();
                if (indexFolderName.equals(indexPathId)) {
                    indexMetaDataList.add(indexMetaData);
                } else {
                    throw new IllegalStateException("[" + indexFolderName + "] invalid index folder name, rename to [" + indexPathId + "]");
                }
            } else {
                logger.debug("[{}] failed to find metadata for existing index location", indexFolderName);
            }
        }
        return indexMetaDataList;
    }

    /**
     * Loads the global state, *without* index state, see {@link #loadFullState()} for that.
     */
    MetaData loadGlobalState() throws IOException {
        return META_DATA_FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.nodeDataPaths());
    }

    /**
     * Writes manifest file (represented by {@link Manifest}) to disk and performs cleanup of old manifest state file if
     * the write succeeds or newly created manifest state if the write fails.
     *
     * @throws WriteStateException if exception when writing state occurs. See also {@link WriteStateException#isDirty()}
     */
    public void writeManifestAndCleanup(String reason, Manifest manifest) throws WriteStateException {
        logger.trace("[_meta] writing state, reason [{}]", reason);
        try {
            long generation = MANIFEST_FORMAT.writeAndCleanup(manifest, nodeEnv.nodeDataPaths());
            logger.trace("[_meta] state written (generation: {})", generation);
        } catch (WriteStateException ex) {
            throw new WriteStateException(ex.isDirty(), "[_meta]: failed to write meta state", ex);
        }
    }

    /**
     * Writes the index state.
     * <p>
     * This method is public for testing purposes.
     *
     * @throws WriteStateException if exception when writing state occurs. {@link WriteStateException#isDirty()} will always return
     *                             false, because new index state file is not yet referenced by manifest file.
     */
    public long writeIndex(String reason, IndexMetaData indexMetaData) throws WriteStateException {
        final Index index = indexMetaData.getIndex();
        logger.trace("[{}] writing state, reason [{}]", index, reason);
        try {
            long generation = INDEX_META_DATA_FORMAT.write(indexMetaData,
                    nodeEnv.indexPaths(indexMetaData.getIndex()));
            logger.trace("[{}] state written", index);
            return generation;
        } catch (WriteStateException ex) {
            throw new WriteStateException(false, "[" + index + "]: failed to write index state", ex);
        }
    }

    /**
     * Writes the global state, *without* the indices states.
     *
     * @throws WriteStateException if exception when writing state occurs. {@link WriteStateException#isDirty()} will always return
     *                             false, because new global state file is not yet referenced by manifest file.
     */
    long writeGlobalState(String reason, MetaData metaData) throws WriteStateException {
        logger.trace("[_global] writing state, reason [{}]", reason);
        try {
            long generation = META_DATA_FORMAT.write(metaData, nodeEnv.nodeDataPaths());
            logger.trace("[_global] state written");
            return generation;
        } catch (WriteStateException ex) {
            throw new WriteStateException(false, "[_global]: failed to write global state", ex);
        }
    }

    /**
     * Removes old state files in global state directory.
     *
     * @param currentGeneration current state generation to keep in the directory.
     */
    void cleanupGlobalState(long currentGeneration) {
        META_DATA_FORMAT.cleanupOldFiles(currentGeneration, nodeEnv.nodeDataPaths());
    }

    /**
     * Removes old state files in index directory.
     *
     * @param index             index to perform clean up on.
     * @param currentGeneration current state generation to keep in the index directory.
     */
    public void cleanupIndex(Index index, long currentGeneration) {
        INDEX_META_DATA_FORMAT.cleanupOldFiles(currentGeneration, nodeEnv.indexPaths(index));
    }

    /**
     * Creates empty cluster state file on disk, deleting global metadata and unreferencing all index metadata
     * (only used for dangling indices at that point).
     */
    public void unreferenceAll() throws IOException {
        MANIFEST_FORMAT.writeAndCleanup(Manifest.empty(), nodeEnv.nodeDataPaths()); // write empty file so that indices become unreferenced
        META_DATA_FORMAT.cleanupOldFiles(Long.MAX_VALUE, nodeEnv.nodeDataPaths());
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
            MetaDataStateFormat.deleteMetaState(nodeEnv.resolveIndexFolder(indexFolderName));
        }
        MANIFEST_FORMAT.cleanupOldFiles(Long.MAX_VALUE, nodeEnv.nodeDataPaths()); // finally delete manifest
    }
}
