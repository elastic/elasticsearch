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

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.IndexStateMetaData;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

/**
 * Handles writing and loading both {@link MetaData} and {@link IndexMetaData}
 */
public class MetaStateService extends AbstractComponent {

    private final NodeEnvironment nodeEnv;

    @Inject
    public MetaStateService(Settings settings, NodeEnvironment nodeEnv) {
        super(settings);
        this.nodeEnv = nodeEnv;
    }

    /**
     * Loads the full state, which includes both the global state and all the indices
     * meta state.
     */
    MetaData loadFullState() throws Exception {
        MetaData globalMetaData = loadGlobalState();
        MetaData.Builder metaDataBuilder;
        if (globalMetaData != null) {
            metaDataBuilder = MetaData.builder(globalMetaData);
        } else {
            metaDataBuilder = MetaData.builder();
        }
        for (String indexFolderName : nodeEnv.availableIndexFolders()) {
            IndexStateMetaData persistedIndex =
                IndexStateMetaData.FORMAT.loadLatestState(logger, nodeEnv.resolveIndexFolder(indexFolderName));
            if (persistedIndex != null) {
                metaDataBuilder.put(persistedIndex.getIndexMetaData(), false);
            } else {
                logger.debug("[{}] failed to find metadata for existing index location", indexFolderName);
            }
        }
        return metaDataBuilder.build();
    }

    /**
     * Loads the index state for the provided index name, returning null if doesn't exists.
     */
    @Nullable
    IndexStateMetaData loadIndexState(Index index) throws IOException {
        return IndexStateMetaData.FORMAT.loadLatestState(logger, nodeEnv.indexPaths(index));
    }

    /**
     * Loads all indices states available on disk, without checking if the index folder name is correct.
     * This should only be used if loading indices states on initialization prior to the index folder name upgrade
     * taking place through {@link org.elasticsearch.common.util.IndexFolderUpgrader#upgradeIndicesIfNeeded(Settings, NodeEnvironment)}.
     */
    List<IndexStateMetaData> loadIndicesStatesNoFolderNameValidation() throws IOException {
        List<IndexStateMetaData> indexMetaDataList = new ArrayList<>();
        for (String indexFolderName : nodeEnv.availableIndexFolders()) {
            IndexStateMetaData persistedIndex =
                IndexStateMetaData.FORMAT.loadLatestState(logger, nodeEnv.resolveIndexFolder(indexFolderName));
            if (persistedIndex != null) {
                indexMetaDataList.add(persistedIndex);
            } else {
                logger.debug("[{}] failed to find metadata for existing index location", indexFolderName);
            }
        }
        return indexMetaDataList;
    }

    /**
     * Loads all indices states available on disk
     */
    List<IndexStateMetaData> loadIndicesStates(Predicate<String> excludeIndexPathIdsPredicate) throws IOException {
        List<IndexStateMetaData> indexMetaDataList = new ArrayList<>();
        for (String indexFolderName : nodeEnv.availableIndexFolders()) {
            if (excludeIndexPathIdsPredicate.test(indexFolderName)) {
                continue;
            }
            IndexStateMetaData persistedIndex =
                IndexStateMetaData.FORMAT.loadLatestState(logger, nodeEnv.resolveIndexFolder(indexFolderName));
            if (persistedIndex != null) {
                final String indexPathId = persistedIndex.getIndexMetaData().getIndex().getUUID();
                if (indexFolderName.equals(indexPathId)) {
                    indexMetaDataList.add(persistedIndex);
                } else {
                    throw new IllegalStateException("[" + indexFolderName+ "] invalid index folder name, rename to [" + indexPathId + "]");
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
        MetaData globalState = MetaData.FORMAT.loadLatestState(logger, nodeEnv.nodeDataPaths());
        // ES 2.0 now requires units for all time and byte-sized settings, so we add the default unit if it's missing
        // TODO: can we somehow only do this for pre-2.0 cluster state?
        if (globalState != null) {
            return MetaData.addDefaultUnitsIfNeeded(logger, globalState);
        } else {
            return null;
        }
    }

    /**
     * Writes the index state.
     */
    void writeIndex(String reason, IndexStateMetaData persistedIndex) throws IOException {
        writeIndex(reason, persistedIndex, nodeEnv.indexPaths(persistedIndex.getIndexMetaData().getIndex()));
    }

    /**
     * Writes the index state in <code>locations</code>, use {@link #writeGlobalState(String, MetaData)}
     * to write index state in index paths
     */
    void writeIndex(String reason, IndexStateMetaData persistedIndex, Path[] locations) throws IOException {
        final IndexMetaData indexMetaData = persistedIndex.getIndexMetaData();
        final Index index = indexMetaData.getIndex();
        logger.trace("[{}] writing state, reason [{}]", index, reason);
        try {
            IndexStateMetaData.FORMAT.write(persistedIndex, indexMetaData.getVersion(), locations);
        } catch (Throwable ex) {
            logger.warn("[{}]: failed to write index state", ex, index);
            throw new IOException("failed to write state for [" + index + "]", ex);
        }
    }

    /**
     * Writes the global state, *without* the indices states.
     */
    void writeGlobalState(String reason, MetaData metaData) throws Exception {
        logger.trace("[_global] writing state, reason [{}]",  reason);
        try {
            MetaData.FORMAT.write(metaData, metaData.version(), nodeEnv.nodeDataPaths());
        } catch (Throwable ex) {
            logger.warn("[_global]: failed to write global state", ex);
            throw new IOException("failed to write global state", ex);
        }
    }

}
