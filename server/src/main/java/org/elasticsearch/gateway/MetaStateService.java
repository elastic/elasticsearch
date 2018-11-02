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

import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.MetaState;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Handles writing and loading both {@link MetaData} and {@link IndexMetaData}.
 * When new {@link MetaData} or {@link IndexMetaData} is written this class changes internal state, to keep track of new
 * <code>globalGeneration</code> and generation for each index. This state is used by {@link #writeMetaState(String)} method.
 * Each time some write method throws and exception or {@link #writeMetaState(String)} finishes either successfully or unsuccessfully
 * internal state is reset.
 * This class is not thread-safe.
 */
public class MetaStateService extends AbstractComponent {

    private final NodeEnvironment nodeEnv;
    private final NamedXContentRegistry namedXContentRegistry;

    private MetaState currentMetaState;
    private long globalGeneration;
    private List<Runnable> cleanupActions;
    private Map<Index, Long> newIndices;

    public MetaStateService(Settings settings, NodeEnvironment nodeEnv, NamedXContentRegistry namedXContentRegistry) throws IOException {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.namedXContentRegistry = namedXContentRegistry;
        this.currentMetaState = loadFullState().v1();
        resetState();
    }

    /**
     * Loads the full state, which includes both the global state and all the indices
     * meta data.
     */
    MetaData loadMetaData() throws IOException {
        Tuple<MetaState, MetaData> stateAndData = loadFullState();
        this.currentMetaState = stateAndData.v1();
        resetState();
        return stateAndData.v2();
    }

    Set<Index> getPreviouslyWrittenIndices() {
        return Collections.unmodifiableSet(currentMetaState.getIndices().keySet());
    }

    /**
     * This method should be called if index metadata has not changed and index is not removed.
     */
    void keepIndex(Index index) {
        logger.trace("[{}] keep index", index);
        newIndices.put(index, currentMetaState.getIndices().get(index));
    }

    /**
     * Writes the index state.
     *
     * This method is public for testing purposes.
     */
    public void writeIndex(String reason, IndexMetaData indexMetaData) throws WriteStateException {
        final Index index = indexMetaData.getIndex();
        logger.trace("[{}] writing state, reason [{}]", index, reason);
        try {
            long generation = IndexMetaData.FORMAT.write(indexMetaData, false, nodeEnv.indexPaths(indexMetaData.getIndex()));
            logger.trace("[{}] state written", index);
            newIndices.put(indexMetaData.getIndex(), generation);
            cleanupActions.add(() -> IndexMetaData.FORMAT.cleanupOldFiles(generation, nodeEnv.indexPaths(index)));
        } catch (WriteStateException ex) {
            resetState();
            throw new WriteStateException(false, "[" + index + "]: failed to write index state", ex);
        }
    }

    /**
     * Writes the global state, *without* the indices states.
     */
    void writeGlobalState(String reason, MetaData metaData) throws WriteStateException {
        logger.trace("[_global] writing state, reason [{}]", reason);
        try {
            globalGeneration = MetaData.FORMAT.write(metaData, false, nodeEnv.nodeDataPaths());
            logger.trace("[_global] state written (generation: {})", globalGeneration);
            cleanupActions.add(() -> MetaData.FORMAT.cleanupOldFiles(globalGeneration, nodeEnv.nodeDataPaths()));
        } catch (WriteStateException ex) {
            resetState();
            throw new WriteStateException(false, "[_global]: failed to write global state", ex);
        }
    }

    public void writeMetaState(String reason) throws WriteStateException {
        assert globalGeneration != -1;
        MetaState metaState = new MetaState(globalGeneration, newIndices);
        logger.trace("[_meta] writing state, reason [{}], globalGen [{}]", reason, globalGeneration);
        try {
            long generation = MetaState.FORMAT.write(metaState, false, nodeEnv.nodeDataPaths());
            logger.trace("[_meta] state written (generation: {})", generation);
            cleanupActions.add(() -> MetaState.FORMAT.cleanupOldFiles(generation, nodeEnv.nodeDataPaths()));
            cleanupActions.forEach(Runnable::run);
            this.currentMetaState = metaState;
        } catch (WriteStateException ex) {
            throw new WriteStateException(ex.isDirty(), "[_meta]: failed to write meta state", ex);
        } finally {
            resetState();
        }
    }

    private void resetState() {
        this.globalGeneration = currentMetaState.getGlobalStateGeneration();
        this.cleanupActions = new ArrayList<>();
        this.newIndices = new HashMap<>();
    }

    private Tuple<MetaState, MetaData> loadFullState() throws IOException {
        final MetaState metaState = MetaState.FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.nodeDataPaths());
        if (metaState == null) {
            return loadFullStateBWC();
        }

        final MetaData.Builder metaDataBuilder;
        final MetaData globalMetaData = MetaData.FORMAT.loadGeneration(logger, namedXContentRegistry, metaState.getGlobalStateGeneration(),
                nodeEnv.nodeDataPaths());
        if (globalMetaData != null) {
            metaDataBuilder = MetaData.builder(globalMetaData);
        } else {
            throw new IOException("failed to find global metadata [generation: " + metaState.getGlobalStateGeneration() + "]");
        }
        for (Map.Entry<Index, Long> entry : metaState.getIndices().entrySet()) {
            Index index = entry.getKey();
            long generation = entry.getValue();
            final String indexFolderName = index.getUUID();
            final IndexMetaData indexMetaData = IndexMetaData.FORMAT.loadGeneration(logger, namedXContentRegistry, generation,
                    nodeEnv.resolveIndexFolder(indexFolderName));
            if (indexMetaData != null) {
                metaDataBuilder.put(indexMetaData, false);
            } else {
                throw new IOException("failed to find metadata for existing index [location: " + indexFolderName + ", generation: " +
                        generation + "]");
            }
        }
        return new Tuple<>(metaState, metaDataBuilder.build());
    }

    /**
     * Zen 1 BWC version of loading metadata from disk. See also {@link #loadFullState()}
     */
    private Tuple<MetaState, MetaData> loadFullStateBWC() throws IOException {
        Map<Index, Long> indices = new HashMap<>();
        Tuple<MetaData, Long> metaDataAndGeneration =
                MetaData.FORMAT.loadLatestStateWithGeneration(logger, namedXContentRegistry, nodeEnv.nodeDataPaths());
        MetaData globalMetaData = metaDataAndGeneration.v1();
        long globalStateGeneration = metaDataAndGeneration.v2();
        boolean isFreshStartup = globalMetaData == null;

        if (isFreshStartup) {
            assert Version.CURRENT.major < 8 : "failed to find manifest file, which is mandatory staring with Elasticsearch version 8.0";
            return Tuple.tuple(MetaState.empty(), null);
        }

        MetaData.Builder metaDataBuilder = MetaData.builder(globalMetaData);
        for (String indexFolderName : nodeEnv.availableIndexFolders()) {
            Tuple<IndexMetaData, Long> indexMetaDataAndGeneration =
                    IndexMetaData.FORMAT.loadLatestStateWithGeneration(logger, namedXContentRegistry,
                            nodeEnv.resolveIndexFolder(indexFolderName));
            IndexMetaData indexMetaData = indexMetaDataAndGeneration.v1();
            long generation = indexMetaDataAndGeneration.v2();
            if (indexMetaData != null) {
                indices.put(indexMetaData.getIndex(), generation);
                metaDataBuilder.put(indexMetaData, false);
            } else {
                logger.debug("[{}] failed to find metadata for existing index location", indexFolderName);
            }
        }
        MetaState metaState = new MetaState(globalStateGeneration, indices);
        return new Tuple<>(metaState, metaDataBuilder.build());
    }
}
