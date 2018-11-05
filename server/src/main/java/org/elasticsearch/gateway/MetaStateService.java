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
 * Handles writing and loading {@link MetaData}, {@link IndexMetaData} and {@link MetaState}.<br>
 * The instance of this class loads manifest file and corresponding metadata into memory on node startup.<br>
 * It's possible to change metadata stored on disk by calling {@link #writeIndex(String, IndexMetaData)} and
 * {@link #writeGlobalState(String, MetaData)} methods. If index metadata or global metadata has not been changed
 * use {@link #keepIndex(Index)} and {@link #keepGlobalState()} methods. Once all necessary changes are done
 * use {@link #writeMetaState(String)} to make these changes visible, thanks to manifest file.<br>
 * When {@link #writeMetaState(String)} finishes successfully, this class updates internally held metaData correspondingly.
 * Use {@link #getMetaData()} to get it.<br>
 * A note on exceptions. Write methods of this class throw {@link WriteStateException}. You can expect {@link WriteStateException#isDirty()}
 * to return <code>false</code> for {@link #writeGlobalState(String, MetaData)} and {@link #writeIndex(String, IndexMetaData)}.
 * {@link #writeMetaState(String)}, on the other hand, may throw dirty exceptions.
 * If one of the write methods throw an exception, internal structures of this class are reset to previously steady state.<br>
 * A note on thread-safety. This class assumes single metadata mutator. However, it's safe to call {@link #getMetaData()} from any
 * thread, any time.<br>
 * A note on backward compatibility. This implementation is backward compatible with "manifest-less" implementation. If there is
 * no manifest file, metadata is loaded from latest state files. However, the first call to {@link #writeMetaState(String)} will
 * create manifest file. One can check if the instance of this class is started in BWC mode by calling {@link #isBwcMode()}.
 */
public class MetaStateService extends AbstractComponent {

    private final NodeEnvironment nodeEnv;
    private final NamedXContentRegistry namedXContentRegistry;

    private MetaState currentMetaState;
    private Long globalGeneration;
    private Map<Index, Long> newIndices;
    private List<Runnable> cleanupActions;

    private volatile MetaData currentMetaData;
    private MetaData.Builder metaDataBuilder;
    private boolean bwcMode;

    public MetaStateService(Settings settings, NodeEnvironment nodeEnv, NamedXContentRegistry namedXContentRegistry) throws IOException {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.namedXContentRegistry = namedXContentRegistry;
        loadFullState();
        resetState();
    }

    /**
     * Returns <code>true</code> if there was metadata, but no manifest file during node startup.
     */
    public boolean isBwcMode() {
        return bwcMode;
    }

    /**
     * Returns current metadata (global metadata and indices metadata).
     */
    MetaData getMetaData() {
        return currentMetaData;
    }

    /**
     * Returns set of indices, which metadata is stored on this node.
     */
    Set<Index> getPreviouslyWrittenIndices() {
        return currentMetaState == null ? Collections.emptySet() : Collections.unmodifiableSet(currentMetaState.getIndices().keySet());
    }

    /**
     * This method should be called if index metadata has not changed and index is not removed.
     */
    void keepIndex(Index index) {
        IndexMetaData metaData = currentMetaData.index(index);
        assert metaData != null;
        Long generation = currentMetaState.getIndices().get(index);
        assert generation != null;
        logger.trace("[{}] keep index", index);
        newIndices.put(index, generation);
        metaDataBuilder.put(metaData, false);
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
            newIndices.put(index, generation);
            metaDataBuilder.put(indexMetaData, false);
            cleanupActions.add(() -> IndexMetaData.FORMAT.cleanupOldFiles(generation, nodeEnv.indexPaths(index)));
        } catch (WriteStateException ex) {
            resetState();
            throw new WriteStateException(false, "[" + index + "]: failed to write index state", ex);
        }
    }

    private void copyGlobalMetaDataToBuilder(MetaData metaData) {
        metaDataBuilder.
                version(metaData.version()).
                clusterUUID(metaData.clusterUUID()).
                persistentSettings(metaData.persistentSettings()).
                templates(metaData.templates()).
                customs(metaData.customs());
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
            copyGlobalMetaDataToBuilder(metaData);
        } catch (WriteStateException ex) {
            resetState();
            throw new WriteStateException(false, "[_global]: failed to write global state", ex);
        }
    }

    public void keepGlobalState() {
        assert currentMetaData != null;
        globalGeneration = currentMetaState.getGlobalStateGeneration();
        copyGlobalMetaDataToBuilder(currentMetaData);
    }

    public void writeMetaState(String reason) throws WriteStateException {
        assert globalGeneration != null;
        MetaState metaState = new MetaState(globalGeneration, newIndices);
        logger.trace("[_meta] writing state, reason [{}], globalGen [{}]", reason, globalGeneration);
        try {
            long generation = MetaState.FORMAT.write(metaState, false, nodeEnv.nodeDataPaths());
            logger.trace("[_meta] state written (generation: {})", generation);
            cleanupActions.add(() -> MetaState.FORMAT.cleanupOldFiles(generation, nodeEnv.nodeDataPaths()));
            cleanupActions.forEach(Runnable::run);

            this.currentMetaData = metaDataBuilder.build();
            this.currentMetaState = metaState;
        } catch (WriteStateException ex) {
            throw new WriteStateException(ex.isDirty(), "[_meta]: failed to write meta state", ex);
        } finally {
            resetState();
        }
    }

    private void resetState() {
        this.globalGeneration = null;
        this.cleanupActions = new ArrayList<>();
        this.newIndices = new HashMap<>();
        this.metaDataBuilder = MetaData.builder();
    }

    private void loadFullState() throws IOException {
        MetaState metaState = MetaState.FORMAT.loadLatestState(logger, namedXContentRegistry, nodeEnv.nodeDataPaths());
        if (metaState == null) {
            loadFullStateBWC();
        } else {
            final MetaData.Builder metaDataBuilder;
            final MetaData globalMetaData = MetaData.FORMAT.loadGeneration(logger, namedXContentRegistry,
                    metaState.getGlobalStateGeneration(), nodeEnv.nodeDataPaths());
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
            currentMetaState = metaState;
            currentMetaData = metaDataBuilder.build();
        }
    }

    /**
     * Zen 1 BWC version of loading metadata from disk. See also {@link #loadFullState()}
     */
    private void loadFullStateBWC() throws IOException {
        Map<Index, Long> indices = new HashMap<>();
        Tuple<MetaData, Long> metaDataAndGeneration =
                MetaData.FORMAT.loadLatestStateWithGeneration(logger, namedXContentRegistry, nodeEnv.nodeDataPaths());
        MetaData globalMetaData = metaDataAndGeneration.v1();
        long globalStateGeneration = metaDataAndGeneration.v2();
        boolean isFreshStartup = globalMetaData == null;

        if (isFreshStartup) {
            assert Version.CURRENT.major < 8 : "failed to find manifest file, which is mandatory staring with Elasticsearch version 8.0";
        } else {
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
            currentMetaState = new MetaState(globalStateGeneration, indices);
            currentMetaData = metaDataBuilder.build();
            bwcMode = true;
        }
    }
}
