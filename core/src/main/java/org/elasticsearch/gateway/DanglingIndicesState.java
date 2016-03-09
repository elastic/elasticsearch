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

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.PersistedIndexMetaData;
import org.elasticsearch.cluster.service.InternalClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;

/**
 * The dangling indices state is responsible for finding new dangling indices (indices that have
 * their state written on disk, but don't exists in the metadata of the cluster), and importing
 * them into the cluster.
 */
public class DanglingIndicesState extends AbstractComponent {

    private final NodeEnvironment nodeEnv;
    private final MetaStateService metaStateService;
    private final LocalAllocateDangledIndices allocateDangledIndices;

    private final Map<Index, IndexMetaData> danglingIndices = new HashMap<>();
    private final Map<Index, IndexMetaData> deletedIndices = new HashMap<>();

    @Inject
    public DanglingIndicesState(Settings settings, NodeEnvironment nodeEnv, MetaStateService metaStateService,
                                LocalAllocateDangledIndices allocateDangledIndices) {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.metaStateService = metaStateService;
        this.allocateDangledIndices = allocateDangledIndices;
    }

    /**
     * Process dangling indices based on the provided meta data, handling cleanup, finding
     * new dangling indices, and allocating outstanding ones.
     */
    public void processDanglingIndices(MetaData metaData) {
        // we are assuming we are running this functionality in the single threaded environment of the cluster state update thread
        assert InternalClusterService.assertClusterStateThread();
        if (nodeEnv.hasNodeFile() == false) {
            return;
        }
        cleanupAllocatedDangledIndices(metaData);
        findNewAndAddDanglingIndices(metaData);
        cleanDeletedIndices();
        allocateDanglingIndices();
    }

    /**
     * The current set of dangling indices.
     */
    Map<Index, IndexMetaData> getDanglingIndices() {
        // This might be a good use case for CopyOnWriteHashMap
        return unmodifiableMap(new HashMap<>(danglingIndices));
    }

    /**
     * The current set of deleted indices that should be removed from the local file system instead of imported.
     */
    Map<Index, IndexMetaData> getDeletedIndices() {
        return unmodifiableMap(new HashMap<>(deletedIndices));
    }

    /**
     * Cleans dangling indices if they are already allocated on the provided meta data.
     */
    void cleanupAllocatedDangledIndices(MetaData metaData) {
        for (Iterator<Index> indices = danglingIndices.keySet().iterator(); indices.hasNext();) {
            final Index index = indices.next();
            final IndexMetaData indexMetaData = metaData.index(index);
            if (indexMetaData != null && indexMetaData.getIndex().getName().equals(index.getName())) {
                if (indexMetaData.getIndex().getUUID().equals(index.getUUID()) == false) {
                    logger.warn("[{}] can not be imported as a dangling index, as there is already another index " +
                        "with the same name but a different uuid. local index will be ignored (but not deleted)", index);
                } else {
                    logger.debug("[{}] no longer dangling (created), removing from dangling list", index);
                }
                danglingIndices.remove(index);
            }
        }
    }

    /**
     * Finds (@{link #findNewAndAddDanglingIndices}) and adds the new dangling indices
     * to the currently tracked dangling indices.
     */
    void findNewAndAddDanglingIndices(MetaData metaData) {
        Tuple<Map<Index, IndexMetaData>, Map<Index, IndexMetaData>> result = findNewDanglingIndices(metaData);
        danglingIndices.putAll(result.v1());
        deletedIndices.putAll(result.v2());
    }

    /**
     * Finds new dangling indices by iterating over the indices and trying to find indices
     * that have state on disk, but are not part of the provided meta data, or not detected
     * as dangled already.  The second tuple value contains dangling indices that should not
     * be imported but rather should be deleted and cleaned up.
     */
    Tuple<Map<Index, IndexMetaData>, Map<Index, IndexMetaData>> findNewDanglingIndices(MetaData metaData) {
        final Set<String> excludeIndexPathIds = new HashSet<>(metaData.indices().size() + danglingIndices.size());
        for (ObjectCursor<IndexMetaData> cursor : metaData.indices().values()) {
            excludeIndexPathIds.add(cursor.value.getIndex().getUUID());
        }
        excludeIndexPathIds.addAll(danglingIndices.keySet().stream().map(Index::getUUID).collect(Collectors.toList()));
        try {
            final List<PersistedIndexMetaData> indexMetaDataList = metaStateService.loadIndicesStates(excludeIndexPathIds::contains);
            Map<Index, IndexMetaData> newIndices = new HashMap<>(indexMetaDataList.size());
            Map<Index, IndexMetaData> delIndices = new HashMap<>();
            for (PersistedIndexMetaData persistedIndex : indexMetaDataList) {
                if (persistedIndex != null) {
                    final Index index = persistedIndex.getIndexMetaData().getIndex();
                    final String clusterUUID = metaData.clusterUUID();
                    if (persistedIndex.isSameClusterUUID(clusterUUID)) {
                        // the cluster metadata's cluster UUID is the same as the cluster UUID that created this index,
                        // so the index shouldn't be imported as dangling but rather deleted and cleaned up, because the
                        // dangling files are probably due to remnants from deleted indices (e.g. the node was offline while
                        // the index was deleted) or failed cleanup
                        logger.info("[{}] dangling index exists on the local file system, but it is deleted in the metadata of the " +
                                    "same cluster that created it, deleting the index on the local file system", index.getName());
                        delIndices.put(index, persistedIndex.getIndexMetaData());
                    } else {
                        if (metaData.hasIndex(index.getName())) {
                            logger.warn("[{}] can not be imported as a dangling index, as index with same name already exists " +
                                        "in cluster metadata", index.getName());
                        } else {
                            logger.info("[{}] dangling index, exists on local file system, but not in cluster metadata, " +
                                        "auto import to cluster state", index.getName());
                            newIndices.put(index, persistedIndex.getIndexMetaData());
                        }
                    }
                }
            }
            return Tuple.tuple(newIndices, delIndices);
        } catch (IOException e) {
            logger.warn("failed to list dangling indices", e);
            return Tuple.tuple(emptyMap(), emptyMap());
        }
    }

    /**
     * Cleans the indices that exist on the local file system but should be deleted according to the metadata state
     * and we have decided that the indices should not be imported as dangling.
     */
    void cleanDeletedIndices() {
        if (deletedIndices.isEmpty()) {
            return;
        }
        for (Iterator<Map.Entry<Index, IndexMetaData>> entries = deletedIndices.entrySet().iterator(); entries.hasNext();) {
            Map.Entry<Index, IndexMetaData> entry = entries.next();
            final String indexName = entry.getKey().getName();
            final IndexMetaData indexMetaData = entry.getValue();
            try {
                nodeEnv.deleteIndexDirectoryUnderLock(indexMetaData.getIndex(), new IndexSettings(indexMetaData, settings));
                entries.remove();
                logger.info("[{}] dangling index deleted from local file system", indexName);
            } catch (IOException e) {
                logger.warn("[{}] failed to delete dangling index from the local file system", e, indexName);
            }
        }
    }

    /**
     * Allocates the provided list of the dangled indices by sending them to the master node
     * for allocation.
     */
    private void allocateDanglingIndices() {
        if (danglingIndices.isEmpty() == true) {
            return;
        }
        try {
            allocateDangledIndices.allocateDangled(Collections.unmodifiableCollection(new ArrayList<>(danglingIndices.values())), new LocalAllocateDangledIndices.Listener() {
                @Override
                public void onResponse(LocalAllocateDangledIndices.AllocateDangledResponse response) {
                    logger.trace("allocated dangled");
                }

                @Override
                public void onFailure(Throwable e) {
                    logger.info("failed to send allocated dangled", e);
                }
            });
        } catch (Throwable e) {
            logger.warn("failed to send allocate dangled", e);
        }
    }

}
