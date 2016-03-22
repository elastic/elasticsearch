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
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.metadata.IndexStateMetaData;
import org.elasticsearch.cluster.service.InternalClusterService;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.indices.IndicesService;

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
    private final IndicesService indicesService;
    private final Map<Index, IndexMetaData> danglingIndices = new HashMap<>();

    @Inject
    public DanglingIndicesState(Settings settings, NodeEnvironment nodeEnv, MetaStateService metaStateService,
                                LocalAllocateDangledIndices allocateDangledIndices, IndicesService indicesService) {
        super(settings);
        this.nodeEnv = nodeEnv;
        this.metaStateService = metaStateService;
        this.allocateDangledIndices = allocateDangledIndices;
        this.indicesService = indicesService;
    }

    /**
     * Process dangling indices based on the provided cluster state, handling cleanup, finding
     * new dangling indices, and allocating outstanding ones.
     */
    public void processDanglingIndices(final ClusterState clusterState) {
        // we are assuming we are running this functionality in the single threaded environment of the cluster state update thread
        assert InternalClusterService.assertClusterStateThread();
        if (nodeEnv.hasNodeFile() == false) {
            return;
        }
        final MetaData metaData = clusterState.metaData();
        cleanupAllocatedDangledIndices(metaData);
        final Map<Index, IndexMetaData> deletedIndices = findOnDiskAndAddDanglingIndices(metaData);
        cleanDeletedIndices(deletedIndices, clusterState);
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
     * Finds (@{link #findDanglingAndDeletedIndices}) and adds the new dangling indices
     * to the currently tracked dangling indices, while returning the indices to be deleted.
     */
    Map<Index, IndexMetaData> findOnDiskAndAddDanglingIndices(MetaData metaData) {
        Tuple<Map<Index, IndexMetaData>, Map<Index, IndexMetaData>> result = findDanglingAndDeletedIndices(metaData);
        danglingIndices.putAll(result.v1());
        return result.v2();
    }

    /**
     * Finds new dangling and deleted indices by iterating over the indices and trying to find indices
     * that have state on disk, but are not part of the provided meta data, or not detected
     * as dangled already.  The first tuple entry are the dangling indices, the second tuple entry are
     * the deleted indices.
     *
     * If the cluster UUID of the on-disk index matches the cluster UUID of the
     * cluster state, then we know that the master node of the cluster has indeed deleted the index on
     * this cluster, so we should delete it from disk.  A common scenario here is when a node holding
     * a shard copy for an index gets removed from the cluster, and while the node is removed, the index
     * gets deleted.  When the node comes back up, the index and its metadata exists on the node's disk,
     * but not in the cluster state, so it should be deleted instead of imported as dangling.
     *
     * If on the other hand, the cluster UUID of the on-disk index does not match the cluster UUID, the
     * likely scenario is that the master node had its data directory wiped out, so it started with a
     * fresh cluster UUID and no indices, so we want to import whatever indices exist on the node's disk
     * as a dangling index.
     */
    Tuple<Map<Index, IndexMetaData>, Map<Index, IndexMetaData>> findDanglingAndDeletedIndices(MetaData metaData) {
        final Set<String> excludeIndexPathIds = new HashSet<>(metaData.indices().size() + danglingIndices.size());
        for (ObjectCursor<IndexMetaData> cursor : metaData.indices().values()) {
            excludeIndexPathIds.add(cursor.value.getIndex().getUUID());
        }
        excludeIndexPathIds.addAll(danglingIndices.keySet().stream().map(Index::getUUID).collect(Collectors.toList()));
        try {
            final List<IndexStateMetaData> indexMetaDataList = metaStateService.loadIndicesStates(excludeIndexPathIds::contains);
            Map<Index, IndexMetaData> newIndices = new HashMap<>(indexMetaDataList.size());
            Map<Index, IndexMetaData> delIndices = new HashMap<>();
            for (IndexStateMetaData persistedIndex : indexMetaDataList) {
                if (persistedIndex != null) {
                    final Index index = persistedIndex.getIndexMetaData().getIndex();
                    final String clusterUUID = metaData.clusterUUID();
                    if (persistedIndex.isSameClusterUUID(clusterUUID)) {
                        // the cluster metadata's cluster UUID is the same as the cluster UUID that created this index,
                        // so the index shouldn't be imported as dangling but rather deleted and cleaned up, because the
                        // dangling files are probably due to remnants from deleted indices (e.g. the node was offline while
                        // the index was deleted) or failed cleanup
                        logger.info("[{}] dangling index exists on the local file system, but it is deleted in the metadata of the " +
                                    "same cluster that created it, deleting the index on the local file system", index);
                        delIndices.put(index, persistedIndex.getIndexMetaData());
                    } else if (metaData.hasIndex(index.getName())) {
                        logger.warn("[{}] can not be imported as a dangling index, as index with same name already exists " +
                                    "in cluster metadata", index);
                    } else {
                        logger.info("[{}] dangling index, exists on local file system, but not in cluster metadata, " +
                                    "auto import to cluster state", index);
                        newIndices.put(index, persistedIndex.getIndexMetaData());
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
    void cleanDeletedIndices(final Map<Index, IndexMetaData> deletedIndices, final ClusterState clusterState) {
        for (Map.Entry<Index, IndexMetaData> entry : deletedIndices.entrySet()) {
            final String indexName = entry.getKey().getName();
            final IndexMetaData indexMetaData = entry.getValue();
            try {
                indicesService.deleteIndexStore("index deleted but metadata still on file system", indexMetaData, clusterState, false);
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
            allocateDangledIndices.allocateDangled(Collections.unmodifiableCollection(new ArrayList<>(danglingIndices.values())),
                new LocalAllocateDangledIndices.Listener() {
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
