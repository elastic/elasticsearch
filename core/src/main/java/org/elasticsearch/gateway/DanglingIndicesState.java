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

import com.google.common.collect.*;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.env.NodeEnvironment;

import java.util.Map;
import java.util.Set;

/**
 * The dangling indices state is responsible for finding new dangling indices (indices that have
 * their state written on disk, but don't exists in the metadata of the cluster), and importing
 * them into the cluster.
 */
public class DanglingIndicesState extends AbstractComponent {

    private final NodeEnvironment nodeEnv;
    private final MetaStateService metaStateService;
    private final LocalAllocateDangledIndices allocateDangledIndices;

    private final Map<String, IndexMetaData> danglingIndices = ConcurrentCollections.newConcurrentMap();

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
        if (nodeEnv.hasNodeFile() == false) {
            return;
        }
        cleanupAllocatedDangledIndices(metaData);
        findNewAndAddDanglingIndices(metaData);
        allocateDanglingIndices();
    }

    /**
     * The current set of dangling indices.
     */
    Map<String, IndexMetaData> getDanglingIndices() {
        return ImmutableMap.copyOf(danglingIndices);
    }

    /**
     * Cleans dangling indices if they are already allocated on the provided meta data.
     */
    void cleanupAllocatedDangledIndices(MetaData metaData) {
        for (String danglingIndex : danglingIndices.keySet()) {
            if (metaData.hasIndex(danglingIndex)) {
                logger.debug("[{}] no longer dangling (created), removing from dangling list", danglingIndex);
                danglingIndices.remove(danglingIndex);
            }
        }
    }

    /**
     * Finds (@{link #findNewAndAddDanglingIndices}) and adds the new dangling indices
     * to the currently tracked dangling indices.
     */
    void findNewAndAddDanglingIndices(MetaData metaData) {
        danglingIndices.putAll(findNewDanglingIndices(metaData));
    }

    /**
     * Finds new dangling indices by iterating over the indices and trying to find indices
     * that have state on disk, but are not part of the provided meta data, or not detected
     * as dangled already.
     */
    Map<String, IndexMetaData> findNewDanglingIndices(MetaData metaData) {
        final Set<String> indices;
        try {
            indices = nodeEnv.findAllIndices();
        } catch (Throwable e) {
            logger.warn("failed to list dangling indices", e);
            return ImmutableMap.of();
        }

        Map<String, IndexMetaData>  newIndices = Maps.newHashMap();
        for (String indexName : indices) {
            if (metaData.hasIndex(indexName) == false && danglingIndices.containsKey(indexName) == false) {
                try {
                    IndexMetaData indexMetaData = metaStateService.loadIndexState(indexName);
                    if (indexMetaData != null) {
                        logger.info("[{}] dangling index, exists on local file system, but not in cluster metadata, auto import to cluster state", indexName);
                        if (!indexMetaData.index().equals(indexName)) {
                            logger.info("dangled index directory name is [{}], state name is [{}], renaming to directory name", indexName, indexMetaData.index());
                            indexMetaData = IndexMetaData.builder(indexMetaData).index(indexName).build();
                        }
                        newIndices.put(indexName, indexMetaData);
                    } else {
                        logger.debug("[{}] dangling index directory detected, but no state found", indexName);
                    }
                } catch (Throwable t) {
                    logger.warn("[{}] failed to load index state for detected dangled index", t, indexName);
                }
            }
        }
        return newIndices;
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
            allocateDangledIndices.allocateDangled(ImmutableList.copyOf(danglingIndices.values()), new LocalAllocateDangledIndices.Listener() {
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
