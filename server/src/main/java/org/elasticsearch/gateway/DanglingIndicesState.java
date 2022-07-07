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
import org.elasticsearch.cluster.metadata.IndexGraveyard;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.emptyMap;

/**
 * The dangling indices state is responsible for finding new dangling indices (indices that have
 * their state written on disk, but don't exists in the metadata of the cluster).
 */
public class DanglingIndicesState {

    private static final Logger logger = LogManager.getLogger(DanglingIndicesState.class);

    private final MetaStateService metaStateService;
    private final ClusterService clusterService;

    @Inject
    public DanglingIndicesState(MetaStateService metaStateService, ClusterService clusterService) {
        this.metaStateService = metaStateService;
        this.clusterService = clusterService;
    }

    /**
     * Finds new dangling indices by iterating over the indices and trying to find indices
     * that have state on disk, but are not part of the provided metadata.
     * @return a map of currently-known dangling indices
     */
    public Map<Index, IndexMetadata> getDanglingIndices() {
        final Metadata metadata = this.clusterService.state().metadata();

        final Set<String> excludeIndexPathIds = Sets.newHashSetWithExpectedSize(metadata.indices().size());

        for (IndexMetadata indexMetadata : metadata.indices().values()) {
            excludeIndexPathIds.add(indexMetadata.getIndex().getUUID());
        }

        try {
            final List<IndexMetadata> indexMetadataList = metaStateService.loadIndicesStates(excludeIndexPathIds::contains);
            final Map<Index, IndexMetadata> danglingIndices = new HashMap<>();
            final IndexGraveyard graveyard = metadata.indexGraveyard();

            for (IndexMetadata indexMetadata : indexMetadataList) {
                Index index = indexMetadata.getIndex();
                if (graveyard.containsIndex(index) == false) {
                    danglingIndices.put(index, stripAliases(indexMetadata));
                }
            }

            return danglingIndices;
        } catch (IOException e) {
            logger.warn("failed to list dangling indices", e);
            return emptyMap();
        }
    }

    /**
     * Removes all aliases from the supplied index metadata.
     *
     * Importing dangling indices with aliases is dangerous, it could for instance result in inability to write to an existing alias if it
     * previously had only one index with any is_write_index indication.
     */
    private static IndexMetadata stripAliases(IndexMetadata indexMetadata) {
        if (indexMetadata.getAliases().isEmpty()) {
            return indexMetadata;
        } else {
            logger.info(
                "[{}] stripping aliases: {} from index before importing",
                indexMetadata.getIndex(),
                indexMetadata.getAliases().keySet()
            );
            return IndexMetadata.builder(indexMetadata).removeAllAliases().build();
        }
    }
}
