/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.persistence;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.health.ClusterIndexHealth;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersions;

import java.util.List;
import java.util.Map;

public class IndexStateResolver {
    private static final Logger logger = LogManager.getLogger(IndexStateResolver.class);

    private volatile boolean checkOutdatedIndices;

    public IndexStateResolver(boolean checkOutdatedIndices) {
        this.checkOutdatedIndices = checkOutdatedIndices;
    }

    public void setCheckOutdatedIndices(boolean checkOutdatedIndices) {
        this.checkOutdatedIndices = checkOutdatedIndices;
    }

    public <T extends ProfilingIndexAbstraction> IndexState<T> getIndexState(ClusterState state, T index) {
        IndexMetadata metadata = index.indexMetadata(state);
        if (metadata == null) {
            return new IndexState<>(index, null, IndexStatus.NEEDS_CREATION);
        }
        if (metadata.getState() == IndexMetadata.State.CLOSE) {
            logger.warn(
                "Index [{}] is closed. This is likely to prevent Universal Profiling from functioning correctly",
                metadata.getIndex()
            );
            return new IndexState<>(index, metadata.getIndex(), IndexStatus.CLOSED);
        }
        final IndexRoutingTable routingTable = state.getRoutingTable().index(metadata.getIndex());
        ClusterHealthStatus indexHealth = new ClusterIndexHealth(metadata, routingTable).getStatus();
        if (indexHealth == ClusterHealthStatus.RED) {
            logger.trace("Index [{}] health status is RED, any pending mapping upgrades will wait until this changes", metadata.getIndex());
            return new IndexState<>(index, metadata.getIndex(), IndexStatus.UNHEALTHY);
        }
        if (checkOutdatedIndices && metadata.getCreationVersion().before(IndexVersions.V_8_9_1)) {
            logger.trace(
                "Index [{}] has been created before version 8.9.1 and must be deleted before proceeding with the upgrade.",
                metadata.getIndex()
            );
            return new IndexState<>(index, metadata.getIndex(), IndexStatus.TOO_OLD);
        }
        MappingMetadata mapping = metadata.mapping();
        if (mapping != null) {
            @SuppressWarnings("unchecked")
            Map<String, Object> meta = (Map<String, Object>) mapping.sourceAsMap().get("_meta");
            int currentIndexVersion;
            int currentTemplateVersion;
            if (meta == null) {
                logger.debug("Missing _meta field in mapping of index [{}], assuming initial version.", metadata.getIndex());
                currentIndexVersion = 1;
                currentTemplateVersion = 1;
            } else {
                // we are extra defensive and treat any unexpected values as an unhealthy index which we won't touch.
                currentIndexVersion = getVersionField(metadata.getIndex(), meta, "index-version");
                currentTemplateVersion = getVersionField(metadata.getIndex(), meta, "index-template-version");
                if (currentIndexVersion == -1 || currentTemplateVersion == -1) {
                    return new IndexState<>(index, metadata.getIndex(), IndexStatus.UNHEALTHY);
                }
            }
            if (index.getVersion() > currentIndexVersion) {
                return new IndexState<>(index, metadata.getIndex(), IndexStatus.NEEDS_VERSION_BUMP);
            } else if (getIndexTemplateVersion() > currentTemplateVersion) {
                // if there are no migrations we can consider the index up-to-date even if the index template version does not match.
                List<Migration> pendingMigrations = index.getMigrations(currentTemplateVersion);
                if (pendingMigrations.isEmpty()) {
                    logger.trace(
                        "Index [{}] with index template version [{}] (current is [{}]) is up-to-date (no pending migrations).",
                        metadata.getIndex(),
                        currentTemplateVersion,
                        getIndexTemplateVersion()
                    );
                    return new IndexState<>(index, metadata.getIndex(), IndexStatus.UP_TO_DATE);
                }
                logger.trace(
                    "Index [{}] with index template version [{}] (current is [{}])  has [{}] pending migrations.",
                    metadata.getIndex(),
                    currentTemplateVersion,
                    getIndexTemplateVersion(),
                    pendingMigrations.size()
                );
                return new IndexState<>(index, metadata.getIndex(), IndexStatus.NEEDS_MAPPINGS_UPDATE, pendingMigrations);
            } else {
                return new IndexState<>(index, metadata.getIndex(), IndexStatus.UP_TO_DATE);
            }
        } else {
            logger.warn("No mapping found for existing index [{}]. Index cannot be migrated.", metadata.getIndex());
            return new IndexState<>(index, metadata.getIndex(), IndexStatus.UNHEALTHY);
        }
    }

    private int getVersionField(Index index, Map<String, Object> meta, String fieldName) {
        Object value = meta.get(fieldName);
        if (value instanceof Integer) {
            return (int) value;
        }
        if (value == null) {
            logger.warn("Metadata version field [{}] of index [{}] is empty.", fieldName, index);
            return -1;
        }
        logger.warn("Metadata version field [{}] of index [{}] is [{}] (expected an integer).", fieldName, index, value);
        return -1;
    }

    // overridable for testing
    protected int getIndexTemplateVersion() {
        return ProfilingIndexTemplateRegistry.INDEX_TEMPLATE_VERSION;
    }

}
