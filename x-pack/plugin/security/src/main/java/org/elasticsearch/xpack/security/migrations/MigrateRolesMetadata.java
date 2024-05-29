/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.migrations;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.indices.SystemIndexMigrationTask;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;
import org.elasticsearch.xpack.security.support.SecuritySystemIndices;

import java.util.Collections;
import java.util.Set;

import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SecurityMainIndexMappingVersion.ADD_REMOTE_CLUSTER_AND_DESCRIPTION_FIELDS;

public class MigrateRolesMetadata implements SystemIndexMigrationTask {

    private static final Logger logger = LogManager.getLogger(MigrateRolesMetadata.class);
    private final SecuritySystemIndices securitySystemIndices;

    public MigrateRolesMetadata(SecuritySystemIndices securitySystemIndices) {
        this.securitySystemIndices = securitySystemIndices;
    }

    @Override
    public void migrate(ActionListener<Void> listener) {
        SecurityIndexManager indexManager = securitySystemIndices.getMainIndexManager();
        BoolQueryBuilder filterQuery = new BoolQueryBuilder().filter(QueryBuilders.termQuery("type", "role"))
            .mustNot(QueryBuilders.existsQuery("metadata_flattened"));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(filterQuery).size(0).trackTotalHits(true);
        SearchRequest countRequest = new SearchRequest(indexManager.getConcreteIndexName());
        countRequest.source(searchSourceBuilder);
        indexManager.getClient().search(countRequest, ActionListener.wrap(response -> {
            // If there are no roles, skip migration
            if (response.getHits().getTotalHits().value > 0) {
                logger.info("Preparing to migrate [" + response.getHits().getTotalHits().value + "] roles");
                updateRolesByQuery(indexManager, filterQuery, listener);
            } else {
                listener.onResponse(null);
            }
        }, listener::onFailure));
    }

    private static void updateRolesByQuery(SecurityIndexManager indexManager, BoolQueryBuilder filterQuery, ActionListener<Void> listener) {
        UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(indexManager.getConcreteIndexName());
        updateByQueryRequest.setQuery(filterQuery);
        updateByQueryRequest.setScript(
            new Script(ScriptType.INLINE, "painless", "ctx._source.metadata_flattened = ctx._source.metadata", Collections.emptyMap())
        );
        indexManager.getClient()
            .admin()
            .cluster()
            .execute(UpdateByQueryAction.INSTANCE, updateByQueryRequest, ActionListener.wrap(bulkByScrollResponse -> {
                logger.info("Migrated [" + bulkByScrollResponse.getTotal() + "] roles");
                listener.onResponse(null);
            }, listener::onFailure));
    }

    @Override
    public Set<NodeFeature> nodeFeaturesRequired() {
        return Set.of(SecuritySystemIndices.SECURITY_ROLES_METADATA_FLATTENED);
    }

    @Override
    public int minMappingVersion() {
        return ADD_REMOTE_CLUSTER_AND_DESCRIPTION_FIELDS.id();
    }

    @Override
    public boolean checkPreConditions() {
        return securitySystemIndices.getMainIndexManager().isReadyForIndexMigration();
    }
}
