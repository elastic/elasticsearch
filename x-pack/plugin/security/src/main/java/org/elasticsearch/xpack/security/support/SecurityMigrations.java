/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.support;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.elasticsearch.xpack.security.support.SecuritySystemIndices.SecurityMainIndexMappingVersion.ADD_REMOTE_CLUSTER_AND_DESCRIPTION_FIELDS;

/**
 * Interface for creating SecurityMigrations that will be automatically applied once to existing .security indices
 * IMPORTANT: A new index version needs to be added to {@link org.elasticsearch.index.IndexVersions} for the migration to be triggered
 */
public class SecurityMigrations {

    public interface SecurityMigration {
        /**
         * Method that will execute the actual migration - needs to be idempotent and non-blocking
         *
         * @param indexManager for the security index
         * @param client the index client
         * @param listener listener to provide updates back to caller
         */
        void migrate(SecurityIndexManager indexManager, Client client, ActionListener<Void> listener);

        /**
         * Any node features that are required for this migration to run. This makes sure that all nodes in the cluster can handle any
         * changes in behaviour introduced by the migration.
         *
         * @return a set of features needed to be supported or an empty set if no change in behaviour is expected
         */
        Set<NodeFeature> nodeFeaturesRequired();

        /**
         * The min mapping version required to support this migration. This makes sure that the index has at least the min mapping that is
         * required to support the migration.
         *
         * @return the minimum mapping version required to apply this migration
         */
        int minMappingVersion();
    }

    public static final Integer ROLE_METADATA_FLATTENED_MIGRATION_VERSION = 1;

    public static final TreeMap<Integer, SecurityMigration> MIGRATIONS_BY_VERSION = new TreeMap<>(
        Map.of(ROLE_METADATA_FLATTENED_MIGRATION_VERSION, new SecurityMigration() {
            private static final Logger logger = LogManager.getLogger(SecurityMigration.class);

            @Override
            public void migrate(SecurityIndexManager indexManager, Client client, ActionListener<Void> listener) {
                BoolQueryBuilder filterQuery = new BoolQueryBuilder().filter(QueryBuilders.termQuery("type", "role"))
                    .mustNot(QueryBuilders.existsQuery("metadata_flattened"));
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(filterQuery).size(0).trackTotalHits(true);
                SearchRequest countRequest = new SearchRequest(indexManager.getConcreteIndexName());
                countRequest.source(searchSourceBuilder);

                client.search(countRequest, ActionListener.wrap(response -> {
                    // If there are no roles, skip migration
                    if (response.getHits().getTotalHits().value() > 0) {
                        logger.info("Preparing to migrate [" + response.getHits().getTotalHits().value() + "] roles");
                        updateRolesByQuery(indexManager, client, filterQuery, listener);
                    } else {
                        listener.onResponse(null);
                    }
                }, listener::onFailure));
            }

            private void updateRolesByQuery(
                SecurityIndexManager indexManager,
                Client client,
                BoolQueryBuilder filterQuery,
                ActionListener<Void> listener
            ) {
                UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(indexManager.getConcreteIndexName());
                updateByQueryRequest.setQuery(filterQuery);
                updateByQueryRequest.setScript(
                    new Script(
                        ScriptType.INLINE,
                        "painless",
                        "ctx._source.metadata_flattened = ctx._source.metadata",
                        Collections.emptyMap()
                    )
                );
                client.admin()
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
        })
    );
}
