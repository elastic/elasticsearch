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
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.UpdateByQueryAction;
import org.elasticsearch.index.reindex.UpdateByQueryRequest;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.xpack.core.security.action.rolemapping.DeleteRoleMappingRequestBuilder;
import org.elasticsearch.xpack.core.security.action.rolemapping.GetRoleMappingsRequestBuilder;
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authz.RoleMappingMetadata;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.elasticsearch.xpack.security.action.rolemapping.ReservedRoleMappingAction.getFileSettingsMetadataHandlerRoleMappingKeys;
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
         * @param securityIndexManager manager for the main security index
         * @param client the index client
         * @param listener listener to provide updates back to caller
         */
        void migrate(SecurityIndexManager securityIndexManager, Client client, ActionListener<Void> listener);

        /**
         * Check preconditions to make sure the cluster is ready for the migration
         *
         * @param clusterState state of the cluster
         *
         * @return true if preconditions are met, false otherwise
         */
        default boolean checkPreconditions(ClusterState clusterState) {
            return true;
        }

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
    public static final Integer OPERATOR_DEFINED_ROLE_MAPPINGS_CLEANUP = 2;

    public static final TreeMap<Integer, SecurityMigration> MIGRATIONS_BY_VERSION = new TreeMap<>(
        Map.of(ROLE_METADATA_FLATTENED_MIGRATION_VERSION, new SecurityMigration() {
            private static final Logger logger = LogManager.getLogger(SecurityMigration.class);

            @Override
            public void migrate(SecurityIndexManager securityIndexManager, Client client, ActionListener<Void> listener) {
                BoolQueryBuilder filterQuery = new BoolQueryBuilder().filter(QueryBuilders.termQuery("type", "role"))
                    .mustNot(QueryBuilders.existsQuery("metadata_flattened"));
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(filterQuery).size(0).trackTotalHits(true);
                SearchRequest countRequest = new SearchRequest(securityIndexManager.getConcreteIndexName());
                countRequest.source(searchSourceBuilder);

                client.search(countRequest, ActionListener.wrap(response -> {
                    // If there are no roles, skip migration
                    if (response.getHits().getTotalHits().value > 0) {
                        logger.info("Preparing to migrate [" + response.getHits().getTotalHits().value + "] roles");
                        updateRolesByQuery(securityIndexManager.getConcreteIndexName(), client, filterQuery, listener);
                    } else {
                        listener.onResponse(null);
                    }
                }, listener::onFailure));
            }

            private void updateRolesByQuery(String indexName, Client client, BoolQueryBuilder filterQuery, ActionListener<Void> listener) {
                UpdateByQueryRequest updateByQueryRequest = new UpdateByQueryRequest(indexName);
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
        }, OPERATOR_DEFINED_ROLE_MAPPINGS_CLEANUP, new SecurityMigration() {
            private static final Logger logger = LogManager.getLogger(SecurityMigration.class);

            @Override
            public void migrate(SecurityIndexManager securityIndexManager, Client client, ActionListener<Void> listener) {
                if (securityIndexManager.getClusterStateRoleMappings().isEmpty()) {
                    listener.onResponse(null);
                }

                getNativeRoleMappingsToDelete(
                    client,
                    securityIndexManager.getClusterStateRoleMappings().stream().map(ExpressionRoleMapping::getName).toList(),
                    ActionListener.wrap(
                        roleMappingIds -> deleteNativeRoleMappings(client, roleMappingIds.iterator(), listener),
                        listener::onFailure
                    )
                );
            }

            private void getNativeRoleMappingsToDelete(
                Client client,
                List<String> clusterStateRoleMappingNames,
                ActionListener<List<String>> listener
            ) {
                if (clusterStateRoleMappingNames.isEmpty()) {
                    listener.onResponse(List.of());
                    return;
                }
                getNativeRoleMappingFromNames(client, listener, clusterStateRoleMappingNames.toArray(String[]::new));
            }

            private void getNativeRoleMappingFromNames(Client client, ActionListener<List<String>> listener, String... names) {
                new GetRoleMappingsRequestBuilder(client).names(names).execute(ActionListener.wrap(response -> {
                    listener.onResponse(Arrays.stream(response.mappings()).map(ExpressionRoleMapping::getName).toList());
                }, listener::onFailure));
            }

            private void deleteNativeRoleMappings(Client client, Iterator<String> namesIterator, ActionListener<Void> listener) {
                String name = namesIterator.next();
                new DeleteRoleMappingRequestBuilder(client).name(name)
                    .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                    .execute(ActionListener.wrap(response -> {
                        if (response.isFound() == false) {
                            logger.warn("Expected role mapping [" + name + "] not found during role mapping clean up.");
                        }
                        if (namesIterator.hasNext()) {
                            deleteNativeRoleMappings(client, namesIterator, listener);
                        } else {
                            listener.onResponse(null);
                        }
                    }, listener::onFailure));
            }

            @Override
            public boolean checkPreconditions(ClusterState clusterState) {
                if (getFileSettingsMetadataHandlerRoleMappingKeys(clusterState).isEmpty()) {
                    // No operator defined role mappings, so doesn't need to be ready since cleanup is a noop
                    return true;
                }

                // TODO Change this to check version of role mapping metadata when available. So, if version is up to date -> needs upgrade
                if (RoleMappingMetadata.getFromClusterState(clusterState).getRoleMappings().stream().anyMatch((roleMapping) -> true)) {
                    // Version is up-to-date for role mapping so ready for cleanup
                    return true;
                }

                // Version is not up-to-date, can't trigger cleanup
                return false;
            }

            @Override
            public Set<NodeFeature> nodeFeaturesRequired() {
                return Set.of(SecuritySystemIndices.SECURITY_ROLE_MAPPING_CLEANUP);
            }

            @Override
            public int minMappingVersion() {
                return ADD_REMOTE_CLUSTER_AND_DESCRIPTION_FIELDS.id();
            }
        })
    );
}
