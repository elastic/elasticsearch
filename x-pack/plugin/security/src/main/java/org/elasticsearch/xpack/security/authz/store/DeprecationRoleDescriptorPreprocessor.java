/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authz.store;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission;

import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

import static org.elasticsearch.xpack.core.ClientHelper.SECURITY_ORIGIN;
import static org.elasticsearch.xpack.core.ClientHelper.executeAsyncWithOrigin;

public final class DeprecationRoleDescriptorPreprocessor implements BiConsumer<Set<RoleDescriptor>, ActionListener<Set<RoleDescriptor>>> {

    private final Logger logger;
    private final DeprecationLogger deprecationLogger;
    private final Client client;

    public DeprecationRoleDescriptorPreprocessor(Client client, Logger logger) {
        this.logger = logger;
        this.deprecationLogger = new DeprecationLogger(logger);
        this.client = client;
    }

    @Override
    public void accept(Set<RoleDescriptor> effectiveRoleDescriptors, ActionListener<Set<RoleDescriptor>> roleBuilder) {
        executeAsyncWithOrigin(client, SECURITY_ORIGIN, GetAliasesAction.INSTANCE,
                client.admin().indices().prepareGetAliases("_all").request(), ActionListener.wrap(getAliasesResponse -> {
                    final ImmutableOpenMap<String, List<AliasMetaData>> allAliasesMap = getAliasesResponse.getAliases();
                    // iterate over all effective role descriptors and their indices privileges
                    for (final RoleDescriptor roleDescriptor : effectiveRoleDescriptors) {
                        for (final IndicesPrivileges indicesPrivilege : roleDescriptor.getIndicesPrivileges()) {
                            final Predicate<String> indexNamePatternPredicate = IndicesPermission
                                    .indexMatcher(Arrays.asList(indicesPrivilege.getIndices()));
                            // iterate over all aliases
                            for (final ObjectObjectCursor<String, List<AliasMetaData>> cursor : allAliasesMap) {
                                final String indexName = cursor.key;
                                // the privilege does not match this index
                                if (false == indexNamePatternPredicate.test(indexName)) {
                                    for (final AliasMetaData aliasMetaData : cursor.value) {
                                        final String aliasName = aliasMetaData.alias();
                                        // but the privilege matches the alias pointing to the index
                                        if (indexNamePatternPredicate.test(aliasName)) {
                                            deprecationLogger.deprecated(
                                                    "Role [{}] grants index privileges over the [{}] alias"
                                                            + " and not over the [{}] index. Granting privileges over an alias"
                                                            + " and hence granting privileges over all the indices that it points to"
                                                            + " is deprecated and will be removed in a future version of Elasticsearch."
                                                            + " Instead define permissions exclusively on indices or index patterns.",
                                                    roleDescriptor.getName(), aliasName, indexName);
                                        }
                                    }
                                }
                            }
                        }
                    }
                    // forward the effective role descriptors untouched to the builder
                    roleBuilder.onResponse(effectiveRoleDescriptors);
                }, e -> {
                    // swallow exception: role building is not hindered by an exception in the deprecation logging logic
                    logger.debug("Swallowed exception encountered while checking Role Descriptors for deprecated features.", e);
                    // forward the effective role descriptors untouched to the builder
                    roleBuilder.onResponse(effectiveRoleDescriptors);
                }));
    }

}
