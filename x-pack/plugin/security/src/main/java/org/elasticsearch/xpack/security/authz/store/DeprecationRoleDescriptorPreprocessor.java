/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

public final class DeprecationRoleDescriptorPreprocessor implements BiConsumer<Set<RoleDescriptor>, ActionListener<Set<RoleDescriptor>>> {

    private final DeprecationLogger deprecationLogger;
    private final ClusterService clusterService;

    public DeprecationRoleDescriptorPreprocessor(ClusterService clusterService, Logger logger) {
        this.deprecationLogger = new DeprecationLogger(logger);
        this.clusterService = clusterService;
    }

    @Override
    public void accept(Set<RoleDescriptor> effectiveRoleDescriptors, ActionListener<Set<RoleDescriptor>> roleBuilder) {
        final SortedMap<String, AliasOrIndex> aliasOrIndexMap = clusterService.state().metaData().getAliasAndIndexLookup();
        // iterate on indices privileges of all effective role descriptors
        for (final RoleDescriptor roleDescriptor : effectiveRoleDescriptors) {
            for (final IndicesPrivileges indicesPrivilege : roleDescriptor.getIndicesPrivileges()) {
                final Predicate<String> namePatternPredicate = IndicesPermission.indexMatcher(Arrays.asList(indicesPrivilege.getIndices()));
                // iterate over all current aliases
                for (final Map.Entry<String, AliasOrIndex> aliasOrIndex : aliasOrIndexMap.entrySet()) {
                    if (aliasOrIndex.getValue().isAlias()) {
                        final String aliasName = aliasOrIndex.getKey();
                        // the privilege name pattern matches the alias name
                        if (namePatternPredicate.test(aliasName)) {
                            for (final IndexMetaData indexMeta : aliasOrIndex.getValue().getIndices()) {
                                // but it does not match the name of an index pointed to by the alias
                                if (false == namePatternPredicate.test(indexMeta.getIndex().getName())) {
                                    deprecationLogger.deprecated(
                                            "Role [{}] grants index privileges over the [{}] alias"
                                                    + " and not over the [{}] index. Granting privileges over an alias"
                                                    + " and hence granting privileges over all the indices that it points to"
                                                    + " is deprecated and will be removed in a future version of Elasticsearch."
                                                    + " Instead define permissions exclusively on indices or index patterns.",
                                            roleDescriptor.getName(), aliasName, indexMeta.getIndex().getName());
                                }
                            }
                        }
                    }
                }
            }
        }
        // forward the effective role descriptors untouched to the builder
        roleBuilder.onResponse(effectiveRoleDescriptors);
    }
}
