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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.function.BiConsumer;
import java.util.function.Predicate;

public final class DeprecationRoleDescriptorPreprocessor implements BiConsumer<Set<RoleDescriptor>, ActionListener<Set<RoleDescriptor>>> {

    private final DeprecationLogger deprecationLogger;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Set<String> cacheKeys;

    public DeprecationRoleDescriptorPreprocessor(ClusterService clusterService, ThreadPool threadPool, Logger logger) {
        this.deprecationLogger = new DeprecationLogger(logger);
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.cacheKeys = Collections.newSetFromMap(Collections.synchronizedMap(new LinkedHashMap<String, Boolean>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<String, Boolean> eldest) {
                return size() > 128;
            }
        }));
    }

    @Override
    public void accept(Set<RoleDescriptor> effectiveRoleDescriptors, ActionListener<Set<RoleDescriptor>> roleBuilder) {
        threadPool.generic().execute(() -> {
            final SortedMap<String, AliasOrIndex> aliasOrIndexMap = clusterService.state().metaData().getAliasAndIndexLookup();
            // stash context as we do not want to propagate deprecation response headers
            try (ThreadContext.StoredContext ignore = threadPool.getThreadContext().stashContext()) {
                // iterate on indices privileges of all effective role descriptors
                for (final RoleDescriptor roleDescriptor : effectiveRoleDescriptors) {
                    for (final IndicesPrivileges indicesPrivilege : roleDescriptor.getIndicesPrivileges()) {
                        // build predicate lazily, only if the role-alias pair has not been checked today
                        Predicate<String> namePatternPredicate = null;
                        // iterate over all current aliases
                        for (final Map.Entry<String, AliasOrIndex> aliasOrIndex : aliasOrIndexMap.entrySet()) {
                            if (aliasOrIndex.getValue().isAlias()) {
                                final String aliasName = aliasOrIndex.getKey();
                                final String cacheKey = buildCacheKey(aliasName, roleDescriptor.getName());
                                // role-alias pair has been logged already today!
                                if (cacheKeys.contains(cacheKey)) {
                                    continue;
                                }
                                if (namePatternPredicate == null) {
                                    namePatternPredicate = IndicesPermission.indexMatcher(Arrays.asList(indicesPrivilege.getIndices()));
                                }
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
                                            cacheKeys.add(cacheKey);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });
        // forward the effective untouched role descriptors to the builder
        roleBuilder.onResponse(effectiveRoleDescriptors);
    }

    private String buildCacheKey(String aliasName, String roleName) {
        final String todayAsString = ZonedDateTime.now(ZoneId.of("UTC")).getDayOfWeek().toString();
        final StringBuilder sb = new StringBuilder();
        return sb.append(aliasName).append('-').append(roleName).append('-').append(todayAsString).toString();
    }
}
