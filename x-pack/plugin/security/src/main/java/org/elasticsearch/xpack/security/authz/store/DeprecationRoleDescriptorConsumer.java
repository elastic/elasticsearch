/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Consumer;
import java.util.function.Predicate;

public final class DeprecationRoleDescriptorConsumer implements Consumer<Collection<RoleDescriptor>> {

    private static final String ROLE_PERMISSION_DEPRECATION_STANZA = "Role [%s] contains index privileges covering the [%s] alias but"
            + " which do not cover some of the indices that it points to [%s]. Granting privileges over an alias and hence granting"
            + " privileges over all the indices that the alias points to is deprecated and will be removed in a future version of"
            + " Elasticsearch. Instead define permissions exclusively on index names or index name patterns.";

    private final DeprecationLogger deprecationLogger;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Set<String> cacheKeys;

    public DeprecationRoleDescriptorConsumer(ClusterService clusterService, ThreadPool threadPool,
                                             DeprecationLogger deprecationLogger) {
        this.deprecationLogger = deprecationLogger;
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
    public void accept(Collection<RoleDescriptor> effectiveRoleDescriptors) {
        threadPool.generic().execute(() -> {
            final SortedMap<String, AliasOrIndex> aliasOrIndexMap = clusterService.state().metaData().getAliasAndIndexLookup();
            logDeprecatedPermission(effectiveRoleDescriptors, aliasOrIndexMap);
        });
    }

    private void logDeprecatedPermission(Collection<RoleDescriptor> effectiveRoleDescriptors,
                                         SortedMap<String, AliasOrIndex> aliasOrIndexMap) {
        for (final RoleDescriptor roleDescriptor : effectiveRoleDescriptors) {
            final String cacheKey = buildCacheKey(roleDescriptor.getName());
            if (false == isCached(cacheKey)) {
                // the aliases and their associated indices that are, respectively, matched and not matched by the permission
                final Map<String, Set<String>> indicesNotCoveredByAlias = new TreeMap<>();
                for (final IndicesPrivileges indicesPrivilege : roleDescriptor.getIndicesPrivileges()) {
                    Predicate<String> namePatternPredicate = null;
                    for (final Map.Entry<String, AliasOrIndex> aliasOrIndex : aliasOrIndexMap.entrySet()) {
                        if (aliasOrIndex.getValue().isAlias()) {
                            if (namePatternPredicate == null) {
                                namePatternPredicate = IndicesPermission.indexMatcher(Arrays.asList(indicesPrivilege.getIndices()));
                            }
                            final String aliasName = aliasOrIndex.getKey();
                            // the privilege name pattern matches the alias name
                            if (namePatternPredicate.test(aliasName)) {
                                for (final IndexMetaData indexMeta : aliasOrIndex.getValue().getIndices()) {
                                    final String indexName = indexMeta.getIndex().getName();
                                    // but it does not match the name of an index pointed to by the alias
                                    if (false == namePatternPredicate.test(indexName)) {
                                        final Set<String> indicesNotMatchedForAlias = indicesNotCoveredByAlias.computeIfAbsent(aliasName,
                                                k -> new TreeSet<String>());
                                        indicesNotMatchedForAlias.add(indexName);
                                    }
                                }
                            }
                        }
                    }
                }
                for (Map.Entry<String, Set<String>> grantedAliasNoIndex : indicesNotCoveredByAlias.entrySet()) {
                    final String logMessage = String.format(Locale.ROOT, ROLE_PERMISSION_DEPRECATION_STANZA, roleDescriptor.getName(),
                            grantedAliasNoIndex.getKey(), String.join(", ", grantedAliasNoIndex.getValue()));
                    deprecationLogger.deprecated(logMessage);
                }
                // mark role as checked for "today"
                addToCache(cacheKey);
            }
        }
        for (final RoleDescriptor roleDescriptor : effectiveRoleDescriptors) {
            final String cacheKey = buildCacheKey(roleDescriptor.getName());
            if (false == isCached(cacheKey)) {
                
                // mark role as checked for "today"
                addToCache(cacheKey);
            }
        }
    }

    private boolean isCached(String cacheKey) {
        return cacheKeys.contains(cacheKey);
    }

    private boolean addToCache(String cacheKey) {
        return cacheKeys.add(cacheKey);
    }

    // package-private for testing
    static String buildCacheKey(String roleName) {
        final String daysSinceEpoch = Long.toString(ZonedDateTime.now(ZoneId.of("UTC")).toEpochSecond() / 86400000L);
        final StringBuilder sb = new StringBuilder();
        return sb.append(roleName).append('-').append(daysSinceEpoch).toString();
    }
}
