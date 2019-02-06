/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
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
import java.util.HashMap;
import java.util.HashSet;
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
                // sort answer by alias for tests
                final TreeMap<String, Set<String>> privilegesByIndexMap = new TreeMap<>();
                final Map<String, Set<String>> privilegesByAliasMap = new HashMap<>();
                // collate privileges by index and by alias separately
                for (final IndicesPrivileges indicesPrivilege : roleDescriptor.getIndicesPrivileges()) {
                    final Predicate<String> namePatternPredicate = IndicesPermission
                            .indexMatcher(Arrays.asList(indicesPrivilege.getIndices()));
                    for (final Map.Entry<String, AliasOrIndex> aliasOrIndex : aliasOrIndexMap.entrySet()) {
                        final String aliasOrIndexName = aliasOrIndex.getKey();
                        if (namePatternPredicate.test(aliasOrIndexName)) {
                            if (aliasOrIndex.getValue().isAlias()) {
                                final Set<String> privilegesByAlias = privilegesByAliasMap.computeIfAbsent(aliasOrIndexName,
                                        k -> new HashSet<String>());
                                privilegesByAlias.addAll(Arrays.asList(indicesPrivilege.getPrivileges()));
                            } else {
                                final Set<String> privilegesByIndex = privilegesByIndexMap.computeIfAbsent(aliasOrIndexName,
                                        k -> new HashSet<String>());
                                privilegesByIndex.addAll(Arrays.asList(indicesPrivilege.getPrivileges()));
                            }
                        }
                    }
                }
                // sort by alias and by index for tests
                final Map<String, Automaton> indexAutomatonMap = new HashMap<>();
                for (final Map.Entry<String, Set<String>> privilegesByAlias : privilegesByAliasMap.entrySet()) {
                    final String aliasName = privilegesByAlias.getKey();
                    final Set<String> aliasPrivileges = privilegesByAliasMap.get(aliasName);
                    final Automaton aliasPrivilegeAutomaton = IndexPrivilege.get(aliasPrivileges).getAutomaton();
                    final TreeSet<String> inferiorIndexNames = new TreeSet<>();
                    // check if the alias grants superiors privileges to the indices it points to
                    for (final IndexMetaData indexMetadata : aliasOrIndexMap.get(aliasName).getIndices()) {
                        final String indexName = indexMetadata.getIndex().getName();
                        final Set<String> indexPrivileges = privilegesByIndexMap.get(indexName);
                        // non null if this index matches the permission 
                        if (indexPrivileges == null) {
                            inferiorIndexNames.add(indexName);
                        } else {
                            final Automaton indexPrivilegeAutomaton = indexAutomatonMap.computeIfAbsent(indexName,
                                    i -> IndexPrivilege.get(indexPrivileges).getAutomaton());
                            if (false == Operations.subsetOf(indexPrivilegeAutomaton, aliasPrivilegeAutomaton)) {
                                inferiorIndexNames.add(indexName);
                            }
                        }
                    }
                    if (false == inferiorIndexNames.isEmpty()) {
                        final String logMessage = String.format(Locale.ROOT, ROLE_PERMISSION_DEPRECATION_STANZA, roleDescriptor.getName(),
                                aliasName, String.join(", ", inferiorIndexNames));
                        deprecationLogger.deprecated(logMessage);
                    }
                }
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
