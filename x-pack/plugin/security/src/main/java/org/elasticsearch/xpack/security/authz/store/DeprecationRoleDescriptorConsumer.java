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
import java.time.format.DateTimeFormatter;
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

/**
 * Inspects for aliases that have greater privileges than the indices that they point to and logs the role descriptor as deprecated.
 * This is done in preparation for the removal of the ability to define privileges over aliases.
 * The log messages are generated asynchronously and do not generate deprecation response headers.
 * One log entry is generated for each role descriptor and alias pair, and it contains all the indices for which
 * privileges are a subset of those of the alias. In this case, the administrator has to adjust the index privileges
 * definition such that name patterns do not cover aliases.
 * If no logging is generated then the roles used for the current indices and aliases are not vulnerable to the
 * ensuing breaking change. However, there could be role descriptors that are never used and are invisible to this check. Moreover,
 * role descriptors can be dynamically added by role providers. In addition, role descriptors are merged when building the effective
 * role, so a role name reported as deprecated might not actually have an impact (if other role descriptors cover its indices).
 * The check iterates over all indices and aliases for each role descriptor so it is quite expensive computationally.
 * For this reason the check is done only once a day for each role. If the role definitions stay the same, the deprecations
 * can change from one day to another only if aliases or indices are added.
 */
public final class DeprecationRoleDescriptorConsumer implements Consumer<Collection<RoleDescriptor>> {

    private static final String ROLE_PERMISSION_DEPRECATION_STANZA = "Role [%s] contains index privileges covering the [%s] alias but"
            + " which do not cover some of the indices that it points to [%s]. Granting privileges over an alias and hence granting"
            + " privileges over all the indices that the alias points to is deprecated and will be removed in a future version of"
            + " Elasticsearch. Instead define permissions exclusively on index names or index name patterns.";

    private final DeprecationLogger deprecationLogger;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Set<String> dailyRoleCache;

    public DeprecationRoleDescriptorConsumer(ClusterService clusterService, ThreadPool threadPool,
                                             DeprecationLogger deprecationLogger) {
        this.deprecationLogger = deprecationLogger;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        // this String Set keeps "<role>-<date>" pairs so that we only log a role once a day.
        this.dailyRoleCache = Collections.newSetFromMap(Collections.synchronizedMap(new LinkedHashMap<String, Boolean>() {
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
            // executing the check asynchronously will not conserve the generated deprecation response headers (which is what we want,
            // because it's not the request that uses deprecated features, but rather the role definition. Plus, due to caching, we can't
            // reliably associate response headers to every request).
            logDeprecatedPermission(effectiveRoleDescriptors, aliasOrIndexMap);
        });
    }

    private void logDeprecatedPermission(Collection<RoleDescriptor> effectiveRoleDescriptors,
                                         SortedMap<String, AliasOrIndex> aliasOrIndexMap) {
        for (final RoleDescriptor roleDescriptor : effectiveRoleDescriptors) {
            final String cacheKey = buildCacheKey(roleDescriptor.getName());
            if (false == isCached(cacheKey)) {
                final Map<String, Set<String>> privilegesByAliasMap = new HashMap<>();
                // sort answer by alias for tests
                final SortedMap<String, Set<String>> privilegesByIndexMap = new TreeMap<>();
                // collate privileges by index and by alias separately
                for (final IndicesPrivileges indexPrivilege : roleDescriptor.getIndicesPrivileges()) {
                    final Predicate<String> namePatternPredicate = IndicesPermission
                            .indexMatcher(Arrays.asList(indexPrivilege.getIndices()));
                    for (final Map.Entry<String, AliasOrIndex> aliasOrIndex : aliasOrIndexMap.entrySet()) {
                        final String aliasOrIndexName = aliasOrIndex.getKey();
                        if (namePatternPredicate.test(aliasOrIndexName)) {
                            if (aliasOrIndex.getValue().isAlias()) {
                                final Set<String> privilegesByAlias = privilegesByAliasMap.computeIfAbsent(aliasOrIndexName,
                                        k -> new HashSet<String>());
                                privilegesByAlias.addAll(Arrays.asList(indexPrivilege.getPrivileges()));
                            } else {
                                final Set<String> privilegesByIndex = privilegesByIndexMap.computeIfAbsent(aliasOrIndexName,
                                        k -> new HashSet<String>());
                                privilegesByIndex.addAll(Arrays.asList(indexPrivilege.getPrivileges()));
                            }
                        }
                    }
                }
                // compute privileges Automaton for each alias and for each of the indices it points to
                final Map<String, Automaton> indexAutomatonMap = new HashMap<>();
                for (final Map.Entry<String, Set<String>> privilegesByAlias : privilegesByAliasMap.entrySet()) {
                    final String aliasName = privilegesByAlias.getKey();
                    final Set<String> aliasPrivilegeNames = privilegesByAlias.getValue();
                    final Automaton aliasPrivilegeAutomaton = IndexPrivilege.get(aliasPrivilegeNames).getAutomaton();
                    final TreeSet<String> inferiorIndexNames = new TreeSet<>();
                    // check if the alias grants superiors privileges than the indices it points to
                    for (final IndexMetaData indexMetadata : aliasOrIndexMap.get(aliasName).getIndices()) {
                        final String indexName = indexMetadata.getIndex().getName();
                        final Set<String> indexPrivileges = privilegesByIndexMap.get(indexName);
                        // null iff the index does not have *any* privilege
                        if (indexPrivileges != null) {
                            // compute automaton once per index no matter how many times it is pointed to
                            final Automaton indexPrivilegeAutomaton = indexAutomatonMap.computeIfAbsent(indexName,
                                    i -> IndexPrivilege.get(indexPrivileges).getAutomaton());
                            if (false == Operations.subsetOf(indexPrivilegeAutomaton, aliasPrivilegeAutomaton)) {
                                inferiorIndexNames.add(indexName);
                            }
                        } else {
                            inferiorIndexNames.add(indexName);
                        }
                    }
                    // log inferior indices for this role, for this alias
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
        return dailyRoleCache.contains(cacheKey);
    }

    private boolean addToCache(String cacheKey) {
        return dailyRoleCache.add(cacheKey);
    }

    // package-private for testing
    static String buildCacheKey(String roleName) {
        final String daysSinceEpoch =  ZonedDateTime.now(ZoneId.of("UTC")).format(DateTimeFormatter.BASIC_ISO_DATE);
        final StringBuilder sb = new StringBuilder();
        return sb.append(roleName).append('-').append(daysSinceEpoch).toString();
    }
}
