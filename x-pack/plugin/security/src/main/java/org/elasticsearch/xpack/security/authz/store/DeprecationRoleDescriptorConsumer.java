/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

/**
 * Inspects all aliases that have greater privileges than the indices that they point to and logs the role descriptor, granting privileges
 * in this manner, as deprecated and requiring changes. This is done in preparation for the removal of the ability to define privileges over
 * aliases. The log messages are generated asynchronously and do not generate deprecation response headers. One log entry is generated for
 * each role descriptor and alias pair, and it contains all the indices for which privileges are a subset of those of the alias. In this
 * case, the administrator has to adjust the index privileges definition of the respective role such that name patterns do not cover aliases
 * (or rename aliases). If no logging is generated then the roles used for the current indices and aliases are not vulnerable to the
 * subsequent breaking change. However, there could be role descriptors that are not used (not mapped to a user that is currently using the
 * system) which are invisible to this check. Moreover, role descriptors can be dynamically added by role providers. In addition, role
 * descriptors are merged when building the effective role, so a role-alias pair reported as deprecated might not actually have an impact if
 * other role descriptors cover its indices. The check iterates over all indices and aliases for each role descriptor so it is quite
 * expensive computationally. For this reason the check is done only once a day for each role. If the role definitions stay the same, the
 * deprecations can change from one day to another only if aliases or indices are added.
 */
public final class DeprecationRoleDescriptorConsumer implements Consumer<Collection<RoleDescriptor>> {

    private static final String ROLE_PERMISSION_DEPRECATION_STANZA = "Role [%s] contains index privileges covering the [%s] alias but"
        + " which do not cover some of the indices that it points to [%s]. Granting privileges over an alias and hence granting"
        + " privileges over all the indices that the alias points to is deprecated and will be removed in a future version of"
        + " Elasticsearch. Instead define permissions exclusively on index names or index name patterns.";

    private static final Logger logger = LogManager.getLogger(DeprecationRoleDescriptorConsumer.class);

    private final DeprecationLogger deprecationLogger;
    private final ClusterService clusterService;
    private final ThreadPool threadPool;
    private final Object mutex;
    private final Queue<RoleDescriptor> workQueue;
    private boolean workerBusy;
    private final Set<String> dailyRoleCache;

    public DeprecationRoleDescriptorConsumer(ClusterService clusterService, ThreadPool threadPool) {
        this(clusterService, threadPool, DeprecationLogger.getLogger(DeprecationRoleDescriptorConsumer.class));
    }

    // package-private for testing
    DeprecationRoleDescriptorConsumer(ClusterService clusterService, ThreadPool threadPool, DeprecationLogger deprecationLogger) {
        this.deprecationLogger = deprecationLogger;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.mutex = new Object();
        this.workQueue = new LinkedList<>();
        this.workerBusy = false;
        // this String Set keeps "<date>-<role>" pairs so that we only log a role once a day.
        this.dailyRoleCache = Collections.newSetFromMap(new LinkedHashMap<String, Boolean>() {
            @Override
            protected boolean removeEldestEntry(final Map.Entry<String, Boolean> eldest) {
                return false == eldest.getKey().startsWith(todayISODate());
            }
        });
    }

    @Override
    public void accept(Collection<RoleDescriptor> effectiveRoleDescriptors) {
        synchronized (mutex) {
            for (RoleDescriptor roleDescriptor : effectiveRoleDescriptors) {
                if (dailyRoleCache.add(buildCacheKey(roleDescriptor))) {
                    workQueue.add(roleDescriptor);
                }
            }
            if (false == workerBusy) {
                workerBusy = true;
                try {
                    // spawn another worker on the generic thread pool
                    threadPool.generic().execute(new AbstractRunnable() {

                        @Override
                        public void onFailure(Exception e) {
                            logger.warn("Failed to produce role deprecation messages", e);
                            synchronized (mutex) {
                                final boolean hasMoreWork = workQueue.peek() != null;
                                if (hasMoreWork) {
                                    workerBusy = true; // just being paranoid :)
                                    try {
                                        threadPool.generic().execute(this);
                                    } catch (RejectedExecutionException e1) {
                                        workerBusy = false;
                                        logger.warn("Failed to start working on role alias permisssion deprecation messages", e1);
                                    }
                                } else {
                                    workerBusy = false;
                                }
                            }
                        }

                        @Override
                        protected void doRun() throws Exception {
                            while (true) {
                                final RoleDescriptor workItem;
                                synchronized (mutex) {
                                    workItem = workQueue.poll();
                                    if (workItem == null) {
                                        workerBusy = false;
                                        break;
                                    }
                                }
                                logger.trace("Begin role [" + workItem.getName() + "] check for alias permission deprecation");
                                // executing the check asynchronously will not conserve the generated deprecation response headers (which is
                                // what we want, because it's not the request that uses deprecated features, but rather the role definition.
                                // Furthermore, due to caching, we can't reliably associate response headers to every request).
                                logDeprecatedPermission(workItem);
                                logger.trace("Completed role [" + workItem.getName() + "] check for alias permission deprecation");
                            }
                        }
                    });
                } catch (RejectedExecutionException e) {
                    workerBusy = false;
                    logger.warn("Failed to start working on role alias permisssion deprecation messages", e);
                }
            }
        }
    }

    private void logDeprecatedPermission(RoleDescriptor roleDescriptor) {
        final SortedMap<String, IndexAbstraction> aliasOrIndexMap = clusterService.state().metadata().getIndicesLookup();
        final Map<String, Set<String>> privilegesByAliasMap = new HashMap<>();
        // sort answer by alias for tests
        final SortedMap<String, Set<String>> privilegesByIndexMap = new TreeMap<>();
        // collate privileges by index and by alias separately
        for (final IndicesPrivileges indexPrivilege : roleDescriptor.getIndicesPrivileges()) {
            final StringMatcher matcher = StringMatcher.of(Arrays.asList(indexPrivilege.getIndices()));
            for (final Map.Entry<String, IndexAbstraction> aliasOrIndex : aliasOrIndexMap.entrySet()) {
                final String aliasOrIndexName = aliasOrIndex.getKey();
                if (matcher.test(aliasOrIndexName)) {
                    if (aliasOrIndex.getValue().getType() == IndexAbstraction.Type.ALIAS) {
                        final Set<String> privilegesByAlias = privilegesByAliasMap.computeIfAbsent(aliasOrIndexName, k -> new HashSet<>());
                        privilegesByAlias.addAll(Arrays.asList(indexPrivilege.getPrivileges()));
                    } else {
                        final Set<String> privilegesByIndex = privilegesByIndexMap.computeIfAbsent(aliasOrIndexName, k -> new HashSet<>());
                        privilegesByIndex.addAll(Arrays.asList(indexPrivilege.getPrivileges()));
                    }
                }
            }
        }
        // compute privileges Automaton for each alias and for each of the indices it points to
        final Map<String, Automaton> indexAutomatonMap = new HashMap<>();
        for (Map.Entry<String, Set<String>> privilegesByAlias : privilegesByAliasMap.entrySet()) {
            final String aliasName = privilegesByAlias.getKey();
            final Set<String> aliasPrivilegeNames = privilegesByAlias.getValue();
            final Automaton aliasPrivilegeAutomaton = IndexPrivilege.get(aliasPrivilegeNames).getAutomaton();
            final SortedSet<String> inferiorIndexNames = new TreeSet<>();
            // check if the alias grants superiors privileges than the indices it points to
            for (Index index : aliasOrIndexMap.get(aliasName).getIndices()) {
                final Set<String> indexPrivileges = privilegesByIndexMap.get(index.getName());
                // null iff the index does not have *any* privilege
                if (indexPrivileges != null) {
                    // compute automaton once per index no matter how many times it is pointed to
                    final Automaton indexPrivilegeAutomaton = indexAutomatonMap.computeIfAbsent(
                        index.getName(),
                        i -> IndexPrivilege.get(indexPrivileges).getAutomaton()
                    );
                    if (false == Operations.subsetOf(indexPrivilegeAutomaton, aliasPrivilegeAutomaton)) {
                        inferiorIndexNames.add(index.getName());
                    }
                } else {
                    inferiorIndexNames.add(index.getName());
                }
            }
            // log inferior indices for this role, for this alias
            if (false == inferiorIndexNames.isEmpty()) {
                final String logMessage = String.format(
                    Locale.ROOT,
                    ROLE_PERMISSION_DEPRECATION_STANZA,
                    roleDescriptor.getName(),
                    aliasName,
                    String.join(", ", inferiorIndexNames)
                );
                deprecationLogger.warn(DeprecationCategory.SECURITY, "index_permissions_on_alias", logMessage);
            }
        }
    }

    private static String todayISODate() {
        return ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.BASIC_ISO_DATE);
    }

    // package-private for testing
    static String buildCacheKey(RoleDescriptor roleDescriptor) {
        return todayISODate() + "-" + roleDescriptor.getName();
    }
}
