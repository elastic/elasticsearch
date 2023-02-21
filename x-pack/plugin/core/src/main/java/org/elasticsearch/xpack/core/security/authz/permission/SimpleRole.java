/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.lucene.util.automaton.Automaton;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.PrivilegesCheckResult;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.PrivilegesToCheck;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.IndicesPermission.IsResourceAuthorizedPredicate;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilege;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class SimpleRole implements Role {

    public static final Setting<Integer> CACHE_SIZE_SETTING = Setting.intSetting(
        "xpack.security.authz.store.roles.has_privileges.cache.max_size",
        1000,
        Setting.Property.NodeScope
    );

    private final String[] names;
    private final ClusterPermission cluster;
    private final IndicesPermission indices;
    private final ApplicationPermission application;
    private final RunAsPermission runAs;
    private final RemoteIndicesPermission remoteIndices;

    SimpleRole(
        String[] names,
        ClusterPermission cluster,
        IndicesPermission indices,
        ApplicationPermission application,
        RunAsPermission runAs,
        RemoteIndicesPermission remoteIndices
    ) {
        this.names = names;
        this.cluster = Objects.requireNonNull(cluster);
        this.indices = Objects.requireNonNull(indices);
        this.application = Objects.requireNonNull(application);
        this.runAs = Objects.requireNonNull(runAs);
        this.remoteIndices = Objects.requireNonNull(remoteIndices);
    }

    @Override
    public String[] names() {
        return names;
    }

    @Override
    public ClusterPermission cluster() {
        return cluster;
    }

    @Override
    public IndicesPermission indices() {
        return indices;
    }

    @Override
    public ApplicationPermission application() {
        return application;
    }

    @Override
    public RunAsPermission runAs() {
        return runAs;
    }

    @Override
    public RemoteIndicesPermission remoteIndices() {
        return remoteIndices;
    }

    @Override
    public boolean hasFieldOrDocumentLevelSecurity() {
        return indices.hasFieldOrDocumentLevelSecurity();
    }

    @Override
    public IsResourceAuthorizedPredicate allowedIndicesMatcher(String action) {
        return indices.allowedIndicesMatcher(action);
    }

    @Override
    public Automaton allowedActionsMatcher(String index) {
        return indices.allowedActionsMatcher(index);
    }

    @Override
    public boolean checkRunAs(String runAsName) {
        return runAs.check(runAsName);
    }

    @Override
    public boolean checkIndicesAction(String action) {
        return indices.check(action);
    }

    @Override
    public boolean checkIndicesPrivileges(
        Set<String> checkForIndexPatterns,
        boolean allowRestrictedIndices,
        Set<String> checkForPrivileges,
        @Nullable ResourcePrivilegesMap.Builder resourcePrivilegesMapBuilder
    ) {
        return indices.checkResourcePrivileges(
            checkForIndexPatterns,
            allowRestrictedIndices,
            checkForPrivileges,
            resourcePrivilegesMapBuilder
        );
    }

    @Override
    public boolean checkClusterAction(String action, TransportRequest request, Authentication authentication) {
        return cluster.check(action, request, authentication);
    }

    @Override
    public boolean grants(ClusterPrivilege clusterPrivilege) {
        return cluster.implies(clusterPrivilege.buildPermission(ClusterPermission.builder()).build());
    }

    @Override
    public boolean checkApplicationResourcePrivileges(
        final String applicationName,
        Set<String> checkForResources,
        Set<String> checkForPrivilegeNames,
        Collection<ApplicationPrivilegeDescriptor> storedPrivileges,
        @Nullable ResourcePrivilegesMap.Builder resourcePrivilegesMapBuilder
    ) {
        return application.checkResourcePrivileges(
            applicationName,
            checkForResources,
            checkForPrivilegeNames,
            storedPrivileges,
            resourcePrivilegesMapBuilder
        );
    }

    @Override
    public IndicesAccessControl authorize(
        String action,
        Set<String> requestedIndicesOrAliases,
        Map<String, IndexAbstraction> aliasAndIndexLookup,
        FieldPermissionsCache fieldPermissionsCache
    ) {
        return indices.authorize(action, requestedIndicesOrAliases, aliasAndIndexLookup, fieldPermissionsCache);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SimpleRole that = (SimpleRole) o;
        return Arrays.equals(this.names, that.names)
            && this.cluster.equals(that.cluster)
            && this.indices.equals(that.indices)
            && this.application.equals(that.application)
            && this.runAs.equals(that.runAs);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(cluster, indices, application, runAs);
        result = 31 * result + Arrays.hashCode(names);
        return result;
    }

    private final AtomicReference<Cache<PrivilegesToCheck, PrivilegesCheckResult>> hasPrivilegesCacheReference = new AtomicReference<>();

    public void cacheHasPrivileges(Settings settings, PrivilegesToCheck privilegesToCheck, PrivilegesCheckResult privilegesCheckResult)
        throws ExecutionException {
        Cache<PrivilegesToCheck, PrivilegesCheckResult> cache = hasPrivilegesCacheReference.get();
        if (cache == null) {
            final CacheBuilder<PrivilegesToCheck, PrivilegesCheckResult> cacheBuilder = CacheBuilder.builder();
            final int cacheSize = CACHE_SIZE_SETTING.get(settings);
            if (cacheSize >= 0) {
                cacheBuilder.setMaximumWeight(cacheSize);
            }
            hasPrivilegesCacheReference.compareAndSet(null, cacheBuilder.build());
            cache = hasPrivilegesCacheReference.get();
        }
        cache.computeIfAbsent(privilegesToCheck, ignore -> privilegesCheckResult);
    }

    public PrivilegesCheckResult checkPrivilegesWithCache(PrivilegesToCheck privilegesToCheck) {
        final Cache<PrivilegesToCheck, PrivilegesCheckResult> cache = hasPrivilegesCacheReference.get();
        if (cache == null) {
            return null;
        }
        return cache.get(privilegesToCheck);
    }

    // package private for testing
    Cache<PrivilegesToCheck, PrivilegesCheckResult> getHasPrivilegesCache() {
        return hasPrivilegesCacheReference.get();
    }
}
