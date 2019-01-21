/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.common.IteratingActionListener;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition.FieldGrantExcludeGroup;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConditionalClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.Privilege;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isIndexDeleted;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isMoveFromRedToNonRed;

/**
 * A composite roles store that combines built in roles, file-based roles, and index-based roles. Checks the built in roles first, then the
 * file roles, and finally the index roles.
 */
public class CompositeRolesStore {


    private static final Setting<Integer> CACHE_SIZE_SETTING =
        Setting.intSetting("xpack.security.authz.store.roles.cache.max_size", 10000, Property.NodeScope);
    private static final Setting<Integer> NEGATIVE_LOOKUP_CACHE_SIZE_SETTING =
        Setting.intSetting("xpack.security.authz.store.roles.negative_lookup_cache.max_size", 10000, Property.NodeScope);
    private static final Logger logger = LogManager.getLogger(CompositeRolesStore.class);

    // the lock is used in an odd manner; when iterating over the cache we cannot have modifiers other than deletes using
    // the iterator but when not iterating we can modify the cache without external locking. When making normal modifications to the cache
    // the read lock is obtained so that we can allow concurrent modifications; however when we need to iterate over the keys or values of
    // the cache the write lock must obtained to prevent any modifications
    private final ReleasableLock readLock;
    private final ReleasableLock writeLock;

    {
        final ReadWriteLock iterationLock = new ReentrantReadWriteLock();
        readLock = new ReleasableLock(iterationLock.readLock());
        writeLock = new ReleasableLock(iterationLock.writeLock());
    }

    private final FileRolesStore fileRolesStore;
    private final NativeRolesStore nativeRolesStore;
    private final NativePrivilegeStore privilegeStore;
    private final XPackLicenseState licenseState;
    private final Cache<Set<String>, Role> roleCache;
    private final Cache<String, Boolean> negativeLookupCache;
    private final ThreadContext threadContext;
    private final AtomicLong numInvalidation = new AtomicLong();
    private final List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>> builtInRoleProviders;
    private final List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>> allRoleProviders;

    public CompositeRolesStore(Settings settings, FileRolesStore fileRolesStore, NativeRolesStore nativeRolesStore,
                               ReservedRolesStore reservedRolesStore, NativePrivilegeStore privilegeStore,
                               List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>> rolesProviders,
                               ThreadContext threadContext, XPackLicenseState licenseState) {
        this.fileRolesStore = fileRolesStore;
        fileRolesStore.addListener(this::invalidate);
        this.nativeRolesStore = nativeRolesStore;
        this.privilegeStore = privilegeStore;
        this.licenseState = licenseState;
        CacheBuilder<Set<String>, Role> builder = CacheBuilder.builder();
        final int cacheSize = CACHE_SIZE_SETTING.get(settings);
        if (cacheSize >= 0) {
            builder.setMaximumWeight(cacheSize);
        }
        this.roleCache = builder.build();
        this.threadContext = threadContext;
        CacheBuilder<String, Boolean> nlcBuilder = CacheBuilder.builder();
        final int nlcCacheSize = NEGATIVE_LOOKUP_CACHE_SIZE_SETTING.get(settings);
        if (nlcCacheSize >= 0) {
            nlcBuilder.setMaximumWeight(nlcCacheSize);
        }
        this.negativeLookupCache = nlcBuilder.build();
        this.builtInRoleProviders = Collections.unmodifiableList(Arrays.asList(reservedRolesStore, fileRolesStore, nativeRolesStore));
        if (rolesProviders.isEmpty()) {
            this.allRoleProviders = this.builtInRoleProviders;
        } else {
            List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>> allList =
                new ArrayList<>(builtInRoleProviders.size() + rolesProviders.size());
            allList.addAll(builtInRoleProviders);
            allList.addAll(rolesProviders);
            this.allRoleProviders = Collections.unmodifiableList(allList);
        }
    }

    public void roles(Set<String> roleNames, FieldPermissionsCache fieldPermissionsCache, ActionListener<Role> roleActionListener) {
        Role existing = roleCache.get(roleNames);
        if (existing != null) {
            roleActionListener.onResponse(existing);
        } else {
            final long invalidationCounter = numInvalidation.get();
            roleDescriptors(roleNames, ActionListener.wrap(
                    rolesRetrievalResult -> {
                        final boolean missingRoles = rolesRetrievalResult.getMissingRoles().isEmpty() == false;
                        if (missingRoles) {
                            logger.debug("Could not find roles with names {}", rolesRetrievalResult.getMissingRoles());
                        }

                        final Set<RoleDescriptor> effectiveDescriptors;
                        if (licenseState.isDocumentAndFieldLevelSecurityAllowed()) {
                            effectiveDescriptors = rolesRetrievalResult.getRoleDescriptors();
                        } else {
                            effectiveDescriptors = rolesRetrievalResult.getRoleDescriptors().stream()
                                    .filter((rd) -> rd.isUsingDocumentOrFieldLevelSecurity() == false)
                                    .collect(Collectors.toSet());
                        }
                        logger.trace("Building role from descriptors [{}] for names [{}]", effectiveDescriptors, roleNames);
                        buildRoleFromDescriptors(effectiveDescriptors, fieldPermissionsCache, privilegeStore, ActionListener.wrap(role -> {
                            if (role != null && rolesRetrievalResult.isSuccess()) {
                                try (ReleasableLock ignored = readLock.acquire()) {
                                    /* this is kinda spooky. We use a read/write lock to ensure we don't modify the cache if we hold
                                     * the write lock (fetching stats for instance - which is kinda overkill?) but since we fetching
                                     * stuff in an async fashion we need to make sure that if the cache got invalidated since we
                                     * started the request we don't put a potential stale result in the cache, hence the
                                     * numInvalidation.get() comparison to the number of invalidation when we started. we just try to
                                     * be on the safe side and don't cache potentially stale results
                                     */
                                    if (invalidationCounter == numInvalidation.get()) {
                                        roleCache.computeIfAbsent(roleNames, (s) -> role);
                                    }
                                }

                                for (String missingRole : rolesRetrievalResult.getMissingRoles()) {
                                    negativeLookupCache.computeIfAbsent(missingRole, s -> Boolean.TRUE);
                                }
                            }
                            roleActionListener.onResponse(role);
                        }, roleActionListener::onFailure));
                    },
                    roleActionListener::onFailure));
        }
    }

    private void roleDescriptors(Set<String> roleNames, ActionListener<RolesRetrievalResult> rolesResultListener) {
        final Set<String> filteredRoleNames = roleNames.stream().filter((s) -> {
            if (negativeLookupCache.get(s) != null) {
                logger.debug("Requested role [{}] does not exist (cached)", s);
                return false;
            } else {
                return true;
            }
        }).collect(Collectors.toSet());

        loadRoleDescriptorsAsync(filteredRoleNames, rolesResultListener);
    }

    private void loadRoleDescriptorsAsync(Set<String> roleNames, ActionListener<RolesRetrievalResult> listener) {
        final RolesRetrievalResult rolesResult = new RolesRetrievalResult();
        final List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>> asyncRoleProviders =
            licenseState.isCustomRoleProvidersAllowed() ? allRoleProviders : builtInRoleProviders;

        final ActionListener<RoleRetrievalResult> descriptorsListener =
            ContextPreservingActionListener.wrapPreservingContext(ActionListener.wrap(ignore -> {
                    rolesResult.setMissingRoles(roleNames);
                    listener.onResponse(rolesResult);
                }, listener::onFailure), threadContext);

        final Predicate<RoleRetrievalResult> iterationPredicate = result -> roleNames.isEmpty() == false;
        new IteratingActionListener<>(descriptorsListener, (rolesProvider, providerListener) -> {
            // try to resolve descriptors with role provider
            rolesProvider.accept(roleNames, ActionListener.wrap(result -> {
                if (result.isSuccess()) {
                    logger.debug(() -> new ParameterizedMessage("Roles [{}] were resolved by [{}]",
                        names(result.getDescriptors()), rolesProvider));
                    final Set<RoleDescriptor> resolvedDescriptors = result.getDescriptors();
                    rolesResult.addDescriptors(resolvedDescriptors);
                    // remove resolved descriptors from the set of roles still needed to be resolved
                    for (RoleDescriptor descriptor : resolvedDescriptors) {
                        roleNames.remove(descriptor.getName());
                    }
                } else {
                    logger.warn(new ParameterizedMessage("role retrieval failed from [{}]", rolesProvider), result.getFailure());
                    rolesResult.setFailure();
                }
                providerListener.onResponse(result);
            }, providerListener::onFailure));
        }, asyncRoleProviders, threadContext, Function.identity(), iterationPredicate).run();
    }

    private String names(Collection<RoleDescriptor> descriptors) {
        return descriptors.stream().map(RoleDescriptor::getName).collect(Collectors.joining(","));
    }

    public static void buildRoleFromDescriptors(Collection<RoleDescriptor> roleDescriptors, FieldPermissionsCache fieldPermissionsCache,
                                                NativePrivilegeStore privilegeStore, ActionListener<Role> listener) {
        if (roleDescriptors.isEmpty()) {
            listener.onResponse(Role.EMPTY);
            return;
        }

        Set<String> clusterPrivileges = new HashSet<>();
        final List<ConditionalClusterPrivilege> conditionalClusterPrivileges = new ArrayList<>();
        Set<String> runAs = new HashSet<>();
        Map<Set<String>, MergeableIndicesPrivilege> indicesPrivilegesMap = new HashMap<>();

        // Keyed by application + resource
        Map<Tuple<String, Set<String>>, Set<String>> applicationPrivilegesMap = new HashMap<>();

        List<String> roleNames = new ArrayList<>(roleDescriptors.size());
        for (RoleDescriptor descriptor : roleDescriptors) {
            roleNames.add(descriptor.getName());
            if (descriptor.getClusterPrivileges() != null) {
                clusterPrivileges.addAll(Arrays.asList(descriptor.getClusterPrivileges()));
            }
            if (descriptor.getConditionalClusterPrivileges() != null) {
                conditionalClusterPrivileges.addAll(Arrays.asList(descriptor.getConditionalClusterPrivileges()));
            }
            if (descriptor.getRunAs() != null) {
                runAs.addAll(Arrays.asList(descriptor.getRunAs()));
            }
            IndicesPrivileges[] indicesPrivileges = descriptor.getIndicesPrivileges();
            for (IndicesPrivileges indicesPrivilege : indicesPrivileges) {
                Set<String> key = newHashSet(indicesPrivilege.getIndices());
                // if a index privilege is an explicit denial, then we treat it as non-existent since we skipped these in the past when
                // merging
                final boolean isExplicitDenial =
                        indicesPrivileges.length == 1 && "none".equalsIgnoreCase(indicesPrivilege.getPrivileges()[0]);
                if (isExplicitDenial == false) {
                    indicesPrivilegesMap.compute(key, (k, value) -> {
                        if (value == null) {
                            return new MergeableIndicesPrivilege(indicesPrivilege.getIndices(), indicesPrivilege.getPrivileges(),
                                    indicesPrivilege.getGrantedFields(), indicesPrivilege.getDeniedFields(), indicesPrivilege.getQuery());
                        } else {
                            value.merge(new MergeableIndicesPrivilege(indicesPrivilege.getIndices(), indicesPrivilege.getPrivileges(),
                                    indicesPrivilege.getGrantedFields(), indicesPrivilege.getDeniedFields(), indicesPrivilege.getQuery()));
                            return value;
                        }
                    });
                }
            }
            for (RoleDescriptor.ApplicationResourcePrivileges appPrivilege : descriptor.getApplicationPrivileges()) {
                Tuple<String, Set<String>> key = new Tuple<>(appPrivilege.getApplication(), newHashSet(appPrivilege.getResources()));
                applicationPrivilegesMap.compute(key, (k, v) -> {
                    if (v == null) {
                        return newHashSet(appPrivilege.getPrivileges());
                    } else {
                        v.addAll(Arrays.asList(appPrivilege.getPrivileges()));
                        return v;
                    }
                });
            }
        }

        final Privilege runAsPrivilege = runAs.isEmpty() ? Privilege.NONE : new Privilege(runAs, runAs.toArray(Strings.EMPTY_ARRAY));
        final Role.Builder builder = Role.builder(roleNames.toArray(new String[roleNames.size()]))
                .cluster(clusterPrivileges, conditionalClusterPrivileges)
                .runAs(runAsPrivilege);
        indicesPrivilegesMap.entrySet().forEach((entry) -> {
            MergeableIndicesPrivilege privilege = entry.getValue();
            builder.add(fieldPermissionsCache.getFieldPermissions(privilege.fieldPermissionsDefinition), privilege.query,
                    IndexPrivilege.get(privilege.privileges), privilege.indices.toArray(Strings.EMPTY_ARRAY));
        });

        if (applicationPrivilegesMap.isEmpty()) {
            listener.onResponse(builder.build());
        } else {
            final Set<String> applicationNames = applicationPrivilegesMap.keySet().stream()
                    .map(Tuple::v1)
                    .collect(Collectors.toSet());
            final Set<String> applicationPrivilegeNames = applicationPrivilegesMap.values().stream()
                    .flatMap(Collection::stream)
                    .collect(Collectors.toSet());
            privilegeStore.getPrivileges(applicationNames, applicationPrivilegeNames, ActionListener.wrap(appPrivileges -> {
                applicationPrivilegesMap.forEach((key, names) ->
                        builder.addApplicationPrivilege(ApplicationPrivilege.get(key.v1(), names, appPrivileges), key.v2()));
                listener.onResponse(builder.build());
            }, listener::onFailure));
        }
    }

    public void invalidateAll() {
        numInvalidation.incrementAndGet();
        negativeLookupCache.invalidateAll();
        try (ReleasableLock ignored = readLock.acquire()) {
            roleCache.invalidateAll();
        }
    }

    public void invalidate(String role) {
        numInvalidation.incrementAndGet();

        // the cache cannot be modified while doing this operation per the terms of the cache iterator
        try (ReleasableLock ignored = writeLock.acquire()) {
            Iterator<Set<String>> keyIter = roleCache.keys().iterator();
            while (keyIter.hasNext()) {
                Set<String> key = keyIter.next();
                if (key.contains(role)) {
                    keyIter.remove();
                }
            }
        }
        negativeLookupCache.invalidate(role);
    }

    public void invalidate(Set<String> roles) {
        numInvalidation.incrementAndGet();

        // the cache cannot be modified while doing this operation per the terms of the cache iterator
        try (ReleasableLock ignored = writeLock.acquire()) {
            Iterator<Set<String>> keyIter = roleCache.keys().iterator();
            while (keyIter.hasNext()) {
                Set<String> key = keyIter.next();
                if (Sets.haveEmptyIntersection(key, roles) == false) {
                    keyIter.remove();
                }
            }
        }

        roles.forEach(negativeLookupCache::invalidate);
    }

    public void usageStats(ActionListener<Map<String, Object>> listener) {
        final Map<String, Object> usage = new HashMap<>(2);
        usage.put("file", fileRolesStore.usageStats());
        nativeRolesStore.usageStats(ActionListener.wrap(map -> {
            usage.put("native", map);
            listener.onResponse(usage);
        }, listener::onFailure));
    }

    public void onSecurityIndexStateChange(SecurityIndexManager.State previousState, SecurityIndexManager.State currentState) {
        if (isMoveFromRedToNonRed(previousState, currentState) || isIndexDeleted(previousState, currentState) ||
            previousState.isIndexUpToDate != currentState.isIndexUpToDate) {
            invalidateAll();
        }
    }

    // pkg - private for testing
    boolean isValueInNegativeLookupCache(String key) {
        return negativeLookupCache.get(key) != null;
    }

    /**
     * A mutable class that can be used to represent the combination of one or more {@link IndicesPrivileges}
     */
    private static class MergeableIndicesPrivilege {
        private Set<String> indices;
        private Set<String> privileges;
        private FieldPermissionsDefinition fieldPermissionsDefinition;
        private Set<BytesReference> query = null;

        MergeableIndicesPrivilege(String[] indices, String[] privileges, @Nullable String[] grantedFields, @Nullable String[] deniedFields,
                                  @Nullable BytesReference query) {
            this.indices = newHashSet(Objects.requireNonNull(indices));
            this.privileges = newHashSet(Objects.requireNonNull(privileges));
            this.fieldPermissionsDefinition = new FieldPermissionsDefinition(grantedFields, deniedFields);
            if (query != null) {
                this.query = newHashSet(query);
            }
        }

        void merge(MergeableIndicesPrivilege other) {
            assert indices.equals(other.indices) : "index names must be equivalent in order to merge";
            Set<FieldGrantExcludeGroup> groups = new HashSet<>();
            groups.addAll(this.fieldPermissionsDefinition.getFieldGrantExcludeGroups());
            groups.addAll(other.fieldPermissionsDefinition.getFieldGrantExcludeGroups());
            this.fieldPermissionsDefinition = new FieldPermissionsDefinition(groups);
            this.privileges.addAll(other.privileges);

            if (this.query == null || other.query == null) {
                this.query = null;
            } else {
                this.query.addAll(other.query);
            }
        }
    }

    private static final class RolesRetrievalResult {

        private final Set<RoleDescriptor> roleDescriptors = new HashSet<>();
        private Set<String> missingRoles = Collections.emptySet();
        private boolean success = true;

        private void addDescriptors(Set<RoleDescriptor> descriptors) {
            roleDescriptors.addAll(descriptors);
        }

        private Set<RoleDescriptor> getRoleDescriptors() {
            return roleDescriptors;
        }

        private void setFailure() {
            success = false;
        }

        private boolean isSuccess() {
            return success;
        }

        private void setMissingRoles(Set<String> missingRoles) {
            this.missingRoles = missingRoles;
        }

        private Set<String> getMissingRoles() {
            return missingRoles;
        }
    }

    public static List<Setting<?>> getSettings() {
        return Arrays.asList(CACHE_SIZE_SETTING, NEGATIVE_LOOKUP_CACHE_SIZE_SETTING);
    }
}
