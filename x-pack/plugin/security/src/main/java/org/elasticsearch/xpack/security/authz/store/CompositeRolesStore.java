/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
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
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.xpack.core.security.SecurityField.setting;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isIndexDeleted;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isMoveFromRedToNonRed;

/**
 * A composite roles store that combines built in roles, file-based roles, and index-based roles. Checks the built in roles first, then the
 * file roles, and finally the index roles.
 */
public class CompositeRolesStore extends AbstractComponent {

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

    public static final Setting<Integer> CACHE_SIZE_SETTING =
            Setting.intSetting(setting("authz.store.roles.cache.max_size"), 10000, Property.NodeScope);

    private final FileRolesStore fileRolesStore;
    private final NativeRolesStore nativeRolesStore;
    private final ReservedRolesStore reservedRolesStore;
    private final NativePrivilegeStore privilegeStore;
    private final XPackLicenseState licenseState;
    private final Cache<Set<String>, Role> roleCache;
    private final Set<String> negativeLookupCache;
    private final ThreadContext threadContext;
    private final AtomicLong numInvalidation = new AtomicLong();
    private final List<BiConsumer<Set<String>, ActionListener<Set<RoleDescriptor>>>> customRolesProviders;

    public CompositeRolesStore(Settings settings, FileRolesStore fileRolesStore, NativeRolesStore nativeRolesStore,
                               ReservedRolesStore reservedRolesStore, NativePrivilegeStore privilegeStore,
                               List<BiConsumer<Set<String>, ActionListener<Set<RoleDescriptor>>>> rolesProviders,
                               ThreadContext threadContext, XPackLicenseState licenseState) {
        super(settings);
        this.fileRolesStore = fileRolesStore;
        fileRolesStore.addListener(this::invalidate);
        this.nativeRolesStore = nativeRolesStore;
        this.reservedRolesStore = reservedRolesStore;
        this.privilegeStore = privilegeStore;
        this.licenseState = licenseState;
        CacheBuilder<Set<String>, Role> builder = CacheBuilder.builder();
        final int cacheSize = CACHE_SIZE_SETTING.get(settings);
        if (cacheSize >= 0) {
            builder.setMaximumWeight(cacheSize);
        }
        this.roleCache = builder.build();
        this.threadContext = threadContext;
        this.negativeLookupCache = ConcurrentCollections.newConcurrentSet();
        this.customRolesProviders = Collections.unmodifiableList(rolesProviders);
    }

    public void roles(Set<String> roleNames, FieldPermissionsCache fieldPermissionsCache, ActionListener<Role> roleActionListener) {
        Role existing = roleCache.get(roleNames);
        if (existing != null) {
            roleActionListener.onResponse(existing);
        } else {
            final long invalidationCounter = numInvalidation.get();
            roleDescriptors(roleNames, ActionListener.wrap(
                    descriptors -> {
                        final Set<RoleDescriptor> effectiveDescriptors;
                        if (licenseState.isDocumentAndFieldLevelSecurityAllowed()) {
                            effectiveDescriptors = descriptors;
                        } else {
                            effectiveDescriptors = descriptors.stream()
                                    .filter((rd) -> rd.isUsingDocumentOrFieldLevelSecurity() == false)
                                    .collect(Collectors.toSet());
                        }
                        logger.trace("Building role from descriptors [{}] for names [{}]", effectiveDescriptors, roleNames);
                        buildRoleFromDescriptors(effectiveDescriptors, fieldPermissionsCache, privilegeStore, ActionListener.wrap(role -> {
                            if (role != null) {
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
                            }
                            roleActionListener.onResponse(role);
                        }, roleActionListener::onFailure));
                    },
                    roleActionListener::onFailure));
        }
    }

    private void roleDescriptors(Set<String> roleNames, ActionListener<Set<RoleDescriptor>> roleDescriptorActionListener) {
        final Set<String> filteredRoleNames = roleNames.stream().filter((s) -> {
            if (negativeLookupCache.contains(s)) {
                logger.debug("Requested role [{}] does not exist (cached)", s);
                return false;
            } else {
                return true;
            }
        }).collect(Collectors.toSet());
        final Set<RoleDescriptor> builtInRoleDescriptors = getBuiltInRoleDescriptors(filteredRoleNames);
        Set<String> remainingRoleNames = difference(filteredRoleNames, builtInRoleDescriptors);
        if (remainingRoleNames.isEmpty()) {
            roleDescriptorActionListener.onResponse(Collections.unmodifiableSet(builtInRoleDescriptors));
        } else {
            nativeRolesStore.getRoleDescriptors(remainingRoleNames.toArray(Strings.EMPTY_ARRAY), ActionListener.wrap((descriptors) -> {
                logger.debug(() -> new ParameterizedMessage("Roles [{}] were resolved from the native index store", names(descriptors)));
                builtInRoleDescriptors.addAll(descriptors);
                callCustomRoleProvidersIfEnabled(builtInRoleDescriptors, filteredRoleNames, roleDescriptorActionListener);
            }, e -> {
                logger.warn("role retrieval failed from the native roles store", e);
                callCustomRoleProvidersIfEnabled(builtInRoleDescriptors, filteredRoleNames, roleDescriptorActionListener);
            }));
        }
    }

    private void callCustomRoleProvidersIfEnabled(Set<RoleDescriptor> builtInRoleDescriptors, Set<String> filteredRoleNames,
                                                  ActionListener<Set<RoleDescriptor>> roleDescriptorActionListener) {
        if (builtInRoleDescriptors.size() != filteredRoleNames.size()) {
            final Set<String> missing = difference(filteredRoleNames, builtInRoleDescriptors);
            assert missing.isEmpty() == false : "the missing set should not be empty if the sizes didn't match";
            if (licenseState.isCustomRoleProvidersAllowed() && !customRolesProviders.isEmpty()) {
                new IteratingActionListener<>(roleDescriptorActionListener, (rolesProvider, listener) -> {
                    // resolve descriptors with role provider
                    rolesProvider.accept(missing, ActionListener.wrap((resolvedDescriptors) -> {
                        logger.debug(() ->
                                new ParameterizedMessage("Roles [{}] were resolved by [{}]", names(resolvedDescriptors), rolesProvider));
                        builtInRoleDescriptors.addAll(resolvedDescriptors);
                        // remove resolved descriptors from the set of roles still needed to be resolved
                        for (RoleDescriptor descriptor : resolvedDescriptors) {
                            missing.remove(descriptor.getName());
                        }
                        if (missing.isEmpty()) {
                            // no more roles to resolve, send the response
                            listener.onResponse(Collections.unmodifiableSet(builtInRoleDescriptors));
                        } else {
                            // still have roles to resolve, keep trying with the next roles provider
                            listener.onResponse(null);
                        }
                    }, listener::onFailure));
                }, customRolesProviders, threadContext, () -> {
                    negativeLookupCache.addAll(missing);
                    return builtInRoleDescriptors;
                }).run();
            } else {
                logger.debug(() ->
                        new ParameterizedMessage("Requested roles [{}] do not exist", Strings.collectionToCommaDelimitedString(missing)));
                negativeLookupCache.addAll(missing);
                roleDescriptorActionListener.onResponse(Collections.unmodifiableSet(builtInRoleDescriptors));
            }
        } else {
            roleDescriptorActionListener.onResponse(Collections.unmodifiableSet(builtInRoleDescriptors));
        }
    }

    private Set<RoleDescriptor> getBuiltInRoleDescriptors(Set<String> roleNames) {
        final Set<RoleDescriptor> descriptors = reservedRolesStore.roleDescriptors().stream()
                .filter((rd) -> roleNames.contains(rd.getName()))
                .collect(Collectors.toCollection(HashSet::new));
        if (descriptors.size() > 0) {
            logger.debug(() -> new ParameterizedMessage("Roles [{}] are builtin roles", names(descriptors)));
        }
        final Set<String> difference = difference(roleNames, descriptors);
        if (difference.isEmpty() == false) {
            final Set<RoleDescriptor> fileRoles = fileRolesStore.roleDescriptors(difference);
            logger.debug(() ->
                    new ParameterizedMessage("Roles [{}] were resolved from [{}]", names(fileRoles), fileRolesStore.getFile()));
            descriptors.addAll(fileRoles);
        }

        return descriptors;
    }

    private String names(Collection<RoleDescriptor> descriptors) {
        return descriptors.stream().map(RoleDescriptor::getName).collect(Collectors.joining(","));
    }

    private Set<String> difference(Set<String> roleNames, Set<RoleDescriptor> descriptors) {
        Set<String> foundNames = descriptors.stream().map(RoleDescriptor::getName).collect(Collectors.toSet());
        return Sets.difference(roleNames, foundNames);
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
        negativeLookupCache.clear();
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
        negativeLookupCache.remove(role);
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

        negativeLookupCache.removeAll(roles);
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
}
