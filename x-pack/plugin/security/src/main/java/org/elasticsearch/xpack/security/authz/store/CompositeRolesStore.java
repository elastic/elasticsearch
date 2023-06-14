/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.RestrictedIndices;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.RemoteIndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.DocumentSubsetBitsetCache;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition.FieldGrantExcludeGroup;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.Privilege;
import org.elasticsearch.xpack.core.security.authz.restriction.Workflow;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.authz.store.RoleKey;
import org.elasticsearch.xpack.core.security.authz.store.RoleReference;
import org.elasticsearch.xpack.core.security.authz.store.RoleReferenceIntersection;
import org.elasticsearch.xpack.core.security.authz.store.RolesRetrievalResult;
import org.elasticsearch.xpack.core.security.support.CacheIteratorHelper;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.InternalUser;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.authz.restriction.WorkflowService;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isIndexDeleted;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isMoveFromRedToNonRed;

/**
 * A composite roles store that can retrieve roles from multiple sources.
 * @see RoleProviders
 */
public class CompositeRolesStore {

    static final Setting<Integer> NEGATIVE_LOOKUP_CACHE_SIZE_SETTING = Setting.intSetting(
        "xpack.security.authz.store.roles.negative_lookup_cache.max_size",
        10000,
        Property.NodeScope
    );
    private static final Setting<Integer> CACHE_SIZE_SETTING = Setting.intSetting(
        "xpack.security.authz.store.roles.cache.max_size",
        10000,
        Property.NodeScope
    );
    private static final Logger logger = LogManager.getLogger(CompositeRolesStore.class);

    private final RoleProviders roleProviders;
    private final NativePrivilegeStore privilegeStore;
    private final FieldPermissionsCache fieldPermissionsCache;
    private final Cache<RoleKey, Role> roleCache;
    private final CacheIteratorHelper<RoleKey, Role> roleCacheHelper;
    private final Cache<String, Boolean> negativeLookupCache;
    private final DocumentSubsetBitsetCache dlsBitsetCache;
    private final AnonymousUser anonymousUser;
    private final AtomicLong numInvalidation = new AtomicLong();
    private final RoleDescriptorStore roleReferenceResolver;
    private final Role superuserRole;
    private final Map<String, Role> internalUserRoles;
    private final RestrictedIndices restrictedIndices;
    private final WorkflowService workflowService;
    private final ThreadContext threadContext;

    public CompositeRolesStore(
        Settings settings,
        RoleProviders roleProviders,
        NativePrivilegeStore privilegeStore,
        ThreadContext threadContext,
        XPackLicenseState licenseState,
        FieldPermissionsCache fieldPermissionsCache,
        ApiKeyService apiKeyService,
        ServiceAccountService serviceAccountService,
        DocumentSubsetBitsetCache dlsBitsetCache,
        RestrictedIndices restrictedIndices,
        Consumer<Collection<RoleDescriptor>> effectiveRoleDescriptorsConsumer,
        WorkflowService workflowService
    ) {
        this.roleProviders = roleProviders;
        roleProviders.addChangeListener(new RoleProviders.ChangeListener() {
            @Override
            public void rolesChanged(Set<String> roles) {
                CompositeRolesStore.this.invalidate(roles);
            }

            @Override
            public void providersChanged() {
                CompositeRolesStore.this.invalidateAll();
            }
        });

        this.privilegeStore = Objects.requireNonNull(privilegeStore);
        this.dlsBitsetCache = Objects.requireNonNull(dlsBitsetCache);
        this.fieldPermissionsCache = Objects.requireNonNull(fieldPermissionsCache);
        CacheBuilder<RoleKey, Role> builder = CacheBuilder.builder();
        final int cacheSize = CACHE_SIZE_SETTING.get(settings);
        if (cacheSize >= 0) {
            builder.setMaximumWeight(cacheSize);
        }
        this.roleCache = builder.build();
        this.roleCacheHelper = new CacheIteratorHelper<>(roleCache);
        CacheBuilder<String, Boolean> nlcBuilder = CacheBuilder.builder();
        final int nlcCacheSize = NEGATIVE_LOOKUP_CACHE_SIZE_SETTING.get(settings);
        if (nlcCacheSize >= 0) {
            nlcBuilder.setMaximumWeight(nlcCacheSize);
        }
        this.negativeLookupCache = nlcBuilder.build();
        this.restrictedIndices = restrictedIndices;
        this.superuserRole = Role.buildFromRoleDescriptor(
            ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR,
            fieldPermissionsCache,
            this.restrictedIndices
        );
        this.internalUserRoles = InternalUsers.get()
            .stream()
            .filter(u -> u.getLocalClusterRoleDescriptor().isPresent())
            .collect(
                Collectors.toMap(
                    u -> u.principal(),
                    u -> Role.buildFromRoleDescriptor(
                        u.getLocalClusterRoleDescriptor().get(),
                        fieldPermissionsCache,
                        this.restrictedIndices
                    )
                )
            );
        this.roleReferenceResolver = new RoleDescriptorStore(
            roleProviders,
            apiKeyService,
            serviceAccountService,
            negativeLookupCache,
            licenseState,
            threadContext,
            effectiveRoleDescriptorsConsumer
        );
        this.anonymousUser = new AnonymousUser(settings);
        this.workflowService = workflowService;
        this.threadContext = threadContext;
    }

    public void getRoles(Authentication authentication, ActionListener<Tuple<Role, Role>> roleActionListener) {
        getRole(authentication.getEffectiveSubject(), ActionListener.wrap(role -> {
            if (authentication.isRunAs()) {
                getRole(
                    authentication.getAuthenticatingSubject(),
                    ActionListener.wrap(
                        authenticatingRole -> roleActionListener.onResponse(new Tuple<>(role, authenticatingRole)),
                        roleActionListener::onFailure
                    )
                );
            } else {
                roleActionListener.onResponse(new Tuple<>(role, role));
            }
        }, roleActionListener::onFailure));
    }

    public void getRole(Subject subject, ActionListener<Role> roleActionListener) {
        final Role internalUserRole = tryGetRoleForInternalUser(subject);
        if (internalUserRole != null) {
            roleActionListener.onResponse(internalUserRole);
            return;
        }

        assert false == subject.getUser() instanceof InternalUser : "Internal user [" + subject.getUser() + "] should not pass here";

        final RoleReferenceIntersection roleReferenceIntersection = subject.getRoleReferenceIntersection(anonymousUser);
        roleReferenceIntersection.buildRole(this::buildRoleFromRoleReference, roleActionListener);
    }

    // Accessible by tests
    Role tryGetRoleForInternalUser(Subject subject) {
        // we need to special case the internal users in this method, if we apply the anonymous roles to every user including these system
        // user accounts then we run into the chance of a deadlock because then we need to get a role that we may be trying to get as the
        // internal user.
        // The SystemUser is special cased as it has special privileges to execute internal actions and should never be passed into this
        // method.
        // The other internal users have directly assigned roles that are handled with special cases here
        final User user = subject.getUser();
        if (user instanceof InternalUser internal) {
            return getInternalUserRole(internal);
        }
        return null;
    }

    // Accessible for testing
    protected Role getInternalUserRole(InternalUser user) {
        String name = user.principal();
        final Role role = this.internalUserRoles.get(name);
        if (role == null) {
            throw new IllegalArgumentException("the internal user [" + name + "] should never have its roles resolved");
        }
        return role;
    }

    public void buildRoleFromRoleReference(RoleReference roleReference, ActionListener<Role> roleActionListener) {
        final RoleKey roleKey = roleReference.id();
        if (roleKey == RoleKey.ROLE_KEY_SUPERUSER) {
            roleActionListener.onResponse(superuserRole);
            return;
        }
        if (roleKey == RoleKey.ROLE_KEY_EMPTY) {
            roleActionListener.onResponse(Role.EMPTY);
            return;
        }

        final Role existing = roleCache.get(roleKey);
        if (existing == null) {
            final long invalidationCounter = numInvalidation.get();
            final Consumer<Exception> failureHandler = e -> {
                // Because superuser does not have write access to restricted indices, it is valid to mix superuser with other roles to
                // gain addition access. However, if retrieving those roles fails for some reason, then that could leave admins in a
                // situation where they are unable to administer their cluster (in order to resolve the problem that is leading to failures
                // in role retrieval). So if a role reference includes superuser, but role retrieval failed, we fallback to the static
                // superuser role.
                if (includesSuperuserRole(roleReference)) {
                    logger.warn(
                        () -> format(
                            "there was a failure resolving the roles [%s], falling back to the [%s] role instead",
                            roleReference.id(),
                            Strings.arrayToCommaDelimitedString(superuserRole.names())
                        ),
                        e
                    );
                    roleActionListener.onResponse(superuserRole);
                } else {
                    roleActionListener.onFailure(e);
                }
            };
            roleReference.resolve(roleReferenceResolver, ActionListener.wrap(rolesRetrievalResult -> {
                if (RolesRetrievalResult.EMPTY == rolesRetrievalResult) {
                    roleActionListener.onResponse(Role.EMPTY);
                } else if (RolesRetrievalResult.SUPERUSER == rolesRetrievalResult) {
                    roleActionListener.onResponse(superuserRole);
                } else {
                    buildThenMaybeCacheRole(
                        roleKey,
                        rolesRetrievalResult.getRoleDescriptors(),
                        rolesRetrievalResult.getMissingRoles(),
                        rolesRetrievalResult.isSuccess(),
                        invalidationCounter,
                        ActionListener.wrap(roleActionListener::onResponse, failureHandler)
                    );
                }
            }, failureHandler));
        } else {
            Workflow workflow = workflowService.readWorkflowFromThreadContext(threadContext);
            roleActionListener.onResponse(existing.forWorkflow(workflow));
        }
    }

    private static boolean includesSuperuserRole(RoleReference roleReference) {
        if (roleReference instanceof RoleReference.NamedRoleReference namedRoles) {
            return Arrays.asList(namedRoles.getRoleNames()).contains(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName());
        } else {
            return false;
        }
    }

    // package private for testing
    RoleDescriptorStore getRoleReferenceResolver() {
        return roleReferenceResolver;
    }

    private void buildThenMaybeCacheRole(
        RoleKey roleKey,
        Collection<RoleDescriptor> roleDescriptors,
        Set<String> missing,
        boolean tryCache,
        long invalidationCounter,
        ActionListener<Role> listener
    ) {
        logger.trace(
            "Building role from descriptors [{}] for names [{}] from source [{}]",
            roleDescriptors,
            roleKey.getNames(),
            roleKey.getSource()
        );
        buildRoleFromDescriptors(roleDescriptors, fieldPermissionsCache, privilegeStore, restrictedIndices, ActionListener.wrap(role -> {
            if (role != null && tryCache) {
                try (ReleasableLock ignored = roleCacheHelper.acquireUpdateLock()) {
                    /* this is kinda spooky. We use a read/write lock to ensure we don't modify the cache if we hold
                     * the write lock (fetching stats for instance - which is kinda overkill?) but since we fetching
                     * stuff in an async fashion we need to make sure that if the cache got invalidated since we
                     * started the request we don't put a potential stale result in the cache, hence the
                     * numInvalidation.get() comparison to the number of invalidation when we started. we just try to
                     * be on the safe side and don't cache potentially stale results
                     */
                    if (invalidationCounter == numInvalidation.get()) {
                        roleCache.computeIfAbsent(roleKey, (s) -> role);
                    }
                }

                for (String missingRole : missing) {
                    negativeLookupCache.computeIfAbsent(missingRole, s -> Boolean.TRUE);
                }
            }
            Workflow workflow = workflowService.readWorkflowFromThreadContext(threadContext);
            listener.onResponse(role.forWorkflow(workflow));
        }, listener::onFailure));
    }

    public void getRoleDescriptorsList(Subject subject, ActionListener<Collection<Set<RoleDescriptor>>> listener) {
        tryGetRoleDescriptorForInternalUser(subject).ifPresentOrElse(
            roleDescriptor -> listener.onResponse(List.of(Set.of(roleDescriptor))),
            () -> {
                final List<RoleReference> roleReferences = subject.getRoleReferenceIntersection(anonymousUser).getRoleReferences();
                final GroupedActionListener<Set<RoleDescriptor>> groupedActionListener = new GroupedActionListener<>(
                    roleReferences.size(),
                    listener
                );

                roleReferences.forEach(roleReference -> {
                    roleReference.resolve(roleReferenceResolver, ActionListener.wrap(rolesRetrievalResult -> {
                        if (rolesRetrievalResult.isSuccess()) {
                            groupedActionListener.onResponse(rolesRetrievalResult.getRoleDescriptors());
                        } else {
                            groupedActionListener.onFailure(new ElasticsearchException("role retrieval had one or more failures"));
                        }
                    }, groupedActionListener::onFailure));
                });
            }
        );
    }

    // Package private for testing
    static Optional<RoleDescriptor> tryGetRoleDescriptorForInternalUser(Subject subject) {
        if (subject.getUser() instanceof InternalUser internalUser) {
            final Optional<RoleDescriptor> roleDescriptor = internalUser.getLocalClusterRoleDescriptor();
            if (roleDescriptor.isEmpty()) {
                throw new IllegalArgumentException(
                    "should never try to get the roles for internal user [" + internalUser.principal() + "]"
                );
            }
            return roleDescriptor;
        } else {
            return Optional.empty();
        }
    }

    public static void buildRoleFromDescriptors(
        Collection<RoleDescriptor> roleDescriptors,
        FieldPermissionsCache fieldPermissionsCache,
        NativePrivilegeStore privilegeStore,
        RestrictedIndices restrictedIndices,
        ActionListener<Role> listener
    ) {
        if (roleDescriptors.isEmpty()) {
            listener.onResponse(Role.EMPTY);
            return;
        }

        final Set<String> clusterPrivileges = new HashSet<>();
        final List<ConfigurableClusterPrivilege> configurableClusterPrivileges = new ArrayList<>();
        final Set<String> runAs = new HashSet<>();

        final Map<Set<String>, MergeableIndicesPrivilege> indicesPrivilegesMap = new HashMap<>();
        final Map<Set<String>, MergeableIndicesPrivilege> restrictedIndicesPrivilegesMap = new HashMap<>();

        final Map<Set<String>, Set<IndicesPrivileges>> remoteIndicesPrivilegesByCluster = new HashMap<>();

        // Keyed by application + resource
        final Map<Tuple<String, Set<String>>, Set<String>> applicationPrivilegesMap = new HashMap<>();
        final Set<String> workflows = new HashSet<>();
        final List<String> roleNames = new ArrayList<>(roleDescriptors.size());
        for (RoleDescriptor descriptor : roleDescriptors) {
            roleNames.add(descriptor.getName());
            if (descriptor.getClusterPrivileges() != null) {
                clusterPrivileges.addAll(Arrays.asList(descriptor.getClusterPrivileges()));
            }
            if (descriptor.getConditionalClusterPrivileges() != null) {
                configurableClusterPrivileges.addAll(Arrays.asList(descriptor.getConditionalClusterPrivileges()));
            }
            if (descriptor.getRunAs() != null) {
                runAs.addAll(Arrays.asList(descriptor.getRunAs()));
            }

            MergeableIndicesPrivilege.collatePrivilegesByIndices(descriptor.getIndicesPrivileges(), true, restrictedIndicesPrivilegesMap);
            MergeableIndicesPrivilege.collatePrivilegesByIndices(descriptor.getIndicesPrivileges(), false, indicesPrivilegesMap);

            if (descriptor.hasRemoteIndicesPrivileges()) {
                groupIndexPrivilegesByCluster(descriptor.getRemoteIndicesPrivileges(), remoteIndicesPrivilegesByCluster);
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

            if (descriptor.hasWorkflowsRestriction()) {
                assert roleDescriptors.size() == 1 : "only single role descriptor with workflow restriction is allowed";
                workflows.addAll(List.of(descriptor.getRestriction().getWorkflows()));
            }
        }

        final Privilege runAsPrivilege = runAs.isEmpty() ? Privilege.NONE : new Privilege(runAs, runAs.toArray(Strings.EMPTY_ARRAY));
        final Role.Builder builder = Role.builder(restrictedIndices, roleNames.toArray(Strings.EMPTY_ARRAY))
            .cluster(clusterPrivileges, configurableClusterPrivileges)
            .workflows(workflows)
            .runAs(runAsPrivilege);
        indicesPrivilegesMap.forEach(
            (key, privilege) -> builder.add(
                fieldPermissionsCache.getFieldPermissions(privilege.fieldPermissionsDefinition),
                privilege.query,
                IndexPrivilege.get(privilege.privileges),
                false,
                privilege.indices.toArray(Strings.EMPTY_ARRAY)
            )
        );
        restrictedIndicesPrivilegesMap.forEach(
            (key, privilege) -> builder.add(
                fieldPermissionsCache.getFieldPermissions(privilege.fieldPermissionsDefinition),
                privilege.query,
                IndexPrivilege.get(privilege.privileges),
                true,
                privilege.indices.toArray(Strings.EMPTY_ARRAY)
            )
        );

        remoteIndicesPrivilegesByCluster.forEach((clusterAliasKey, remoteIndicesPrivilegesForCluster) -> {
            remoteIndicesPrivilegesForCluster.forEach(
                (privilege) -> builder.addRemoteGroup(
                    clusterAliasKey,
                    fieldPermissionsCache.getFieldPermissions(
                        new FieldPermissionsDefinition(privilege.getGrantedFields(), privilege.getDeniedFields())
                    ),
                    privilege.getQuery() == null ? null : newHashSet(privilege.getQuery()),
                    IndexPrivilege.get(newHashSet(Objects.requireNonNull(privilege.getPrivileges()))),
                    privilege.allowRestrictedIndices(),
                    newHashSet(Objects.requireNonNull(privilege.getIndices())).toArray(new String[0])
                )
            );
        });
        if (applicationPrivilegesMap.isEmpty()) {
            listener.onResponse(builder.build());
        } else {
            final Set<String> applicationNames = applicationPrivilegesMap.keySet().stream().map(Tuple::v1).collect(Collectors.toSet());
            final Set<String> applicationPrivilegeNames = applicationPrivilegesMap.values()
                .stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
            privilegeStore.getPrivileges(applicationNames, applicationPrivilegeNames, ActionListener.wrap(appPrivileges -> {
                applicationPrivilegesMap.forEach(
                    (key, names) -> ApplicationPrivilege.get(key.v1(), names, appPrivileges)
                        .forEach(priv -> builder.addApplicationPrivilege(priv, key.v2()))
                );
                listener.onResponse(builder.build());
            }, listener::onFailure));
        }
    }

    public void invalidateAll() {
        numInvalidation.incrementAndGet();
        negativeLookupCache.invalidateAll();
        try (ReleasableLock ignored = roleCacheHelper.acquireUpdateLock()) {
            roleCache.invalidateAll();
        }
        dlsBitsetCache.clear("role store invalidation");
    }

    public void invalidate(String role) {
        numInvalidation.incrementAndGet();

        roleCacheHelper.removeKeysIf(key -> key.getNames().contains(role));
        negativeLookupCache.invalidate(role);
    }

    public void invalidate(Set<String> roles) {
        numInvalidation.incrementAndGet();
        roleCacheHelper.removeKeysIf(key -> Sets.haveEmptyIntersection(key.getNames(), roles) == false);
        roles.forEach(negativeLookupCache::invalidate);
    }

    public void usageStats(ActionListener<Map<String, Object>> listener) {
        final Map<String, Object> usage = new HashMap<>();
        usage.put("dls", Map.of("bit_set_cache", dlsBitsetCache.usageStats()));
        roleProviders.usageStats(listener.map(roleUsage -> {
            usage.putAll(roleUsage);
            return usage;
        }));
    }

    public void onSecurityIndexStateChange(SecurityIndexManager.State previousState, SecurityIndexManager.State currentState) {
        if (isMoveFromRedToNonRed(previousState, currentState)
            || isIndexDeleted(previousState, currentState)
            || Objects.equals(previousState.indexUUID, currentState.indexUUID) == false
            || previousState.isIndexUpToDate != currentState.isIndexUpToDate) {
            invalidateAll();
        }
    }

    // pkg - private for testing
    boolean isValueInNegativeLookupCache(String key) {
        return negativeLookupCache.get(key) != null;
    }

    private static void groupIndexPrivilegesByCluster(
        final RemoteIndicesPrivileges[] remoteIndicesPrivileges,
        final Map<Set<String>, Set<IndicesPrivileges>> remoteIndexPrivilegesByCluster
    ) {
        assert remoteIndicesPrivileges != null;
        // if a remote index privilege is an explicit denial, then we treat it as non-existent to stay consistent with local index
        // privileges
        final boolean isExplicitDenial = remoteIndicesPrivileges.length == 1
            && "none".equalsIgnoreCase(remoteIndicesPrivileges[0].indicesPrivileges().getPrivileges()[0]);
        if (isExplicitDenial) {
            return;
        }
        for (final RemoteIndicesPrivileges remoteIndicesPrivilege : remoteIndicesPrivileges) {
            final IndicesPrivileges indicesPrivilege = remoteIndicesPrivilege.indicesPrivileges();
            final Set<String> clusterAliasKey = newHashSet(remoteIndicesPrivilege.remoteClusters());
            remoteIndexPrivilegesByCluster.computeIfAbsent(clusterAliasKey, k -> new HashSet<>()).add(indicesPrivilege);
        }
    }

    /**
     * A mutable class that can be used to represent the combination of one or more {@link IndicesPrivileges}
     */
    private static class MergeableIndicesPrivilege {
        private final Set<String> indices;
        private final Set<String> privileges;
        private FieldPermissionsDefinition fieldPermissionsDefinition;
        private Set<BytesReference> query = null;

        MergeableIndicesPrivilege(
            String[] indices,
            String[] privileges,
            @Nullable String[] grantedFields,
            @Nullable String[] deniedFields,
            @Nullable BytesReference query
        ) {
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

        private static void collatePrivilegesByIndices(
            final IndicesPrivileges[] indicesPrivileges,
            final boolean allowsRestrictedIndices,
            final Map<Set<String>, MergeableIndicesPrivilege> indicesPrivilegesMap
        ) {
            // if an index privilege is an explicit denial, then we treat it as non-existent since we skipped these in the past when
            // merging
            final boolean isExplicitDenial = indicesPrivileges.length == 1
                && "none".equalsIgnoreCase(indicesPrivileges[0].getPrivileges()[0]);
            if (isExplicitDenial) {
                return;
            }
            for (final IndicesPrivileges indicesPrivilege : indicesPrivileges) {
                if (indicesPrivilege.allowRestrictedIndices() != allowsRestrictedIndices) {
                    continue;
                }
                final Set<String> key = newHashSet(indicesPrivilege.getIndices());
                indicesPrivilegesMap.compute(key, (k, value) -> {
                    if (value == null) {
                        return new MergeableIndicesPrivilege(
                            indicesPrivilege.getIndices(),
                            indicesPrivilege.getPrivileges(),
                            indicesPrivilege.getGrantedFields(),
                            indicesPrivilege.getDeniedFields(),
                            indicesPrivilege.getQuery()
                        );
                    } else {
                        value.merge(
                            new MergeableIndicesPrivilege(
                                indicesPrivilege.getIndices(),
                                indicesPrivilege.getPrivileges(),
                                indicesPrivilege.getGrantedFields(),
                                indicesPrivilege.getDeniedFields(),
                                indicesPrivilege.getQuery()
                            )
                        );
                        return value;
                    }
                });
            }
        }
    }

    public static List<Setting<?>> getSettings() {
        return Arrays.asList(CACHE_SIZE_SETTING, NEGATIVE_LOOKUP_CACHE_SIZE_SETTING);
    }
}
