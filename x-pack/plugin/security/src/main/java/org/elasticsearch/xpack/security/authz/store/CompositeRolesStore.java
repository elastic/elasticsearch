/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.cache.Cache;
import org.elasticsearch.common.cache.CacheBuilder;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.common.logging.DeprecationCategory;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ReleasableLock;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.license.XPackLicenseState.Feature;
import org.elasticsearch.xpack.core.common.IteratingActionListener;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.DocumentSubsetBitsetCache;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsCache;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissionsDefinition.FieldGrantExcludeGroup;
import org.elasticsearch.xpack.core.security.authz.permission.LimitedRole;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.Privilege;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.authz.store.RoleRetrievalResult;
import org.elasticsearch.xpack.core.security.support.CacheIteratorHelper;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;
import org.elasticsearch.xpack.core.security.user.AnonymousUser;
import org.elasticsearch.xpack.core.security.user.AsyncSearchUser;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.core.security.user.XPackSecurityUser;
import org.elasticsearch.xpack.core.security.user.XPackUser;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.service.ServiceAccountService;
import org.elasticsearch.xpack.security.support.SecurityIndexManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.function.Predicate.not;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.xpack.core.security.authc.Authentication.VERSION_API_KEY_ROLES_AS_BYTES;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isIndexDeleted;
import static org.elasticsearch.xpack.security.support.SecurityIndexManager.isMoveFromRedToNonRed;

/**
 * A composite roles store that combines built in roles, file-based roles, and index-based roles. Checks the built in roles first, then the
 * file roles, and finally the index roles.
 */
public class CompositeRolesStore {

    private static final String ROLES_STORE_SOURCE = "roles_stores";
    private static final Setting<Integer> CACHE_SIZE_SETTING =
        Setting.intSetting("xpack.security.authz.store.roles.cache.max_size", 10000, Property.NodeScope);
    private static final Setting<Integer> NEGATIVE_LOOKUP_CACHE_SIZE_SETTING =
        Setting.intSetting("xpack.security.authz.store.roles.negative_lookup_cache.max_size", 10000, Property.NodeScope);
    private static final Logger logger = LogManager.getLogger(CompositeRolesStore.class);

    private final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(CompositeRolesStore.class);

    private final FileRolesStore fileRolesStore;
    private final NativeRolesStore nativeRolesStore;
    private final NativePrivilegeStore privilegeStore;
    private final XPackLicenseState licenseState;
    private final Consumer<Collection<RoleDescriptor>> effectiveRoleDescriptorsConsumer;
    private final FieldPermissionsCache fieldPermissionsCache;
    private final Cache<RoleKey, Role> roleCache;
    private final CacheIteratorHelper<RoleKey, Role> roleCacheHelper;
    private final Cache<String, Boolean> negativeLookupCache;
    private final DocumentSubsetBitsetCache dlsBitsetCache;
    private final ThreadContext threadContext;
    private final AtomicLong numInvalidation = new AtomicLong();
    private final AnonymousUser anonymousUser;
    private final ApiKeyService apiKeyService;
    private final ServiceAccountService serviceAccountService;
    private final boolean isAnonymousEnabled;
    private final List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>> builtInRoleProviders;
    private final List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>> allRoleProviders;

    public CompositeRolesStore(Settings settings, FileRolesStore fileRolesStore, NativeRolesStore nativeRolesStore,
                               ReservedRolesStore reservedRolesStore, NativePrivilegeStore privilegeStore,
                               List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>> rolesProviders,
                               ThreadContext threadContext, XPackLicenseState licenseState, FieldPermissionsCache fieldPermissionsCache,
                               ApiKeyService apiKeyService, ServiceAccountService serviceAccountService,
                               DocumentSubsetBitsetCache dlsBitsetCache,
                               Consumer<Collection<RoleDescriptor>> effectiveRoleDescriptorsConsumer) {
        this.fileRolesStore = Objects.requireNonNull(fileRolesStore);
        this.dlsBitsetCache = Objects.requireNonNull(dlsBitsetCache);
        fileRolesStore.addListener(this::invalidate);
        this.nativeRolesStore = Objects.requireNonNull(nativeRolesStore);
        this.privilegeStore = Objects.requireNonNull(privilegeStore);
        this.licenseState = Objects.requireNonNull(licenseState);
        this.fieldPermissionsCache = Objects.requireNonNull(fieldPermissionsCache);
        this.apiKeyService = Objects.requireNonNull(apiKeyService);
        this.serviceAccountService = Objects.requireNonNull(serviceAccountService);
        this.effectiveRoleDescriptorsConsumer = Objects.requireNonNull(effectiveRoleDescriptorsConsumer);
        CacheBuilder<RoleKey, Role> builder = CacheBuilder.builder();
        final int cacheSize = CACHE_SIZE_SETTING.get(settings);
        if (cacheSize >= 0) {
            builder.setMaximumWeight(cacheSize);
        }
        this.roleCache = builder.build();
        this.roleCacheHelper = new CacheIteratorHelper<>(roleCache);
        this.threadContext = threadContext;
        CacheBuilder<String, Boolean> nlcBuilder = CacheBuilder.builder();
        final int nlcCacheSize = NEGATIVE_LOOKUP_CACHE_SIZE_SETTING.get(settings);
        if (nlcCacheSize >= 0) {
            nlcBuilder.setMaximumWeight(nlcCacheSize);
        }
        this.negativeLookupCache = nlcBuilder.build();
        this.builtInRoleProviders = List.of(reservedRolesStore, fileRolesStore, nativeRolesStore);
        if (rolesProviders.isEmpty()) {
            this.allRoleProviders = this.builtInRoleProviders;
        } else {
            List<BiConsumer<Set<String>, ActionListener<RoleRetrievalResult>>> allList =
                new ArrayList<>(builtInRoleProviders.size() + rolesProviders.size());
            allList.addAll(builtInRoleProviders);
            allList.addAll(rolesProviders);
            this.allRoleProviders = Collections.unmodifiableList(allList);
        }
        this.anonymousUser = new AnonymousUser(settings);
        this.isAnonymousEnabled = AnonymousUser.isAnonymousEnabled(settings);
    }

    public void roles(Set<String> roleNames, ActionListener<Role> roleActionListener) {
        final RoleKey roleKey = new RoleKey(roleNames, ROLES_STORE_SOURCE);
        Role existing = roleCache.get(roleKey);
        if (existing != null) {
            roleActionListener.onResponse(existing);
        } else {
            final long invalidationCounter = numInvalidation.get();
            roleDescriptors(roleNames, ActionListener.wrap(
                    rolesRetrievalResult -> {
                        logDeprecatedRoles(rolesRetrievalResult.roleDescriptors);
                        final boolean missingRoles = rolesRetrievalResult.getMissingRoles().isEmpty() == false;
                        if (missingRoles) {
                            logger.debug(() -> new ParameterizedMessage("Could not find roles with names {}",
                                    rolesRetrievalResult.getMissingRoles()));
                        }
                        final Set<RoleDescriptor> effectiveDescriptors;
                        Set<RoleDescriptor> roleDescriptors = rolesRetrievalResult.getRoleDescriptors();
                        if (roleDescriptors.stream().anyMatch(RoleDescriptor::isUsingDocumentOrFieldLevelSecurity) &&
                            licenseState.checkFeature(Feature.SECURITY_DLS_FLS) == false) {
                            effectiveDescriptors = roleDescriptors.stream()
                                .filter(not(RoleDescriptor::isUsingDocumentOrFieldLevelSecurity))
                                .collect(Collectors.toSet());
                        } else {
                            effectiveDescriptors = roleDescriptors;
                        }
                        logger.trace(() -> new ParameterizedMessage("Exposing effective role descriptors [{}] for role names [{}]",
                                effectiveDescriptors, roleNames));
                        effectiveRoleDescriptorsConsumer.accept(Collections.unmodifiableCollection(effectiveDescriptors));
                        logger.trace(() -> new ParameterizedMessage("Building role from descriptors [{}] for role names [{}]",
                                effectiveDescriptors, roleNames));
                        buildThenMaybeCacheRole(roleKey, effectiveDescriptors, rolesRetrievalResult.getMissingRoles(),
                            rolesRetrievalResult.isSuccess(), invalidationCounter, roleActionListener);
                    },
                    roleActionListener::onFailure));
        }
    }

    void logDeprecatedRoles(Set<RoleDescriptor> roleDescriptors) {
        roleDescriptors.stream()
            .filter(rd -> Boolean.TRUE.equals(rd.getMetadata().get(MetadataUtils.DEPRECATED_METADATA_KEY)))
            .forEach(rd -> {
                String reason = Objects.toString(
                    rd.getMetadata().get(MetadataUtils.DEPRECATED_REASON_METADATA_KEY), "Please check the documentation");
                deprecationLogger.deprecate(DeprecationCategory.SECURITY, "deprecated_role-" + rd.getName(), "The role [" + rd.getName() +
                            "] is deprecated and will be removed in a future version of Elasticsearch. " + reason);
            });
    }

    public void getRoles(User user, Authentication authentication, ActionListener<Role> roleActionListener) {
        // we need to special case the internal users in this method, if we apply the anonymous roles to every user including these system
        // user accounts then we run into the chance of a deadlock because then we need to get a role that we may be trying to get as the
        // internal user. The SystemUser is special cased as it has special privileges to execute internal actions and should never be
        // passed into this method. The XPackUser has the Superuser role and we can simply return that
        if (SystemUser.is(user)) {
            throw new IllegalArgumentException("the user [" + user.principal() + "] is the system user and we should never try to get its" +
                " roles");
        }
        if (XPackUser.is(user)) {
            assert XPackUser.INSTANCE.roles().length == 1;
            roleActionListener.onResponse(XPackUser.ROLE);
            return;
        }
        if (XPackSecurityUser.is(user)) {
            roleActionListener.onResponse(ReservedRolesStore.SUPERUSER_ROLE);
            return;
        }
        if (AsyncSearchUser.is(user)) {
            roleActionListener.onResponse(AsyncSearchUser.ROLE);
            return;
        }

        if (authentication.isServiceAccount()) {
            getRolesForServiceAccount(authentication, roleActionListener);
        } else if (ApiKeyService.isApiKeyAuthentication(authentication)) {
            getRolesForApiKey(authentication, roleActionListener);
        } else {
            Set<String> roleNames = new HashSet<>(Arrays.asList(user.roles()));
            if (isAnonymousEnabled && anonymousUser.equals(user) == false) {
                if (anonymousUser.roles().length == 0) {
                    throw new IllegalStateException("anonymous is only enabled when the anonymous user has roles");
                }
                Collections.addAll(roleNames, anonymousUser.roles());
            }

            if (roleNames.isEmpty()) {
                roleActionListener.onResponse(Role.EMPTY);
            } else if (roleNames.contains(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName())) {
                roleActionListener.onResponse(ReservedRolesStore.SUPERUSER_ROLE);
            } else {
                roles(roleNames, roleActionListener);
            }
        }
    }

    private void getRolesForServiceAccount(Authentication authentication, ActionListener<Role> roleActionListener) {
        serviceAccountService.getRoleDescriptor(authentication, ActionListener.wrap(roleDescriptor -> {
            final RoleKey roleKey = new RoleKey(Set.of(roleDescriptor.getName()), "service_account");
            final Role existing = roleCache.get(roleKey);
            if (existing == null) {
                final long invalidationCounter = numInvalidation.get();
                buildThenMaybeCacheRole(roleKey, List.of(roleDescriptor), Set.of(), true, invalidationCounter, roleActionListener);
            } else {
                roleActionListener.onResponse(existing);
            }
        }, roleActionListener::onFailure));
    }

    private void getRolesForApiKey(Authentication authentication, ActionListener<Role> roleActionListener) {
        if (authentication.getVersion().onOrAfter(VERSION_API_KEY_ROLES_AS_BYTES)) {
            buildAndCacheRoleForApiKey(authentication, false, ActionListener.wrap(
                role -> {
                    if (role == Role.EMPTY) {
                        buildAndCacheRoleForApiKey(authentication, true, roleActionListener);
                    } else {
                        buildAndCacheRoleForApiKey(authentication, true, ActionListener.wrap(
                            limitedByRole -> roleActionListener.onResponse(
                                limitedByRole == Role.EMPTY ? role : LimitedRole.createLimitedRole(role, limitedByRole)),
                            roleActionListener::onFailure
                        ));
                    }
                },
                roleActionListener::onFailure
            ));
        } else {
            apiKeyService.getRoleForApiKey(authentication, ActionListener.wrap(apiKeyRoleDescriptors -> {
                final List<RoleDescriptor> descriptors = apiKeyRoleDescriptors.getRoleDescriptors();
                if (descriptors == null) {
                    roleActionListener.onFailure(new IllegalStateException("missing role descriptors"));
                } else if (apiKeyRoleDescriptors.getLimitedByRoleDescriptors() == null) {
                    buildAndCacheRoleFromDescriptors(descriptors,
                        apiKeyRoleDescriptors.getApiKeyId() + "_role_desc", roleActionListener);
                } else {
                    buildAndCacheRoleFromDescriptors(descriptors, apiKeyRoleDescriptors.getApiKeyId() + "_role_desc",
                        ActionListener.wrap(
                            role -> buildAndCacheRoleFromDescriptors(apiKeyRoleDescriptors.getLimitedByRoleDescriptors(),
                                apiKeyRoleDescriptors.getApiKeyId() + "_limited_role_desc", ActionListener.wrap(
                                    limitedBy -> roleActionListener.onResponse(LimitedRole.createLimitedRole(role, limitedBy)),
                                    roleActionListener::onFailure)), roleActionListener::onFailure));
                }
            }, roleActionListener::onFailure));
        }
    }

    public void buildAndCacheRoleFromDescriptors(Collection<RoleDescriptor> roleDescriptors, String source,
                                                  ActionListener<Role> listener) {
        if (ROLES_STORE_SOURCE.equals(source)) {
            throw new IllegalArgumentException("source [" + ROLES_STORE_SOURCE + "] is reserved for internal use");
        }
        RoleKey roleKey = new RoleKey(roleDescriptors.stream().map(RoleDescriptor::getName).collect(Collectors.toSet()), source);
        Role existing = roleCache.get(roleKey);
        if (existing != null) {
            listener.onResponse(existing);
        } else {
            final long invalidationCounter = numInvalidation.get();
            buildThenMaybeCacheRole(roleKey, roleDescriptors, Collections.emptySet(), true, invalidationCounter, listener);
        }
    }

    private void buildThenMaybeCacheRole(RoleKey roleKey, Collection<RoleDescriptor> roleDescriptors, Set<String> missing,
                                         boolean tryCache, long invalidationCounter, ActionListener<Role> listener) {
        logger.trace("Building role from descriptors [{}] for names [{}] from source [{}]", roleDescriptors, roleKey.names, roleKey.source);
        buildRoleFromDescriptors(roleDescriptors, fieldPermissionsCache, privilegeStore, ActionListener.wrap(role -> {
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
            listener.onResponse(role);
        }, listener::onFailure));
    }

    private void buildAndCacheRoleForApiKey(Authentication authentication, boolean limitedBy, ActionListener<Role> roleActionListener) {
        final Tuple<String, BytesReference> apiKeyIdAndBytes = apiKeyService.getApiKeyIdAndRoleBytes(authentication, limitedBy);
        final String roleDescriptorsHash =
                MessageDigests.toHexString(MessageDigests.digest(apiKeyIdAndBytes.v2(), MessageDigests.sha256()));
        final RoleKey roleKey = new RoleKey(Set.of("apikey:" + roleDescriptorsHash), limitedBy ? "apikey_limited_role" : "apikey_role");
        final Role existing = roleCache.get(roleKey);
        if (existing == null) {
            final long invalidationCounter = numInvalidation.get();
            final List<RoleDescriptor> roleDescriptors = apiKeyService.parseRoleDescriptors(apiKeyIdAndBytes.v1(), apiKeyIdAndBytes.v2());
            buildThenMaybeCacheRole(roleKey, roleDescriptors, Collections.emptySet(),
                true, invalidationCounter, roleActionListener);
        } else {
            roleActionListener.onResponse(existing);
        }
    }

    public void getRoleDescriptors(Set<String> roleNames, ActionListener<Set<RoleDescriptor>> listener) {
        roleDescriptors(roleNames, ActionListener.wrap(rolesRetrievalResult -> {
            if (rolesRetrievalResult.isSuccess()) {
                listener.onResponse(rolesRetrievalResult.getRoleDescriptors());
            } else {
                listener.onFailure(new ElasticsearchException("role retrieval had one or more failures"));
            }
        }, listener::onFailure));
    }

    private void roleDescriptors(Set<String> roleNames, ActionListener<RolesRetrievalResult> rolesResultListener) {
        final Set<String> filteredRoleNames = roleNames.stream().filter((s) -> {
            if (negativeLookupCache.get(s) != null) {
                logger.debug(() -> new ParameterizedMessage("Requested role [{}] does not exist (cached)", s));
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
            licenseState.checkFeature(Feature.SECURITY_CUSTOM_ROLE_PROVIDERS) ? allRoleProviders : builtInRoleProviders;

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
        final List<ConfigurableClusterPrivilege> configurableClusterPrivileges = new ArrayList<>();
        Set<String> runAs = new HashSet<>();
        final Map<Set<String>, MergeableIndicesPrivilege> restrictedIndicesPrivilegesMap = new HashMap<>();
        final Map<Set<String>, MergeableIndicesPrivilege> indicesPrivilegesMap = new HashMap<>();

        // Keyed by application + resource
        Map<Tuple<String, Set<String>>, Set<String>> applicationPrivilegesMap = new HashMap<>();

        List<String> roleNames = new ArrayList<>(roleDescriptors.size());
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
                .cluster(clusterPrivileges, configurableClusterPrivileges)
                .runAs(runAsPrivilege);
        indicesPrivilegesMap.entrySet().forEach((entry) -> {
            MergeableIndicesPrivilege privilege = entry.getValue();
            builder.add(fieldPermissionsCache.getFieldPermissions(privilege.fieldPermissionsDefinition), privilege.query,
                    IndexPrivilege.get(privilege.privileges), false, privilege.indices.toArray(Strings.EMPTY_ARRAY));
        });
        restrictedIndicesPrivilegesMap.entrySet().forEach((entry) -> {
            MergeableIndicesPrivilege privilege = entry.getValue();
            builder.add(fieldPermissionsCache.getFieldPermissions(privilege.fieldPermissionsDefinition), privilege.query,
                    IndexPrivilege.get(privilege.privileges), true, privilege.indices.toArray(Strings.EMPTY_ARRAY));
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
                applicationPrivilegesMap.forEach((key, names) -> ApplicationPrivilege.get(key.v1(), names, appPrivileges)
                    .forEach(priv -> builder.addApplicationPrivilege(priv, key.v2())));
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

        roleCacheHelper.removeKeysIf(key -> key.names.contains(role));
        negativeLookupCache.invalidate(role);
    }

    public void invalidate(Set<String> roles) {
        numInvalidation.incrementAndGet();
        roleCacheHelper.removeKeysIf(key -> Sets.haveEmptyIntersection(key.names, roles) == false);
        roles.forEach(negativeLookupCache::invalidate);
    }

    public void usageStats(ActionListener<Map<String, Object>> listener) {
        final Map<String, Object> usage = new HashMap<>(2);
        usage.put("file", fileRolesStore.usageStats());
        usage.put("dls", Map.of("bit_set_cache", dlsBitsetCache.usageStats()));
        nativeRolesStore.usageStats(ActionListener.wrap(map -> {
            usage.put("native", map);
            listener.onResponse(usage);
        }, listener::onFailure));
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

        private static void collatePrivilegesByIndices(IndicesPrivileges[] indicesPrivileges, boolean allowsRestrictedIndices,
                Map<Set<String>, MergeableIndicesPrivilege> indicesPrivilegesMap) {
            for (final IndicesPrivileges indicesPrivilege : indicesPrivileges) {
                // if a index privilege is an explicit denial, then we treat it as non-existent since we skipped these in the past when
                // merging
                final boolean isExplicitDenial = indicesPrivileges.length == 1
                        && "none".equalsIgnoreCase(indicesPrivilege.getPrivileges()[0]);
                if (isExplicitDenial || (indicesPrivilege.allowRestrictedIndices() != allowsRestrictedIndices)) {
                    continue;
                }
                final Set<String> key = newHashSet(indicesPrivilege.getIndices());
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

    private static final class RoleKey {

        private final Set<String> names;
        private final String source;

        private RoleKey(Set<String> names, String source) {
            this.names = Objects.requireNonNull(names);
            this.source = Objects.requireNonNull(source);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            RoleKey roleKey = (RoleKey) o;
            return names.equals(roleKey.names) &&
                source.equals(roleKey.source);
        }

        @Override
        public int hashCode() {
            return Objects.hash(names, source);
        }
    }

    public static List<Setting<?>> getSettings() {
        return Arrays.asList(CACHE_SIZE_SETTING, NEGATIVE_LOOKUP_CACHE_SIZE_SETTING);
    }
}
