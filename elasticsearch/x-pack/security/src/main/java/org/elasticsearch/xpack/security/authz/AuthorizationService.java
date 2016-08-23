/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.CompositeIndicesRequest;
import org.elasticsearch.action.IndicesRequest;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.search.ClearScrollAction;
import org.elasticsearch.action.search.SearchScrollAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasOrIndex;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Setting.Property;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.search.action.SearchTransportService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.security.SecurityTemplateService;
import org.elasticsearch.xpack.security.authc.Authentication;
import org.elasticsearch.xpack.security.authc.AuthenticationFailureHandler;
import org.elasticsearch.xpack.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.security.authz.indicesresolver.DefaultIndicesAndAliasesResolver;
import org.elasticsearch.xpack.security.authz.indicesresolver.IndicesAndAliasesResolver;
import org.elasticsearch.xpack.security.authz.permission.ClusterPermission;
import org.elasticsearch.xpack.security.authz.permission.DefaultRole;
import org.elasticsearch.xpack.security.authz.permission.GlobalPermission;
import org.elasticsearch.xpack.security.authz.permission.Role;
import org.elasticsearch.xpack.security.authz.permission.RunAsPermission;
import org.elasticsearch.xpack.security.authz.permission.SuperuserRole;
import org.elasticsearch.xpack.security.authz.privilege.ClusterPrivilege;
import org.elasticsearch.xpack.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.audit.AuditTrailService;
import org.elasticsearch.xpack.security.user.AnonymousUser;
import org.elasticsearch.xpack.security.user.SystemUser;
import org.elasticsearch.xpack.security.user.User;
import org.elasticsearch.xpack.security.user.XPackUser;

import static org.elasticsearch.xpack.security.Security.setting;
import static org.elasticsearch.xpack.security.support.Exceptions.authorizationError;

/**
 *
 */
public class AuthorizationService extends AbstractComponent {

    public static final Setting<Boolean> ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING =
            Setting.boolSetting(setting("authc.anonymous.authz_exception"), true, Property.NodeScope);
    public static final String INDICES_PERMISSIONS_KEY = "_indices_permissions";
    static final String ORIGINATING_ACTION_KEY = "_originating_action_name";

    private static final Predicate<String> MONITOR_INDEX_PREDICATE = IndexPrivilege.MONITOR.predicate();

    private final ClusterService clusterService;
    private final CompositeRolesStore rolesStore;
    private final AuditTrailService auditTrail;
    private final IndicesAndAliasesResolver[] indicesAndAliasesResolvers;
    private final AuthenticationFailureHandler authcFailureHandler;
    private final ThreadContext threadContext;
    private final AnonymousUser anonymousUser;
    private final boolean isAnonymousEnabled;
    private final boolean anonymousAuthzExceptionEnabled;

    public AuthorizationService(Settings settings, CompositeRolesStore rolesStore, ClusterService clusterService,
                                AuditTrailService auditTrail, AuthenticationFailureHandler authcFailureHandler,
                                ThreadPool threadPool, AnonymousUser anonymousUser) {
        super(settings);
        this.rolesStore = rolesStore;
        this.clusterService = clusterService;
        this.auditTrail = auditTrail;
        this.indicesAndAliasesResolvers = new IndicesAndAliasesResolver[] {
                new DefaultIndicesAndAliasesResolver(this, new IndexNameExpressionResolver(settings))
        };
        this.authcFailureHandler = authcFailureHandler;
        this.threadContext = threadPool.getThreadContext();
        this.anonymousUser = anonymousUser;
        this.isAnonymousEnabled = AnonymousUser.isAnonymousEnabled(settings);
        this.anonymousAuthzExceptionEnabled = ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING.get(settings);
    }

    /**
     * Returns all indices and aliases the given user is allowed to execute the given action on.
     *
     * @param user      The user
     * @param action    The action
     */
    public List<String> authorizedIndicesAndAliases(User user, String action) {
        final String[] anonymousRoles = isAnonymousEnabled ? anonymousUser.roles() : Strings.EMPTY_ARRAY;
        String[] rolesNames = user.roles();
        if (rolesNames.length == 0 && anonymousRoles.length == 0) {
            return Collections.emptyList();
        }

        List<Predicate<String>> predicates = new ArrayList<>();
        for (String roleName : rolesNames) {
            Role role = rolesStore.role(roleName);
            if (role != null) {
                predicates.add(role.indices().allowedIndicesMatcher(action));
            }
        }
        if (anonymousUser.equals(user) == false) {
            for (String roleName : anonymousRoles) {
                Role role = rolesStore.role(roleName);
                if (role != null) {
                    predicates.add(role.indices().allowedIndicesMatcher(action));
                }
            }
        }
        Predicate<String> predicate = predicates.stream().reduce(s -> false, (p1, p2) -> p1.or(p2));

        List<String> indicesAndAliases = new ArrayList<>();
        MetaData metaData = clusterService.state().metaData();
        // TODO: can this be done smarter? I think there are usually more indices/aliases in the cluster then indices defined a roles?
        for (Map.Entry<String, AliasOrIndex> entry : metaData.getAliasAndIndexLookup().entrySet()) {
            String aliasOrIndex = entry.getKey();
            if (predicate.test(aliasOrIndex)) {
                indicesAndAliases.add(aliasOrIndex);
            }
        }

        if (XPackUser.is(user) == false) {
            // we should filter out the .security index from wildcards
            if (indicesAndAliases.remove(SecurityTemplateService.SECURITY_INDEX_NAME)) {
                logger.debug("removed [{}] from user [{}] list of authorized indices",
                        SecurityTemplateService.SECURITY_INDEX_NAME, user.principal());
            }
        }
        return Collections.unmodifiableList(indicesAndAliases);
    }

    /**
     * Verifies that the given user can execute the given request (and action). If the user doesn't
     * have the appropriate privileges for this action/request, an {@link ElasticsearchSecurityException}
     * will be thrown.
     *
     * @param authentication  The authentication information
     * @param action          The action
     * @param request         The request
     * @throws ElasticsearchSecurityException   If the given user is no allowed to execute the given request
     */
    public void authorize(Authentication authentication, String action, TransportRequest request) throws ElasticsearchSecurityException {
        // prior to doing any authorization lets set the originating action in the context only
        setOriginatingAction(action);

        // first we need to check if the user is the system. If it is, we'll just authorize the system access
        if (SystemUser.is(authentication.getRunAsUser())) {
            if (SystemUser.isAuthorized(action) && SystemUser.is(authentication.getUser())) {
                setIndicesAccessControl(IndicesAccessControl.ALLOW_ALL);
                grant(authentication, action, request);
                return;
            }
            throw denial(authentication, action, request);
        }

        // get the roles of the authenticated user, which may be different than the effective
        GlobalPermission permission = permission(authentication.getUser().roles());

        final boolean isRunAs = authentication.getUser() != authentication.getRunAsUser();
        // permission can be null as it might be that the user's role
        // is unknown
        if (permission == null || permission.isEmpty()) {
            if (isRunAs) {
                // the request is a run as request so we should call the specific audit event for a denied run as attempt
                throw denyRunAs(authentication, action, request);
            } else {
                throw denial(authentication, action, request);
            }
        }

        // check if the request is a run as request
        if (isRunAs) {
            // first we must authorize for the RUN_AS action
            RunAsPermission runAs = permission.runAs();
            if (runAs != null && runAs.check(authentication.getRunAsUser().principal())) {
                grantRunAs(authentication, action, request);
                permission = permission(authentication.getRunAsUser().roles());

                // permission can be null as it might be that the user's role
                // is unknown
                if (permission == null || permission.isEmpty()) {
                    throw denial(authentication, action, request);
                }
            } else {
                throw denyRunAs(authentication, action, request);
            }
        }

        // first, we'll check if the action is a cluster action. If it is, we'll only check it
        // against the cluster permissions
        if (ClusterPrivilege.ACTION_MATCHER.test(action)) {
            ClusterPermission cluster = permission.cluster();
            // we use the effectiveUser for permission checking since we are running as a user!
            if (cluster != null && cluster.check(action, request, authentication)) {
                setIndicesAccessControl(IndicesAccessControl.ALLOW_ALL);
                grant(authentication, action, request);
                return;
            }
            throw denial(authentication, action, request);
        }

        // ok... this is not a cluster action, let's verify it's an indices action
        if (!IndexPrivilege.ACTION_MATCHER.test(action)) {
            throw denial(authentication, action, request);
        }

        // some APIs are indices requests that are not actually associated with indices. For example,
        // search scroll request, is categorized under the indices context, but doesn't hold indices names
        // (in this case, the security check on the indices was done on the search request that initialized
        // the scroll... and we rely on the signed scroll id to provide security over this request).
        // so we only check indices if indeed the request is an actual IndicesRequest, if it's not,
        // we just grant it if it's a scroll, deny otherwise
        if (!(request instanceof IndicesRequest) && !(request instanceof CompositeIndicesRequest)) {
            if (isScrollRelatedAction(action)) {
                //note that clear scroll shard level actions can originate from a clear scroll all, which doesn't require any
                //indices permission as it's categorized under cluster. This is why the scroll check is performed
                //even before checking if the user has any indices permission.
                grant(authentication, action, request);
                return;
            }
            assert false : "only scroll related requests are known indices api that don't support retrieving the indices they relate to";
            throw denial(authentication, action, request);
        }

        if (permission.indices() == null || permission.indices().isEmpty()) {
            throw denial(authentication, action, request);
        }

        ClusterState clusterState = clusterService.state();
        Set<String> indexNames = resolveIndices(authentication, action, request, clusterState);
        assert !indexNames.isEmpty() : "every indices request needs to have its indices set thus the resolved indices must not be empty";
        MetaData metaData = clusterState.metaData();
        IndicesAccessControl indicesAccessControl = permission.authorize(action, indexNames, metaData);
        if (!indicesAccessControl.isGranted()) {
            throw denial(authentication, action, request);
        } else if (indicesAccessControl.getIndexPermissions(SecurityTemplateService.SECURITY_INDEX_NAME) != null
                && indicesAccessControl.getIndexPermissions(SecurityTemplateService.SECURITY_INDEX_NAME).isGranted()
                && XPackUser.is(authentication.getRunAsUser()) == false
                && MONITOR_INDEX_PREDICATE.test(action) == false
                && Arrays.binarySearch(authentication.getRunAsUser().roles(), SuperuserRole.NAME) < 0) {
            // only the XPackUser is allowed to work with this index, but we should allow indices monitoring actions through for debugging
            // purposes. These monitor requests also sometimes resolve indices concretely and then requests them
            logger.debug("user [{}] attempted to directly perform [{}] against the security index [{}]",
                    authentication.getRunAsUser().principal(), action, SecurityTemplateService.SECURITY_INDEX_NAME);
            throw denial(authentication, action, request);
        } else {
            setIndicesAccessControl(indicesAccessControl);
        }

        //if we are creating an index we need to authorize potential aliases created at the same time
        if (IndexPrivilege.CREATE_INDEX_MATCHER.test(action)) {
            assert request instanceof CreateIndexRequest;
            Set<Alias> aliases = ((CreateIndexRequest) request).aliases();
            if (!aliases.isEmpty()) {
                Set<String> aliasesAndIndices = Sets.newHashSet(indexNames);
                for (Alias alias : aliases) {
                    aliasesAndIndices.add(alias.name());
                }
                indicesAccessControl = permission.authorize("indices:admin/aliases", aliasesAndIndices, metaData);
                if (!indicesAccessControl.isGranted()) {
                    throw denial(authentication, "indices:admin/aliases", request);
                }
                // no need to re-add the indicesAccessControl in the context,
                // because the create index call doesn't do anything FLS or DLS
            }
        }

        grant(authentication, action, request);
    }

    private void setIndicesAccessControl(IndicesAccessControl accessControl) {
        if (threadContext.getTransient(INDICES_PERMISSIONS_KEY) == null) {
            threadContext.putTransient(INDICES_PERMISSIONS_KEY, accessControl);
        }
    }

    private void setOriginatingAction(String action) {
        String originatingAction = threadContext.getTransient(ORIGINATING_ACTION_KEY);
        if (originatingAction == null) {
            threadContext.putTransient(ORIGINATING_ACTION_KEY, action);
        }
    }

    private GlobalPermission permission(String[] roleNames) {
        if (roleNames.length == 0) {
            return DefaultRole.INSTANCE;
        }

        if (roleNames.length == 1) {
            Role role = rolesStore.role(roleNames[0]);
            return role == null ? DefaultRole.INSTANCE : GlobalPermission.Compound.builder().add(DefaultRole.INSTANCE).add(role).build();
        }

        // we'll take all the roles and combine their associated permissions

        GlobalPermission.Compound.Builder roles = GlobalPermission.Compound.builder().add(DefaultRole.INSTANCE);
        for (String roleName : roleNames) {
            Role role = rolesStore.role(roleName);
            if (role != null) {
                roles.add(role);
            }
        }
        return roles.build();
    }

    private Set<String> resolveIndices(Authentication authentication, String action, TransportRequest request, ClusterState clusterState) {
        MetaData metaData = clusterState.metaData();
        for (IndicesAndAliasesResolver resolver : indicesAndAliasesResolvers) {
            if (resolver.requestType().isInstance(request)) {
                return resolver.resolve(authentication.getRunAsUser(), action, request, metaData);
            }
        }
        assert false : "we should be able to resolve indices for any known request that requires indices privileges";
        throw denial(authentication, action, request);
    }

    private static boolean isScrollRelatedAction(String action) {
        return action.equals(SearchScrollAction.NAME) ||
                action.equals(SearchTransportService.FETCH_ID_SCROLL_ACTION_NAME) ||
                action.equals(SearchTransportService.QUERY_FETCH_SCROLL_ACTION_NAME) ||
                action.equals(SearchTransportService.QUERY_SCROLL_ACTION_NAME) ||
                action.equals(SearchTransportService.FREE_CONTEXT_SCROLL_ACTION_NAME) ||
                action.equals(ClearScrollAction.NAME) ||
                action.equals(SearchTransportService.CLEAR_SCROLL_CONTEXTS_ACTION_NAME);
    }

    private ElasticsearchSecurityException denial(Authentication authentication, String action, TransportRequest request) {
        auditTrail.accessDenied(authentication.getUser(), action, request);
        return denialException(authentication, action);
    }

    private ElasticsearchSecurityException denyRunAs(Authentication authentication, String action, TransportRequest request) {
        auditTrail.runAsDenied(authentication.getUser(), action, request);
        return denialException(authentication, action);
    }

    private void grant(Authentication authentication, String action, TransportRequest request) {
        auditTrail.accessGranted(authentication.getUser(), action, request);
    }

    private void grantRunAs(Authentication authentication, String action, TransportRequest request) {
        auditTrail.runAsGranted(authentication.getUser(), action, request);
    }

    private ElasticsearchSecurityException denialException(Authentication authentication, String action) {
        final User user = authentication.getUser();
        // Special case for anonymous user
        if (isAnonymousEnabled && anonymousUser.equals(user)) {
            if (anonymousAuthzExceptionEnabled == false) {
                throw authcFailureHandler.authenticationRequired(action, threadContext);
            }
        }
        // check for run as
        if (user != authentication.getRunAsUser()) {
            return authorizationError("action [{}] is unauthorized for user [{}] run as [{}]", action, user.principal(),
                    authentication.getRunAsUser().principal());
        }
        return authorizationError("action [{}] is unauthorized for user [{}]", action, user.principal());
    }

    public static void addSettings(List<Setting<?>> settings) {
        settings.add(ANONYMOUS_AUTHORIZATION_EXCEPTION_SETTING);
    }
}
