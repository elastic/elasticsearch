/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.example;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse;
import org.elasticsearch.xpack.core.security.action.user.GetUserPrivilegesResponse.Indices;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.Subject;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.PrivilegesToCheck;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.PrivilegesCheckResult;
import org.elasticsearch.xpack.core.security.authz.ResolvedIndices;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor.IndicesPrivileges;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl;
import org.elasticsearch.xpack.core.security.authz.accesscontrol.IndicesAccessControl.IndexAccessControl;
import org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions;
import org.elasticsearch.xpack.core.security.authz.permission.ResourcePrivileges;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ConfigurableClusterPrivilege;
import org.elasticsearch.xpack.core.security.user.User;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A custom implementation of an authorization engine. This engine is extremely basic in that it
 * authorizes based upon the name of a single role. If users have this role they are granted access.
 */
public class CustomAuthorizationEngine implements AuthorizationEngine {

    @Override
    public void resolveAuthorizationInfo(RequestInfo requestInfo, ActionListener<AuthorizationInfo> listener) {
        final Authentication authentication = requestInfo.getAuthentication();
        if (authentication.isRunAs()) {
            final CustomAuthorizationInfo authenticatedUserAuthzInfo =
                new CustomAuthorizationInfo(authentication.getAuthenticatingSubject().getUser().roles(), null);
            listener.onResponse(new CustomAuthorizationInfo(authentication.getEffectiveSubject().getUser().roles(),
                authenticatedUserAuthzInfo));
        } else {
            listener.onResponse(new CustomAuthorizationInfo(authentication.getEffectiveSubject().getUser().roles(), null));
        }
    }

    @Override
    public void resolveAuthorizationInfo(Subject subject, ActionListener<AuthorizationInfo> listener) {
        listener.onResponse(new CustomAuthorizationInfo(subject.getUser().roles(), null));
    }

    @Override
    public void authorizeRunAs(RequestInfo requestInfo, AuthorizationInfo authorizationInfo, ActionListener<AuthorizationResult> listener) {
        if (isSuperuser(requestInfo.getAuthentication().getAuthenticatingSubject().getUser())) {
            listener.onResponse(AuthorizationResult.granted());
        } else {
            listener.onResponse(AuthorizationResult.deny());
        }
    }

    @Override
    public void authorizeClusterAction(RequestInfo requestInfo, AuthorizationInfo authorizationInfo,
                                       ActionListener<AuthorizationResult> listener) {
        if (isSuperuser(requestInfo.getAuthentication().getEffectiveSubject().getUser())) {
            listener.onResponse(AuthorizationResult.granted());
        } else {
            listener.onResponse(AuthorizationResult.deny());
        }
    }

    @Override
    public void authorizeIndexAction(RequestInfo requestInfo, AuthorizationInfo authorizationInfo,
                                     AsyncSupplier<ResolvedIndices> indicesAsyncSupplier,
                                     Map<String, IndexAbstraction> aliasOrIndexLookup,
                                     ActionListener<IndexAuthorizationResult> listener) {
        if (isSuperuser(requestInfo.getAuthentication().getEffectiveSubject().getUser())) {
            indicesAsyncSupplier.getAsync(ActionListener.wrap(resolvedIndices -> {
                Map<String, IndexAccessControl> indexAccessControlMap = new HashMap<>();
                for (String name : resolvedIndices.getLocal()) {
                    indexAccessControlMap.put(name, new IndexAccessControl(FieldPermissions.DEFAULT, null));
                }
                IndicesAccessControl indicesAccessControl =
                    new IndicesAccessControl(true, Collections.unmodifiableMap(indexAccessControlMap));
                listener.onResponse(new IndexAuthorizationResult(true, indicesAccessControl));
            }, listener::onFailure));
        } else {
            listener.onResponse(new IndexAuthorizationResult(true, IndicesAccessControl.DENIED));
        }
    }

    @Override
    public void loadAuthorizedIndices(RequestInfo requestInfo, AuthorizationInfo authorizationInfo,
                                      Map<String, IndexAbstraction> indicesLookup, ActionListener<Set<String>> listener) {
        if (isSuperuser(requestInfo.getAuthentication().getEffectiveSubject().getUser())) {
            listener.onResponse(indicesLookup.keySet());
        } else {
            listener.onResponse(Collections.emptySet());
        }
    }

    @Override
    public void validateIndexPermissionsAreSubset(RequestInfo requestInfo, AuthorizationInfo authorizationInfo,
                                                  Map<String, List<String>> indexNameToNewNames,
                                                  ActionListener<AuthorizationResult> listener) {
        if (isSuperuser(requestInfo.getAuthentication().getEffectiveSubject().getUser())) {
            listener.onResponse(AuthorizationResult.granted());
        } else {
            listener.onResponse(AuthorizationResult.deny());
        }
    }

    @Override
    public void checkPrivileges(AuthorizationInfo authorizationInfo,
                                PrivilegesToCheck privilegesToCheck,
                                Collection<ApplicationPrivilegeDescriptor> applicationPrivilegeDescriptors,
                                ActionListener<PrivilegesCheckResult> listener) {
        if (isSuperuser(authorizationInfo)) {
            listener.onResponse(getPrivilegesCheckResult(privilegesToCheck, true));
        } else {
            listener.onResponse(getPrivilegesCheckResult(privilegesToCheck, false));
        }
    }

    @Override
    public void getUserPrivileges(AuthorizationInfo authorizationInfo,
                                  ActionListener<GetUserPrivilegesResponse> listener) {
        if (isSuperuser(authorizationInfo)) {
            listener.onResponse(getUserPrivilegesResponse(true));
        } else {
            listener.onResponse(getUserPrivilegesResponse(false));
        }
    }

    private PrivilegesCheckResult getPrivilegesCheckResult(PrivilegesToCheck privilegesToCheck, boolean authorized) {
        Map<String, Boolean> clusterPrivMap = new HashMap<>();
        for (String clusterPriv : privilegesToCheck.cluster()) {
            clusterPrivMap.put(clusterPriv, authorized);
        }
        final Map<String, ResourcePrivileges> indices = new LinkedHashMap<>();
        for (IndicesPrivileges check : privilegesToCheck.index()) {
            for (String index : check.getIndices()) {
                final Map<String, Boolean> privileges = new HashMap<>();
                final ResourcePrivileges existing = indices.get(index);
                if (existing != null) {
                    privileges.putAll(existing.getPrivileges());
                }
                for (String privilege : check.getPrivileges()) {
                    privileges.put(privilege, authorized);
                }
                indices.put(index, ResourcePrivileges.builder(index).addPrivileges(privileges).build());
            }
        }
        final Map<String, Collection<ResourcePrivileges>> privilegesByApplication = new HashMap<>();
        Set<String> applicationNames = Arrays.stream(privilegesToCheck.application())
            .map(RoleDescriptor.ApplicationResourcePrivileges::getApplication)
            .collect(Collectors.toSet());
        for (String applicationName : applicationNames) {
            final Map<String, ResourcePrivileges> appPrivilegesByResource = new LinkedHashMap<>();
            for (RoleDescriptor.ApplicationResourcePrivileges p : privilegesToCheck.application()) {
                if (applicationName.equals(p.getApplication())) {
                    for (String resource : p.getResources()) {
                        final Map<String, Boolean> privileges = new HashMap<>();
                        final ResourcePrivileges existing = appPrivilegesByResource.get(resource);
                        if (existing != null) {
                            privileges.putAll(existing.getPrivileges());
                        }
                        for (String privilege : p.getPrivileges()) {
                            privileges.put(privilege, authorized);
                        }
                        appPrivilegesByResource.put(resource, ResourcePrivileges.builder(resource).addPrivileges(privileges).build());
                    }
                }
            }
            privilegesByApplication.put(applicationName, appPrivilegesByResource.values());
        }
        return new PrivilegesCheckResult(authorized,
            new PrivilegesCheckResult.Details(clusterPrivMap, indices, privilegesByApplication));
    }

    private GetUserPrivilegesResponse getUserPrivilegesResponse(boolean isSuperuser) {
        final Set<String> cluster = isSuperuser ? Collections.singleton("ALL") : Collections.emptySet();
        final Set<ConfigurableClusterPrivilege> conditionalCluster = Collections.emptySet();
        final Set<GetUserPrivilegesResponse.Indices> indices = isSuperuser ? Collections.singleton(new Indices(Collections.singleton("*"),
            Collections.singleton("*"), Collections.emptySet(), Collections.emptySet(), true)) : Collections.emptySet();

        final Set<RoleDescriptor.ApplicationResourcePrivileges> application = isSuperuser ?
            Collections.singleton(
                RoleDescriptor.ApplicationResourcePrivileges.builder().application("*").privileges("*").resources("*").build()) :
            Collections.emptySet();
        final Set<String> runAs = isSuperuser ? Collections.singleton("*") : Collections.emptySet();
        return new GetUserPrivilegesResponse(cluster, conditionalCluster, indices, application, runAs);
    }

    public static class CustomAuthorizationInfo implements AuthorizationInfo {

        private final String[] roles;
        private final CustomAuthorizationInfo authenticatedAuthzInfo;

        CustomAuthorizationInfo(String[] roles, CustomAuthorizationInfo authenticatedAuthzInfo) {
            this.roles = roles;
            this.authenticatedAuthzInfo = authenticatedAuthzInfo;
        }

        @Override
        public Map<String, Object> asMap() {
            return Collections.singletonMap("roles", roles);
        }

        @Override
        public CustomAuthorizationInfo getAuthenticatedUserAuthorizationInfo() {
            return authenticatedAuthzInfo;
        }
    }

    private boolean isSuperuser(User user) {
        return Arrays.asList(user.roles()).contains("custom_superuser");
    }

    private boolean isSuperuser(AuthorizationInfo authorizationInfo) {
        assert authorizationInfo instanceof CustomAuthorizationInfo;
        return Arrays.asList(((CustomAuthorizationInfo)authorizationInfo).asMap().get("roles")).contains("custom_superuser");
    }
}
