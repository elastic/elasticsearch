/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.store;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.hash.MessageDigests;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * RoleReference is a handle to the actual role definitions (role descriptors).
 * It has different sub-types depending on how the role descriptors should be resolved.
 */
public interface RoleReference {

    /**
     * Unique ID of the instance. Instances that have equal ID means they are equivalent
     * in terms of authorization.
     * It is currently used as cache key for role caching purpose.
     * Callers can use this value to determine whether it should skip
     * resolving the role descriptors and subsequently building the role.
     */
    RoleKey id();

    /**
     * Resolve concrete role descriptors for the roleReference.
     */
    void resolve(RoleReferenceResolver resolver, ActionListener<RolesRetrievalResult> listener);

    /**
     * Referencing a collection of role descriptors by their names
     */
    final class NamedRoleReference implements RoleReference {
        private final String[] roleNames;
        private final Set<String> workflows;

        public NamedRoleReference(String[] roleNames) {
            this(roleNames, Set.of());
        }

        public NamedRoleReference(String[] roleNames, Set<String> workflows) {
            this.roleNames = roleNames;
            this.workflows = workflows;
        }

        public String[] getRoleNames() {
            return roleNames;
        }

        public Set<String> getWorkflows() {
            return workflows;
        }

        @Override
        public RoleKey id() {
            if (roleNames.length == 0) {
                return RoleKey.ROLE_KEY_EMPTY;
            } else {
                final Set<String> distinctRoles = new HashSet<>(List.of(roleNames));
                if (distinctRoles.size() == 1 && distinctRoles.contains(ReservedRolesStore.SUPERUSER_ROLE_DESCRIPTOR.getName())) {
                    return RoleKey.ROLE_KEY_SUPERUSER;
                } else {
                    return new RoleKey(Set.copyOf(distinctRoles), RoleKey.ROLES_STORE_SOURCE, workflows);
                }
            }
        }

        @Override
        public void resolve(RoleReferenceResolver resolver, ActionListener<RolesRetrievalResult> listener) {
            resolver.resolveNamedRoleReference(this, listener);
        }
    }

    /**
     * Referencing API Key role descriptors. Can be either the assigned (key) role descriptors or the limited-by (owner's) role descriptors
     */
    final class ApiKeyRoleReference implements RoleReference {

        private final String apiKeyId;
        private final BytesReference roleDescriptorsBytes;
        private final ApiKeyRoleType roleType;
        private final Set<String> workflows;
        private RoleKey id = null;

        public ApiKeyRoleReference(String apiKeyId, BytesReference roleDescriptorsBytes, ApiKeyRoleType roleType) {
            this(apiKeyId, roleDescriptorsBytes, roleType, Set.of());
        }

        public ApiKeyRoleReference(String apiKeyId, BytesReference roleDescriptorsBytes, ApiKeyRoleType roleType, Set<String> workflows) {
            this.apiKeyId = apiKeyId;
            this.roleDescriptorsBytes = roleDescriptorsBytes;
            this.roleType = roleType;
            this.workflows = workflows;
        }

        @Override
        public RoleKey id() {
            // Hashing can be expensive. memorize the result in case the method is called multiple times.
            if (id == null) {
                final String roleDescriptorsHash = MessageDigests.toHexString(
                    MessageDigests.digest(roleDescriptorsBytes, MessageDigests.sha256())
                );
                id = new RoleKey(Set.of("apikey:" + roleDescriptorsHash), "apikey_" + roleType, workflows);
            }
            return id;
        }

        @Override
        public void resolve(RoleReferenceResolver resolver, ActionListener<RolesRetrievalResult> listener) {
            resolver.resolveApiKeyRoleReference(this, listener);
        }

        public String getApiKeyId() {
            return apiKeyId;
        }

        public BytesReference getRoleDescriptorsBytes() {
            return roleDescriptorsBytes;
        }

        public ApiKeyRoleType getRoleType() {
            return roleType;
        }

        public Set<String> getWorkflows() {
            return workflows;
        }
    }

    final class CrossClusterAccessRoleReference implements RoleReference {

        private final CrossClusterAccessSubjectInfo.RoleDescriptorsBytes roleDescriptorsBytes;
        private RoleKey id = null;
        private final String userPrincipal;
        private final Set<String> workflows;

        public CrossClusterAccessRoleReference(
            String userPrincipal,
            CrossClusterAccessSubjectInfo.RoleDescriptorsBytes roleDescriptorsBytes
        ) {
            this(userPrincipal, roleDescriptorsBytes, Set.of());
        }

        public CrossClusterAccessRoleReference(
            String userPrincipal,
            CrossClusterAccessSubjectInfo.RoleDescriptorsBytes roleDescriptorsBytes,
            Set<String> workflows
        ) {
            this.userPrincipal = userPrincipal;
            this.roleDescriptorsBytes = roleDescriptorsBytes;
            this.workflows = workflows;
        }

        @Override
        public RoleKey id() {
            // Hashing can be expensive. memorize the result in case the method is called multiple times.
            if (id == null) {
                final String roleDescriptorsHash = roleDescriptorsBytes.digest();
                id = new RoleKey(Set.of("cross_cluster_access:" + roleDescriptorsHash), "cross_cluster_access", workflows);
            }
            return id;
        }

        @Override
        public void resolve(RoleReferenceResolver resolver, ActionListener<RolesRetrievalResult> listener) {
            resolver.resolveCrossClusterAccessRoleReference(this, listener);
        }

        public String getUserPrincipal() {
            return userPrincipal;
        }

        public CrossClusterAccessSubjectInfo.RoleDescriptorsBytes getRoleDescriptorsBytes() {
            return roleDescriptorsBytes;
        }

        public Set<String> getWorkflows() {
            return workflows;
        }
    }

    final class FixedRoleReference implements RoleReference {

        private final RoleDescriptor roleDescriptor;
        private final String source;
        private final Set<String> workflows;

        public FixedRoleReference(RoleDescriptor roleDescriptor, String source) {
            this(roleDescriptor, source, Set.of());
        }

        public FixedRoleReference(RoleDescriptor roleDescriptor, String source, Set<String> workflows) {
            this.roleDescriptor = roleDescriptor;
            this.source = source;
            this.workflows = workflows;
        }

        @Override
        public RoleKey id() {
            return new RoleKey(Set.of(roleDescriptor.getName()), source, workflows);
        }

        @Override
        public void resolve(RoleReferenceResolver resolver, ActionListener<RolesRetrievalResult> listener) {
            final RolesRetrievalResult rolesRetrievalResult = new RolesRetrievalResult();
            rolesRetrievalResult.addDescriptors(Set.of(roleDescriptor));
            listener.onResponse(rolesRetrievalResult);
        }
    }

    /**
     * Same as {@link ApiKeyRoleReference} but for BWC purpose (prior to v7.9.0)
     */
    final class BwcApiKeyRoleReference implements RoleReference {
        private final String apiKeyId;
        private final Map<String, Object> roleDescriptorsMap;
        private final ApiKeyRoleType roleType;
        private final Set<String> workflows;

        public BwcApiKeyRoleReference(String apiKeyId, Map<String, Object> roleDescriptorsMap, ApiKeyRoleType roleType) {
            this(apiKeyId, roleDescriptorsMap, roleType, Set.of());
        }

        public BwcApiKeyRoleReference(
            String apiKeyId,
            Map<String, Object> roleDescriptorsMap,
            ApiKeyRoleType roleType,
            Set<String> workflows
        ) {
            this.apiKeyId = apiKeyId;
            this.roleDescriptorsMap = roleDescriptorsMap;
            this.roleType = roleType;
            this.workflows = workflows;
        }

        @Override
        public RoleKey id() {
            // Since api key id is unique, it is sufficient and more correct to use it as the names
            return new RoleKey(Set.of(apiKeyId), "bwc_api_key_" + roleType, workflows);
        }

        @Override
        public void resolve(RoleReferenceResolver resolver, ActionListener<RolesRetrievalResult> listener) {
            resolver.resolveBwcApiKeyRoleReference(this, listener);
        }

        public String getApiKeyId() {
            return apiKeyId;
        }

        public Map<String, Object> getRoleDescriptorsMap() {
            return roleDescriptorsMap;
        }

        public ApiKeyRoleType getRoleType() {
            return roleType;
        }

        public Set<String> getWorkflows() {
            return workflows;
        }
    }

    /**
     * Referencing role descriptors by the service account principal
     */
    final class ServiceAccountRoleReference implements RoleReference {
        private final String principal;
        private final Set<String> workflows;

        public ServiceAccountRoleReference(String principal) {
            this(principal, Set.of());
        }

        public ServiceAccountRoleReference(String principal, Set<String> workflows) {
            this.principal = principal;
            this.workflows = workflows;
        }

        public String getPrincipal() {
            return principal;
        }

        public Set<String> getWorkflows() {
            return workflows;
        }

        @Override
        public RoleKey id() {
            return new RoleKey(Set.of(principal), "service_account", workflows);
        }

        @Override
        public void resolve(RoleReferenceResolver resolver, ActionListener<RolesRetrievalResult> listener) {
            resolver.resolveServiceAccountRoleReference(this, listener);
        }
    }

    /**
     * The type of one set of API key roles.
     */
    enum ApiKeyRoleType {
        /**
         * Roles directly specified by the creator user on API key creation
         */
        ASSIGNED,
        /**
         * Roles captured for the owner user as the upper bound of the assigned roles
         */
        LIMITED_BY;
    }
}
