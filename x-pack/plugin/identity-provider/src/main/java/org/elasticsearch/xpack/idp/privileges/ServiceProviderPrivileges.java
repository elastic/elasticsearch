/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.privileges;

import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;

import java.util.Map;
import java.util.Objects;

public class ServiceProviderPrivileges {

    private final String applicationName;
    private final String resource;
    private final String loginAction;
    private final Map<String, String> groups;

    public ServiceProviderPrivileges(String applicationName, String resource, String loginAction, Map<String, String> groups) {
        this.applicationName = Objects.requireNonNull(applicationName, "Application name cannot be null");
        this.resource = Objects.requireNonNull(resource, "Resource cannot be null");
        this.loginAction = Objects.requireNonNull(loginAction, "Login action cannot be null");
        this.groups = Map.copyOf(groups);
    }

    /**
     * Returns the "application" (see {@link RoleDescriptor.ApplicationResourcePrivileges#getApplication()}) in the IdP's security cluster
     * under which this service provider is defined.
     */
    public String getApplicationName() {
        return applicationName;
    }

    /**
     * Returns the "resource" (see {@link RoleDescriptor.ApplicationResourcePrivileges#getResources()}) that represents this
     * Service Provider in the IdP's security cluster.
     */
    public String getResource() {
        return resource;
    }

    /**
     * Returns the "action" (see {@link ApplicationPrivilegeDescriptor#getActions()}) in the IdP's security cluster that represents
     * the right to SSO (login) to this Service Provider.
     */
    public String getLoginAction() {
        return loginAction;
    }

    /**
     * Returns a mapping from "group name" (key) to "{@link ApplicationPrivilegeDescriptor#getActions() action name}" (value)
     * that represents the groups that should be exposed to this Service Provider.
     * The "group name" (but not the action name) will be provided to the service provider.
     * The actions will be resolved as application privileges from the IdP's security cluster.
     */
    public Map<String, String> getGroupActions() {
        return groups;
    }
}
