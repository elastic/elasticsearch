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
    private final Map<String, String> roles;

    public ServiceProviderPrivileges(String applicationName, String resource, Map<String, String> roles) {
        this.applicationName = Objects.requireNonNull(applicationName, "Application name cannot be null");
        this.resource = Objects.requireNonNull(resource, "Resource cannot be null");
        this.roles = Map.copyOf(roles);
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
     * Returns a mapping from "role name" (key) to "{@link ApplicationPrivilegeDescriptor#getActions() action name}" (value)
     * that represents the roles that should be exposed to this Service Provider.
     * The "role name" (but not the action name) will be provided to the service provider.
     * These roles have no semantic meaning within the IdP, they are simply metadata that we pass to the Service Provider. They may not
     * have any relationship to the roles that the user has in this cluster, and the service provider may refer to them using a different
     * terminology (e.g. "groups").
     * The actions will be resolved as application privileges from the IdP's security cluster.
     */
    public Map<String, String> getRoleActions() {
        return roles;
    }
}
