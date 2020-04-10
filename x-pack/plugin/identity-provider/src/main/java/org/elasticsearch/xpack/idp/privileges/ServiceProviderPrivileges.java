/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.idp.privileges;

import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;

import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

public class ServiceProviderPrivileges {

    private final String applicationName;
    private final String resource;
    private final Function<String, Set<String>> roleMapping;

    public ServiceProviderPrivileges(String applicationName, String resource, Function<String, Set<String>> roleMapping) {
        this.applicationName = Objects.requireNonNull(applicationName, "Application name cannot be null");
        this.resource = Objects.requireNonNull(resource, "Resource cannot be null");
        this.roleMapping = Objects.requireNonNull(roleMapping, "Role Mapping cannot be null");
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
     * Returns a mapping from "{@link ApplicationPrivilegeDescriptor#getActions() action name}" (input) to "role name" (output)
     * that represents the roles that should be exposed to this Service Provider.
     * The "role name" (but not the action name) will be provided to the service provider.
     * These roles have no semantic meaning within the IdP, they are simply metadata that we pass to the Service Provider. They may not
     * have any relationship to the roles that the user has in this cluster, and the service provider may refer to them using a different
     * terminology (e.g. "groups").
     * The actions will be resolved as application privileges from the IdP's security cluster.
     */
    public Function<String, Set<String>> getRoleMapping() {
        return roleMapping;
    }
}
