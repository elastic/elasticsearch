/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.xpack.security.authc.Realms;
import org.elasticsearch.xpack.security.authc.support.mapper.NativeRoleMappingStore;
import org.elasticsearch.xpack.security.authz.store.CompositeRolesStore;
import org.elasticsearch.xpack.security.transport.filter.IPFilter;

/**
 * A wrapper around the services needed to produce usage information for the security feature.
 *
 * This class is temporary until actions can be constructed directly by plugins.
 */
class SecurityUsageServices {
    final Realms realms;
    final CompositeRolesStore rolesStore;
    final NativeRoleMappingStore roleMappingStore;
    final IPFilter ipFilter;

    SecurityUsageServices(Realms realms, CompositeRolesStore rolesStore, NativeRoleMappingStore roleMappingStore, IPFilter ipFilter) {
        this.realms = realms;
        this.rolesStore = rolesStore;
        this.roleMappingStore = roleMappingStore;
        this.ipFilter = ipFilter;
    }
}
