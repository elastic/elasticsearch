/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security;

import org.elasticsearch.xpack.security.authz.RoleDescriptor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * SPI interface to any plugins that want to provide custom extensions to aid the security module in functioning without
 * needing to explicitly know about the behavior of the implementing plugin.
 */
public interface SecurityExtension {

    /**
     * Gets a set of reserved roles, consisting of the role name and the descriptor.
     */
    default Map<String, RoleDescriptor> getReservedRoles() {
        return Collections.emptyMap();
    }
}
