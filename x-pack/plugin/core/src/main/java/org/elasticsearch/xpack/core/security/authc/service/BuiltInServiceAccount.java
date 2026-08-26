/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.service;

import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

/**
 * A service account that ships with Elasticsearch in the reserved {@link ServiceAccountSettings#BUILTIN_NAMESPACE}
 * namespace. Its privileges are fixed at compile time, so it is authorized from the descriptor below rather than from
 * roles looked up in cluster state.
 */
public interface BuiltInServiceAccount extends ServiceAccount {

    /**
     * The account's privileges, named after {@link #id()}'s principal.
     */
    RoleDescriptor roleDescriptor();
}
