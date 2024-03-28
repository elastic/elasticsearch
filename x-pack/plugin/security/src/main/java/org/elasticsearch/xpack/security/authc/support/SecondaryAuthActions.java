/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc.support;

import java.util.Set;

/**
 * Actions that are only available when a secondary authenticator is present. The user represented by the secondary authenticator will
 * be used as the user for these actions. Secondary authorization requires both the primary and secondary authentication passes.
 * Any actions returned here will ensure that the RBAC authorization represents the secondary user.
 * If these actions are called without a secondary authenticated user, an exception will be thrown.
 * {@see SecondaryAuthenticator}
 */
@FunctionalInterface
public interface SecondaryAuthActions {
    Set<String> get();
}
