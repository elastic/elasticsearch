/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.xpack.security.authz.permission.Role;

import java.util.Map;

/**
 * An interface for looking up a role given a string role name
 */
public interface RolesStore {

    Role role(String role);

    Map<String, Object> usageStats();
}
