/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.service;

import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.util.List;
import java.util.Objects;

/**
 * Describes how a service account's privileges are determined.
 * Built-in accounts use fixed inline descriptors; managed accounts use named roles.
 */
public sealed interface ServiceAccountAuthorization permits ServiceAccountAuthorization.Fixed, ServiceAccountAuthorization.AssignedRoles {

    record Fixed(RoleDescriptor roleDescriptor) implements ServiceAccountAuthorization {
        public Fixed {
            Objects.requireNonNull(roleDescriptor, "role descriptor cannot be null");
        }
    }

    record AssignedRoles(List<String> roleNames) implements ServiceAccountAuthorization {
        public AssignedRoles {
            Objects.requireNonNull(roleNames, "role names cannot be null");
            roleNames = List.copyOf(roleNames);
        }
    }
}
