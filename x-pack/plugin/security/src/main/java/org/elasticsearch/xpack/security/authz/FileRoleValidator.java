/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

/**
 * Provides a check which will be applied to roles in the file-based roles store.
 */
@FunctionalInterface
public interface FileRoleValidator {
    ActionRequestValidationException validatePredefinedRole(RoleDescriptor roleDescriptor);

    /**
     * The default file role validator used in stateful Elasticsearch, a no-op.
     */
    class Default implements FileRoleValidator {
        @Override
        public ActionRequestValidationException validatePredefinedRole(RoleDescriptor roleDescriptor) {
            return null;
        }
    }
}
