/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

// TODO diff package
package org.elasticsearch.xpack.security.operator;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.support.DLSRoleQueryValidator;
import org.elasticsearch.xpack.core.security.support.NativeRealmValidationUtil;
import org.elasticsearch.xpack.core.security.support.Validation;

import java.util.function.Predicate;

import static org.elasticsearch.action.ValidateActions.addValidationError;

// TODO this is gnarly
public interface RoleDescriptorRequestValidatorFactory {

    RoleDescriptorRequestValidator validator(
        Predicate<String> reservedRoleNameChecker,
        NamedXContentRegistry xContentRegistry,
        SecurityContext securityContext
    );

    class Default implements RoleDescriptorRequestValidatorFactory {
        @Override
        public RoleDescriptorRequestValidator validator(
            Predicate<String> reservedRoleNameChecker,
            NamedXContentRegistry xContentRegistry,
            SecurityContext securityContext
        ) {
            return new DefaultValidator(xContentRegistry);
        }
    }

    class DefaultValidator implements RoleDescriptorRequestValidator {
        private final NamedXContentRegistry xContentRegistry;

        public DefaultValidator(NamedXContentRegistry xContentRegistry) {
            this.xContentRegistry = xContentRegistry;
        }

        @Override
        public Exception validate(RoleDescriptor roleDescriptor) {
            ActionRequestValidationException validationException = null;
            // TODO doesn't apply to API keys
            Validation.Error error = NativeRealmValidationUtil.validateRoleName(roleDescriptor.getName(), false);
            if (error != null) {
                validationException = addValidationError(error.toString(), validationException);
            }
            validationException = org.elasticsearch.xpack.core.security.action.role.RoleDescriptorRequestValidator.validate(
                roleDescriptor,
                validationException
            );
            if (validationException != null) {
                return validationException;
            }
            try {
                DLSRoleQueryValidator.validateQueryField(roleDescriptor.getIndicesPrivileges(), xContentRegistry);
            } catch (ElasticsearchException | IllegalArgumentException e) {
                return e;
            }
            return null;
        }
    }
}
