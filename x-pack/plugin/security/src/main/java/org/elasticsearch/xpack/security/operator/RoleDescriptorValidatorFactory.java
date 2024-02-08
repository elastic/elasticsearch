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
import org.elasticsearch.xpack.core.security.action.role.RoleDescriptorRequestValidator;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.support.DLSRoleQueryValidator;
import org.elasticsearch.xpack.core.security.support.NativeRealmValidationUtil;
import org.elasticsearch.xpack.core.security.support.Validation;

import java.util.function.Predicate;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public interface RoleDescriptorValidatorFactory {

    RoleDescriptorValidator create(
        NamedXContentRegistry xContentRegistry,
        SecurityContext securityContext,
        Predicate<String> reservedRoleNameChecker
    );

    class Default implements RoleDescriptorValidatorFactory {
        @Override
        public RoleDescriptorValidator create(
            NamedXContentRegistry xContentRegistry,
            SecurityContext securityContext,
            Predicate<String> reservedRoleNameChecker
        ) {
            return new Noop(xContentRegistry);
        }
    }

    class Noop extends RoleDescriptorValidator {

        public Noop(NamedXContentRegistry xContentRegistry) {
            super(xContentRegistry);
        }

        @Override
        public RuntimeException validate(RoleDescriptor roleDescriptor, boolean validateRoleName) {
            return null;
        }
    }

    class RoleDescriptorValidator {
        protected final NamedXContentRegistry xContentRegistry;

        public RoleDescriptorValidator(NamedXContentRegistry xContentRegistry) {
            this.xContentRegistry = xContentRegistry;
        }

        public final RuntimeException validate(RoleDescriptor roleDescriptor) {
            return validate(roleDescriptor, true);
        }

        public RuntimeException validate(RoleDescriptor roleDescriptor, boolean validateRoleName) {
            ActionRequestValidationException validationException = null;
            if (validateRoleName) {
                Validation.Error error = NativeRealmValidationUtil.validateRoleName(roleDescriptor.getName(), false);
                if (error != null) {
                    validationException = addValidationError(error.toString(), validationException);
                }
            }
            validationException = RoleDescriptorRequestValidator.validate(roleDescriptor, validationException);
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
