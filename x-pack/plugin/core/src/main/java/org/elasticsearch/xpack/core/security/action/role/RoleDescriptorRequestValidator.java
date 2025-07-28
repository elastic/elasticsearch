/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.action.role;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexComponentSelectorPredicate;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.authz.restriction.WorkflowResolver;
import org.elasticsearch.xpack.core.security.support.MetadataUtils;
import org.elasticsearch.xpack.core.security.support.Validation;

import java.util.Arrays;
import java.util.Set;

import static org.elasticsearch.action.ValidateActions.addValidationError;

public class RoleDescriptorRequestValidator {

    private RoleDescriptorRequestValidator() {}

    public static ActionRequestValidationException validate(RoleDescriptor roleDescriptor) {
        return validate(roleDescriptor, null);
    }

    public static ActionRequestValidationException validate(
        RoleDescriptor roleDescriptor,
        ActionRequestValidationException validationException
    ) {
        if (roleDescriptor.getName() == null) {
            validationException = addValidationError("role name is missing", validationException);
        }
        if (roleDescriptor.getClusterPrivileges() != null) {
            for (String cp : roleDescriptor.getClusterPrivileges()) {
                try {
                    ClusterPrivilegeResolver.resolve(cp);
                } catch (IllegalArgumentException ile) {
                    validationException = addValidationError(ile.getMessage(), validationException);
                }
            }
        }
        if (roleDescriptor.getIndicesPrivileges() != null) {
            for (RoleDescriptor.IndicesPrivileges idp : roleDescriptor.getIndicesPrivileges()) {
                try {
                    IndexPrivilege.resolveBySelectorAccess(Set.of(idp.getPrivileges()));
                } catch (IllegalArgumentException ile) {
                    validationException = addValidationError(ile.getMessage(), validationException);
                }
                for (final String indexName : idp.getIndices()) {
                    validationException = validateIndexNameExpression(indexName, validationException);
                }
            }
        }
        final RoleDescriptor.RemoteIndicesPrivileges[] remoteIndicesPrivileges = roleDescriptor.getRemoteIndicesPrivileges();
        for (RoleDescriptor.RemoteIndicesPrivileges ridp : remoteIndicesPrivileges) {
            if (Arrays.asList(ridp.remoteClusters()).contains("")) {
                validationException = addValidationError("remote index cluster alias cannot be an empty string", validationException);
            }
            try {
                Set<IndexPrivilege> privileges = IndexPrivilege.resolveBySelectorAccess(Set.of(ridp.indicesPrivileges().getPrivileges()));
                if (privileges.stream().anyMatch(p -> p.getSelectorPredicate() == IndexComponentSelectorPredicate.FAILURES)) {
                    validationException = addValidationError(
                        "remote index privileges cannot contain privileges that grant access to the failure store",
                        validationException
                    );
                }
            } catch (IllegalArgumentException ile) {
                validationException = addValidationError(ile.getMessage(), validationException);
            }
            for (String indexName : ridp.indicesPrivileges().getIndices()) {
                validationException = validateIndexNameExpression(indexName, validationException);
            }
        }
        if (roleDescriptor.hasRemoteClusterPermissions()) {
            try {
                roleDescriptor.getRemoteClusterPermissions().validate();
            } catch (IllegalArgumentException e) {
                validationException = addValidationError(e.getMessage(), validationException);
            }
        }
        if (roleDescriptor.getApplicationPrivileges() != null) {
            for (RoleDescriptor.ApplicationResourcePrivileges privilege : roleDescriptor.getApplicationPrivileges()) {
                try {
                    ApplicationPrivilege.validateApplicationNameOrWildcard(privilege.getApplication());
                } catch (IllegalArgumentException e) {
                    validationException = addValidationError(e.getMessage(), validationException);
                }
                for (String privilegeName : privilege.getPrivileges()) {
                    try {
                        ApplicationPrivilege.validatePrivilegeOrActionName(privilegeName);
                    } catch (IllegalArgumentException e) {
                        validationException = addValidationError(e.getMessage(), validationException);
                    }
                }
            }
        }
        if (roleDescriptor.getMetadata() != null && MetadataUtils.containsReservedMetadata(roleDescriptor.getMetadata())) {
            validationException = addValidationError(
                "role descriptor metadata keys may not start with [" + MetadataUtils.RESERVED_PREFIX + "]",
                validationException
            );
        }
        if (roleDescriptor.hasWorkflowsRestriction()) {
            for (String workflowName : roleDescriptor.getRestriction().getWorkflows()) {
                try {
                    WorkflowResolver.resolveWorkflowByName(workflowName);
                } catch (IllegalArgumentException e) {
                    validationException = addValidationError(e.getMessage(), validationException);
                }
            }
        }
        if (roleDescriptor.hasDescription()) {
            Validation.Error error = Validation.Roles.validateRoleDescription(roleDescriptor.getDescription());
            if (error != null) {
                validationException = addValidationError(error.toString(), validationException);
            }
        }
        return validationException;
    }

    private static ActionRequestValidationException validateIndexNameExpression(
        String indexNameExpression,
        ActionRequestValidationException validationException
    ) {
        if (IndexNameExpressionResolver.hasSelectorSuffix(indexNameExpression)) {
            validationException = addValidationError(
                "selectors ["
                    + IndexNameExpressionResolver.SelectorResolver.SELECTOR_SEPARATOR
                    + "] are not allowed in the index name expression ["
                    + indexNameExpression
                    + "]",
                validationException
            );
        }
        return validationException;
    }
}
