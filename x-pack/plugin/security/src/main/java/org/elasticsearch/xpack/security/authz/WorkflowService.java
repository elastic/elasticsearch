/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.AuthorizationEngine.AuthorizationInfo;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.restriction.Workflow;
import org.elasticsearch.xpack.core.security.authz.restriction.WorkflowResolver;

import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.core.security.support.Exceptions.authorizationError;

public class WorkflowService {

    private static final Logger logger = LogManager.getLogger(WorkflowService.class);

    private static final String WORKFLOW_HEADER = "_xpack_security_workflow";

    public Workflow resolveWorkflowAndStoreInThreadContext(RestHandler restHandler, ThreadContext threadContext) {
        Workflow workflow = resolveWorkflow(restHandler);
        if (workflow != null) {
            assert threadContext.getHeader(WORKFLOW_HEADER) == null
                : "thread context should not have workflow set. existing workflow [" + threadContext.getHeader(WORKFLOW_HEADER) + "]";
            threadContext.putHeader(WORKFLOW_HEADER, workflow.name());
        }
        return workflow;
    }

    public Workflow readWorkflowFromThreadContext(ThreadContext threadContext) {
        String workflowName = threadContext.getHeader(WORKFLOW_HEADER);
        if (workflowName == null) {
            return null;
        }
        return WorkflowResolver.resolveWorkflowByName(workflowName);
    }

    /**
     * Checks the workflows restriction from the provided authorization information
     * against the originating workflow that is stored in the thread context.
     *
     * @throws ElasticsearchSecurityException if access is denied due to workflows restriction
     */
    public void checkWorkflowRestriction(
        final Authentication authentication,
        final AuthorizationInfo authzInfo,
        final ThreadContext threadContext
    ) throws ElasticsearchSecurityException {
        if (authzInfo instanceof RBACEngine.RBACAuthorizationInfo rbacAuthzInfo) {
            final Workflow workflow = readWorkflowFromThreadContext(threadContext);
            final Role role = rbacAuthzInfo.getRole();
            if (false == role.checkWorkflowRestriction(workflow)) {
                logger.trace(
                    () -> format(
                        "accessed workflow [%s] but role is restricted to %s workflow(s)",
                        workflow == null ? "<undefined>" : workflow.name(),
                        getWorkflowNames(role)
                    )
                );
                throw accessRestrictedByWorkflows(role);
            }
            if (authentication.isRunAs()) {
                final Role authenticatedRole = rbacAuthzInfo.getAuthenticatedUserAuthorizationInfo().getRole();
                if (false == authenticatedRole.checkWorkflowRestriction(workflow)) {
                    logger.trace(
                        () -> format(
                            "accessed workflow [%s] but run-as role is restricted to %s workflow(s)",
                            workflow == null ? "<undefined>" : workflow.name(),
                            getWorkflowNames(role)
                        )
                    );
                    throw accessRestrictedByWorkflows(authenticatedRole);
                }
            }
        }
    }

    private static ElasticsearchSecurityException accessRestrictedByWorkflows(Role role) {
        String message = "Access is restricted only to " + getWorkflowNames(role) + " workflow(s).";
        return authorizationError(message);
    }

    private static Set<String> getWorkflowNames(Role role) {
        return role.workflowsRestriction().workflows().stream().map(Workflow::name).collect(Collectors.toSet());
    }

    private Workflow resolveWorkflow(RestHandler restHandler) {
        final String restHandlerName = resolveRestHandlerName(restHandler);
        if (restHandlerName == null) {
            logger.trace(() -> format("unable to resolve name of REST handler [%s]", restHandler.getClass()));
            return null;
        }
        Workflow workflow = WorkflowResolver.resolveWorkflowForRestHandler(restHandlerName);
        if (workflow != null) {
            logger.trace(() -> format("resolved workflow [%s] for REST handler [%s]", workflow.name(), restHandlerName));
        }
        return workflow;
    }

    private static String resolveRestHandlerName(RestHandler restHandler) {
        if (restHandler instanceof BaseRestHandler baseRestHandler) {
            return baseRestHandler.getName();
        } else {
            return null;
        }
    }

}
