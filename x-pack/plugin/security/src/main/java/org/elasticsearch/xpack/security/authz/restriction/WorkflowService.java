/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.restriction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.security.authz.restriction.Workflow;
import org.elasticsearch.xpack.core.security.authz.restriction.WorkflowResolver;

import static org.elasticsearch.core.Strings.format;

public class WorkflowService {

    private static final Logger logger = LogManager.getLogger(WorkflowService.class);

    public static final String WORKFLOW_THREAD_CONTEXT_KEY = "_xpack_security_workflow";

    private final XPackLicenseState licenseState;

    public WorkflowService(XPackLicenseState licenseState) {
        this.licenseState = licenseState;
    }

    public void resolveWorkflowAndStoreInThreadContext(
        RestHandler restHandler,
        ThreadContext threadContext,
        ActionListener<Workflow> listener
    ) {
        resolveWorkflow(restHandler, ActionListener.wrap(workflow -> {
            maybeStoreWorkflowInThreadContext(workflow, threadContext);
            listener.onResponse(workflow);
        }, listener::onFailure));
    }

    private static void maybeStoreWorkflowInThreadContext(Workflow workflow, ThreadContext threadContext) {
        if (workflow != null) {
            assert threadContext.getHeader(WORKFLOW_THREAD_CONTEXT_KEY) == null
                : "thread context should not have workflow set. "
                    + "existing workflow ["
                    + threadContext.getHeader(WORKFLOW_THREAD_CONTEXT_KEY)
                    + "]";
            threadContext.putHeader(WORKFLOW_THREAD_CONTEXT_KEY, workflow.name());
        }
    }

    private void resolveWorkflow(RestHandler restHandler, ActionListener<Workflow> listener) {
        // TODO: Resolve workflow only if the current license allows it.
        final String restHandlerName = resolveRestHandlerName(restHandler);
        if (restHandlerName == null) {
            logger.trace(() -> format("unable to resolve name of REST handler [%s]", restHandler.getClass()));
            listener.onResponse(null);
            return;
        }
        Workflow workflow = WorkflowResolver.resolveWorkflowForRestHandler(restHandlerName);
        if (workflow != null) {
            logger.trace(() -> format("resolved workflow [%s] for REST handler [%s]", workflow.name(), restHandlerName));
        }
        listener.onResponse(workflow);
    }

    private static String resolveRestHandlerName(RestHandler restHandler) {
        if (restHandler instanceof BaseRestHandler baseRestHandler) {
            return baseRestHandler.getName();
        } else if (restHandler.getConcreteRestHandler() instanceof BaseRestHandler baseRestHandler) {
            return baseRestHandler.getName();
        } else {
            return null;
        }
    }

}
