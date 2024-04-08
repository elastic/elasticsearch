/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz.restriction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestHandler;
import org.elasticsearch.xpack.core.security.authz.restriction.Workflow;
import org.elasticsearch.xpack.core.security.authz.restriction.WorkflowResolver;

import static org.elasticsearch.core.Strings.format;

public class WorkflowService {

    private static final Logger logger = LogManager.getLogger(WorkflowService.class);

    private static final String WORKFLOW_HEADER = "_xpack_security_workflow";

    public static Workflow resolveWorkflowAndStoreInThreadContext(RestHandler restHandler, ThreadContext threadContext) {
        Workflow workflow = resolveWorkflow(restHandler);
        if (workflow != null) {
            assert threadContext.getHeader(WORKFLOW_HEADER) == null
                : "thread context should not have workflow set. existing workflow [" + threadContext.getHeader(WORKFLOW_HEADER) + "]";
            threadContext.putHeader(WORKFLOW_HEADER, workflow.name());
        }
        return workflow;
    }

    public static String readWorkflowFromThreadContext(ThreadContext threadContext) {
        return threadContext.getHeader(WORKFLOW_HEADER);
    }

    private static Workflow resolveWorkflow(RestHandler restHandler) {
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
