/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.restriction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class WorkflowResolver {

    private static final Logger logger = LogManager.getLogger(WorkflowResolver.class);

    /**
     * Allows access to Search Application query REST API.
     */
    public static final Workflow SEARCH_APPLICATION_QUERY_WORKFLOW = Workflow.builder()
        .name("search_application_query")
        .addAllowedRestHandlers("search_application_query_action")
        .build();

    private static final Set<Workflow> ALL_WORKFLOWS = Set.of(SEARCH_APPLICATION_QUERY_WORKFLOW);

    private static final Map<String, Workflow> WORKFLOW_LOOKUP_MAP_BY_REST_HANDLER;
    private static final Map<String, Workflow> WORKFLOW_LOOKUP_MAP_BY_NAME;
    static {
        final Map<String, Workflow> lookupByName = new HashMap<>(ALL_WORKFLOWS.size());
        final Map<String, Workflow> lookupByRestHandler = new HashMap<>();
        for (final Workflow workflow : ALL_WORKFLOWS) {
            assert lookupByName.containsKey(workflow.name()) == false
                : "Workflow names must be unique. Workflow with the name [" + workflow.name() + "] has been defined more than once.";
            lookupByName.put(workflow.name(), workflow);
            for (String restHandler : workflow.allowedRestHandlers()) {
                assert lookupByRestHandler.containsKey(restHandler) == false
                    : "REST handler must belong to a single workflow. "
                        + "REST handler with the name ["
                        + restHandler
                        + "] has been assigned to more than one workflow.";
                lookupByRestHandler.put(restHandler, workflow);
            }
        }
        WORKFLOW_LOOKUP_MAP_BY_NAME = Map.copyOf(lookupByName);
        WORKFLOW_LOOKUP_MAP_BY_REST_HANDLER = Map.copyOf(lookupByRestHandler);
    }

    /**
     * Returns all workflows.
     */
    public static Set<Workflow> allWorkflows() {
        return ALL_WORKFLOWS;
    }

    /**
     * Resolves a {@link Workflow} from a given {@code name}.
     * Workflow names are unique, hence there can be only one {@link Workflow} for a given name.
     *
     * @param name a workflow name
     * @return a resolved {@link Workflow}
     * @throws IllegalArgumentException if a workflow with the given {@code name} does not exist
     */
    public static Workflow resolveWorkflowByName(final String name) {
        final String filteredName = Objects.requireNonNull(name).toLowerCase(Locale.ROOT).trim();

        final Workflow resolvedWorkflow = WORKFLOW_LOOKUP_MAP_BY_NAME.get(filteredName);
        if (resolvedWorkflow != null) {
            return resolvedWorkflow;
        }

        final String errorMessage = "Unknown workflow ["
            + name
            + "]. A workflow must be "
            + "one of the predefined workflow names ["
            + Strings.collectionToCommaDelimitedString(WORKFLOW_LOOKUP_MAP_BY_NAME.keySet())
            + "].";
        logger.debug(errorMessage);
        throw new IllegalArgumentException(errorMessage);
    }

    /**
     * Resolves a workflow based on the given REST handler name.
     *
     * @param restHandler a unique REST handler name
     * @return a {@link Workflow} to which the given REST handler belongs
     *         or {@code null} if the REST handler does not belong to any workflows
     */
    public static Workflow resolveWorkflowForRestHandler(String restHandler) {
        return WORKFLOW_LOOKUP_MAP_BY_REST_HANDLER.get(restHandler);
    }

    private WorkflowResolver() {
        throw new IllegalAccessError("not permitted");
    }
}
