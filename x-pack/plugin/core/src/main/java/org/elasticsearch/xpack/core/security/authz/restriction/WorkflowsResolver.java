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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class WorkflowsResolver {

    private static final Logger logger = LogManager.getLogger(WorkflowsResolver.class);

    /**
     * Allows access to Search Application query REST API.
     */
    public static final Workflow SEARCH_APPLICATION_QUERY_WORKFLOW = Workflow.builder()
        .name("search_application_query")
        .addAllowedRestHandlers("search_application_query_action")
        .build();

    /**
     * Allows access to Search Application analytics REST API.
     */
    public static final Workflow SEARCH_APPLICATION_ANALYTICS_WORKFLOW = Workflow.builder()
        .name("search_application_analytics")
        .addAllowedRestHandlers("analytics_post_event_action")
        .build();

    private static final Map<String, Workflow> WORKFLOW_LOOKUP_MAP_BY_REST_HANDLER;
    private static final Map<String, Workflow> WORKFLOW_LOOKUP_MAP_BY_NAME;
    static {
        final Set<Workflow> workflows = readStaticFields(WorkflowsResolver.class, Workflow.class);
        if (workflows.isEmpty()) {
            WORKFLOW_LOOKUP_MAP_BY_NAME = Map.of();
            WORKFLOW_LOOKUP_MAP_BY_REST_HANDLER = Map.of();
        } else {
            final Map<String, Workflow> lookupByName = new HashMap<>(workflows.size());
            final Map<String, Workflow> lookupByRestHandler = new HashMap<>();
            for (final Workflow workflow : workflows) {
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
    }

    /**
     * Returns all workflow names.
     */
    public static Set<String> names() {
        return Set.copyOf(WORKFLOW_LOOKUP_MAP_BY_NAME.keySet());
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

    public static Workflow resolveWorkflowForRestHandler(String restHandler) {
        return WORKFLOW_LOOKUP_MAP_BY_REST_HANDLER.get(restHandler);
    }

    private static <T> Set<T> readStaticFields(Class<?> source, Class<T> fieldType) {
        final Field[] fields = source.getFields();
        final Set<T> result = new HashSet<>();
        for (Field field : fields) {
            if (Modifier.isStatic(field.getModifiers()) && fieldType.isAssignableFrom(field.getType())) {
                try {
                    T value = fieldType.cast(field.get(null));
                    assert value != null
                        : "null value defined for field ["
                            + field.getName()
                            + "] of type ["
                            + fieldType.getCanonicalName()
                            + "] in class ["
                            + source.getCanonicalName()
                            + "]";
                    assert result.contains(value) == false
                        : "field value " + value + " defined more than once in " + source.getCanonicalName();
                    result.add(value);
                } catch (IllegalArgumentException | IllegalAccessException e) {
                    throw new IllegalStateException(
                        "failed to read field ["
                            + field.getName()
                            + "] of type ["
                            + fieldType
                            + "] from ["
                            + source.getCanonicalName()
                            + "]",
                        e
                    );
                }
            }
        }
        return result;
    }

    private WorkflowsResolver() {
        throw new IllegalAccessError("not permitted");
    }
}
