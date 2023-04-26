/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.permission;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.core.security.authz.permission.WorkflowPermission.Workflow;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public final class WorkflowPermissionResolver {

    private static final Logger logger = LogManager.getLogger(WorkflowPermissionResolver.class);

    /**
     * Allows access to search application query REST endpoints.
     */
    public static final Workflow SEARCH_APPLICATION = Workflow.builder()
        .name("search_application")
        .endpoints("search_application_query_action")
        .build();

    private static final Map<String, Workflow> WORKFLOW_LOOKUP_MAP;
    static {
        final Set<Workflow> workflows = readStaticFields(WorkflowPermissionResolver.class, Workflow.class);
        if (workflows.isEmpty()) {
            WORKFLOW_LOOKUP_MAP = Map.of();
        } else {
            final Map<String, Workflow> lookup = new HashMap<>(workflows.size());
            for (final Workflow workflow : workflows) {
                assert lookup.containsKey(workflow.name()) == false
                    : "Workflow names must be unique. Workflow with the name [" + workflow.name() + "] has been defined more than once.";
                lookup.put(workflow.name(), workflow);
            }
            WORKFLOW_LOOKUP_MAP = Map.copyOf(lookup);
        }
    }

    /**
     * Returns all workflow names.
     */
    public static Set<String> names() {
        return Set.copyOf(WORKFLOW_LOOKUP_MAP.keySet());
    }

    /**
     * Resolves a {@link Workflow} from a given {@code name}.
     * Workflow names are unique, hence there can be only one {@link Workflow} for a given name.
     *
     * @param name a workflow name
     * @return a resolved {@link Workflow}
     * @throws IllegalArgumentException if a workflow with the given {@code name} does not exist
     */
    public static Workflow resolveWorkflow(final String name) {
        final String filteredName = Objects.requireNonNull(name).toLowerCase(Locale.ROOT).trim();

        final Workflow resolvedWorkflow = WORKFLOW_LOOKUP_MAP.get(filteredName);
        if (resolvedWorkflow != null) {
            return resolvedWorkflow;
        }

        final String errorMessage = "Unknown workflow ["
            + name
            + "]. A workflow must be "
            + "one of the predefined workflow names ["
            + Strings.collectionToCommaDelimitedString(WORKFLOW_LOOKUP_MAP.keySet())
            + "].";
        logger.debug(errorMessage);
        throw new IllegalArgumentException(errorMessage);
    }

    public static WorkflowPermission resolve(Set<String> names) {
        if (names == null || names.isEmpty()) {
            return WorkflowPermission.ALLOW_ALL;
        }
        final Set<Workflow> workflows = new HashSet<>(names.size());
        for (String name : names) {
            workflows.add(WorkflowPermissionResolver.resolveWorkflow(name));
        }
        return new WorkflowPermission(workflows);
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

    private WorkflowPermissionResolver() {
        throw new IllegalAccessError("not permitted");
    }
}
