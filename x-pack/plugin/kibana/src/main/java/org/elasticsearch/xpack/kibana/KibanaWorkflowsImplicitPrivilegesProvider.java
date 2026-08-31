/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.kibana;

import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilege;
import org.elasticsearch.xpack.core.security.authz.privilege.ImplicitPrivilegesProvider;
import org.elasticsearch.xpack.core.security.authz.privilege.ResolvedApplicationPrivilege;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Grants DLS/FLS-scoped read access to workflow execution indices from Kibana application privileges.
 * The base workflow grant excludes {@code managed:true}. The base step grant requires
 * {@code managed:false} so legacy documents fail closed. A managed grant removes these restrictions
 * only in spaces that grant both execution actions.
 */
public class KibanaWorkflowsImplicitPrivilegesProvider implements ImplicitPrivilegesProvider {

    static final String KIBANA_APPLICATION = "kibana-.kibana";

    static final String READ_EXECUTION_ACTION = "api:workflowsManagement:readExecution";
    static final String READ_MANAGED_EXECUTION_ACTION = "api:workflowsManagement:managed:readExecution";

    static final String[] WORKFLOW_EXECUTION_INDICES = { ".workflows-executions*" };
    static final String[] STEP_EXECUTION_INDICES = { ".workflows-step-executions*" };
    static final String[] ALL_EXECUTION_INDICES = { ".workflows-executions*", ".workflows-step-executions*" };

    static final String RESOURCE_PREFIX = "space:";
    static final String ALL_RESOURCES = "*";
    static final String INDEX_READ_PRIVILEGE = "read";
    static final String SPACE_ID_FIELD = "spaceId";
    static final String MANAGED_FIELD = "managed";

    // Keep in sync with the workflow and step execution mappings in Kibana.
    static final String[] GRANTED_FIELDS = {
        "spaceId",
        "id",
        "workflowId",
        "managed",
        "managedBy",
        "originManagedWorkflowId",
        "managedVersion",
        "status",
        "createdAt",
        "isTestRun",
        "stepId",
        "createdBy",
        "executedBy",
        "startedAt",
        "finishedAt",
        "duration",
        "triggeredBy",
        "eventChainDepth",
        "eventChainVisitedWorkflowIds",
        "dispatchEventId",
        "concurrencyGroupKey",
        "version",
        "stepType",
        "workflowRunId",
        "usage.*",
        "stepUsage.*",
        "hitl.*" };

    @Override
    public Collection<RoleDescriptor.IndicesPrivileges> getImplicitIndicesPrivileges(
        Collection<ResolvedApplicationPrivilege> applicationPrivileges
    ) {
        final Set<String> readResources = new HashSet<>();
        final Set<String> readManagedResources = new HashSet<>();

        for (ResolvedApplicationPrivilege resolved : applicationPrivileges) {
            final ApplicationPrivilege privilege = resolved.privilege();
            if (applicationMatchesKibana(privilege.getApplication()) == false) {
                continue;
            }
            if (privilege.predicate().test(READ_EXECUTION_ACTION)) {
                readResources.addAll(resolved.resources());
            }
            if (privilege.predicate().test(READ_MANAGED_EXECUTION_ACTION)) {
                readManagedResources.addAll(resolved.resources());
            }
        }

        if (readResources.isEmpty()) {
            return List.of();
        }

        final boolean readAllSpaces = readResources.contains(ALL_RESOURCES);
        final Set<String> readSpaceIds = extractSpaceIds(readResources);
        if (readAllSpaces == false && readSpaceIds.isEmpty()) {
            return List.of();
        }

        final List<RoleDescriptor.IndicesPrivileges> result = new ArrayList<>(3);

        result.add(buildWorkflowExecutionGrant(readAllSpaces, readSpaceIds));
        result.add(buildStepExecutionGrant(readAllSpaces, readSpaceIds));

        final Set<String> managedResources = intersection(readResources, readManagedResources);
        final boolean managedAllSpaces = managedResources.contains(ALL_RESOURCES);
        final Set<String> managedSpaceIds = extractSpaceIds(managedResources);
        if (managedAllSpaces || managedSpaceIds.isEmpty() == false) {
            result.add(buildGrant2(managedAllSpaces, managedSpaceIds));
        }

        return result;
    }

    private static RoleDescriptor.IndicesPrivileges buildWorkflowExecutionGrant(boolean allSpaces, Set<String> spaceIds) {
        return RoleDescriptor.IndicesPrivileges.builder()
            .indices(WORKFLOW_EXECUTION_INDICES)
            .privileges(INDEX_READ_PRIVILEGE)
            .query(buildWorkflowExecutionDlsQuery(allSpaces, spaceIds))
            .grantedFields(GRANTED_FIELDS)
            .build();
    }

    private static RoleDescriptor.IndicesPrivileges buildStepExecutionGrant(boolean allSpaces, Set<String> spaceIds) {
        return RoleDescriptor.IndicesPrivileges.builder()
            .indices(STEP_EXECUTION_INDICES)
            .privileges(INDEX_READ_PRIVILEGE)
            .query(buildStepExecutionDlsQuery(allSpaces, spaceIds))
            .grantedFields(GRANTED_FIELDS)
            .build();
    }

    private static RoleDescriptor.IndicesPrivileges buildGrant2(boolean allSpaces, Set<String> spaceIds) {
        return RoleDescriptor.IndicesPrivileges.builder()
            .indices(ALL_EXECUTION_INDICES)
            .privileges(INDEX_READ_PRIVILEGE)
            .query(buildGrant2DlsQuery(allSpaces, spaceIds))
            .grantedFields(GRANTED_FIELDS)
            .build();
    }

    private static Set<String> extractSpaceIds(Set<String> resources) {
        return resources.stream()
            .filter(resource -> resource.startsWith(RESOURCE_PREFIX))
            .map(resource -> resource.substring(RESOURCE_PREFIX.length()))
            .collect(Collectors.toSet());
    }

    private static Set<String> intersection(Set<String> a, Set<String> b) {
        if (a.contains(ALL_RESOURCES) && b.contains(ALL_RESOURCES)) {
            return Set.of(ALL_RESOURCES);
        }
        if (a.contains(ALL_RESOURCES)) {
            return new HashSet<>(b);
        }
        if (b.contains(ALL_RESOURCES)) {
            return new HashSet<>(a);
        }
        final Set<String> result = new HashSet<>(a);
        result.retainAll(b);
        return result;
    }

    static String buildWorkflowExecutionDlsQuery(boolean allSpaces, Set<String> spaceIds) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.startObject("bool");

            if (allSpaces == false) {
                builder.startArray("filter");
                builder.startObject();
                builder.startObject("terms");
                builder.array(SPACE_ID_FIELD, spaceIds.toArray(new String[0]));
                builder.endObject();
                builder.endObject();
                builder.endArray();
            }

            builder.startArray("must_not");
            builder.startObject();
            builder.startObject("term");
            builder.field(MANAGED_FIELD, true);
            builder.endObject();
            builder.endObject();
            builder.endArray();

            builder.endObject();
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static String buildStepExecutionDlsQuery(boolean allSpaces, Set<String> spaceIds) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.startObject("bool");
            builder.startArray("filter");
            if (allSpaces == false) {
                builder.startObject();
                builder.startObject("terms");
                builder.array(SPACE_ID_FIELD, spaceIds.toArray(new String[0]));
                builder.endObject();
                builder.endObject();
            }
            builder.startObject();
            builder.startObject("term");
            builder.field(MANAGED_FIELD, false);
            builder.endObject();
            builder.endObject();
            builder.endArray();
            builder.endObject();
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    static String buildGrant2DlsQuery(boolean allSpaces, Set<String> spaceIds) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            if (allSpaces) {
                builder.startObject();
                builder.startObject("match_all");
                builder.endObject();
                builder.endObject();
            } else {
                builder.startObject();
                builder.startObject("terms");
                builder.array(SPACE_ID_FIELD, spaceIds.toArray(new String[0]));
                builder.endObject();
                builder.endObject();
            }
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Whether a resolved privilege's application targets the Kibana application. Resolution
     * expands wildcard application names against the stored privileges, so the value is normally
     * concrete and settled by equality; a residual wildcard (e.g. {@code "kibana-*"} or
     * {@code "*"} with no matching stored descriptor) is matched with an automaton.
     */
    private static boolean applicationMatchesKibana(String application) {
        return application.contains("*")
            ? Automatons.predicate(application).test(KIBANA_APPLICATION)
            : KIBANA_APPLICATION.equals(application);
    }
}
