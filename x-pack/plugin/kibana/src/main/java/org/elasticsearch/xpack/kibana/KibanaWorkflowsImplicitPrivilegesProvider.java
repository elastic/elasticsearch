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
 * Implicitly grants DLS/FLS-scoped read access to the workflow execution indices
 * ({@code .workflows-executions*} and {@code .workflows-step-executions*}) for users whose Kibana
 * roles include the {@code workflowsManagement} feature privilege.
 *
 * <h2>Two-grant conjunction</h2>
 * <p>
 * Two grants are always emitted together — never one without the other.
 * <ul>
 *   <li><b>Grant 1</b> — base execution read, restricted to user-authored (non-managed) documents.
 *       DLS: {@code bool.filter: terms spaceId ∈ R, must_not: term managed:true}.
 *   <li><b>Grant 2</b> — managed execution read, for spaces where the role also holds
 *       {@link #READ_MANAGED_EXECUTION_ACTION}. DLS: {@code bool.filter: terms spaceId ∈ R∩M} with
 *       no {@code managed} clause, so both managed and user-authored documents are visible in those
 *       spaces.
 * </ul>
 *
 * <h2>Why {@code must_not managed:true}</h2>
 * <p>
 * The {@code managed} field is absent on the majority of existing execution documents (it is
 * stamped as {@code Boolean(existingWorkflow?.managed)}, which is {@code undefined} for
 * user-authored workflows). A {@code term managed:false} filter would hide those documents.
 * {@code must_not managed:true} correctly matches both {@code false} and field-absent cases.
 *
 * <h2>Why {@code .*} for object and nested fields</h2>
 * <p>
 * {@link org.elasticsearch.xpack.core.security.authz.permission.FieldPermissions} builds the FLS
 * automaton with {@link Automatons#patterns}, which performs no implicit subfield expansion. A bare
 * {@code usage} grant matches only a scalar field named {@code usage} — the
 * {@code FieldSubsetReader} walker drops the whole object when the {@code '.'}-step fails.
 * Object-typed fields ({@code usage}, {@code stepUsage}, {@code hitl}) must therefore be listed
 * as {@code usage.*} etc.
 *
 * <h2>Action strings</h2>
 * <p>
 * Action string derivation (Kibana side):
 * {@code WORKFLOWS_MANAGEMENT_FEATURE_ID = 'workflowsManagement'} →
 * {@code WorkflowsManagementApiActions.readExecution = 'workflowsManagement:readExecution'} →
 * {@code api:[...]} declaration in {@code features.ts} →
 * {@code feature_privilege_builder/api.ts} calls {@code actions.api.get(operation)} →
 * final string {@code api:workflowsManagement:readExecution}.
 */
public class KibanaWorkflowsImplicitPrivilegesProvider implements ImplicitPrivilegesProvider {

    static final String KIBANA_APPLICATION = "kibana-.kibana";

    // Action strings constructed by Kibana's ApiActions.get(operation): `api:${operation}`.
    // Source: kbn-workflows/common/privileges.ts + feature_privilege_builder/api.ts.
    static final String READ_EXECUTION_ACTION = "api:workflowsManagement:readExecution";
    static final String READ_MANAGED_EXECUTION_ACTION = "api:workflowsManagement:managed:readExecution";

    // Both execution indices, wildcarded for future shard rollovers.
    // Keep in sync with Kibana's WORKFLOWS_EXECUTIONS_INDEX / WORKFLOWS_STEP_EXECUTIONS_INDEX
    // in workflows_execution_engine/common/mappings.ts — those names and this pattern are a
    // cross-repo contract.
    static final String[] WORKFLOWS_EXECUTION_INDICES = { ".workflows-executions*", ".workflows-step-executions*" };

    static final String RESOURCE_PREFIX = "space:";
    static final String ALL_RESOURCES = "*";
    static final String INDEX_READ_PRIVILEGE = "read";
    static final String SPACE_ID_FIELD = "spaceId";
    static final String MANAGED_FIELD = "managed";

    // FLS allowlist: all mapped, non-disabled fields from both execution indices.
    // workflowDefinition is deliberately excluded (mapped `enabled:false` — not user-readable).
    // Object/nested fields use the `.*` form: FieldSubsetReader drops the whole object when the
    // automaton cannot step through '.', so a bare field name is insufficient.
    // Keep in sync with WORKFLOWS_EXECUTIONS_INDEX_MAPPINGS and WORKFLOWS_STEP_EXECUTIONS_INDEX_MAPPINGS
    // (workflows_execution_engine/common/mappings.ts). A sync test on the Kibana side enforces this.
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
        // R = spaces where the role holds READ_EXECUTION_ACTION
        // M = spaces where the role holds READ_MANAGED_EXECUTION_ACTION
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
            // Without the base action there is nothing to grant.
            return List.of();
        }

        final List<RoleDescriptor.IndicesPrivileges> result = new ArrayList<>(2);

        // Grant 1: user-authored executions — base read action only, managed docs excluded.
        result.add(buildGrant1(readResources));

        // Grant 2: managed executions — intersection of base and managed resources.
        // Emitted only when non-empty; never emit a grant with an empty space-id set
        // (no DLS query would make it ALLOW_ALL and defeat grant 1).
        final Set<String> managedResources = intersection(readResources, readManagedResources);
        if (managedResources.isEmpty() == false) {
            result.add(buildGrant2(managedResources));
        }

        return result;
    }

    private static RoleDescriptor.IndicesPrivileges buildGrant1(Set<String> readResources) {
        return RoleDescriptor.IndicesPrivileges.builder()
            .indices(WORKFLOWS_EXECUTION_INDICES)
            .privileges(INDEX_READ_PRIVILEGE)
            .query(buildGrant1DlsQuery(readResources))
            .grantedFields(GRANTED_FIELDS)
            .build();
    }

    private static RoleDescriptor.IndicesPrivileges buildGrant2(Set<String> managedResources) {
        return RoleDescriptor.IndicesPrivileges.builder()
            .indices(WORKFLOWS_EXECUTION_INDICES)
            .privileges(INDEX_READ_PRIVILEGE)
            .query(buildGrant2DlsQuery(managedResources))
            .grantedFields(GRANTED_FIELDS)
            .build();
    }

    /**
     * Returns the intersection of {@code a} and {@code b}, with wildcard ({@code "*"}) semantics:
     * if either set contains {@code "*"}, the intersection is the other set; if both contain
     * {@code "*"}, the result is {@code {"*"}}.
     */
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

    /**
     * Grant 1 DLS query: non-managed executions in the granted spaces.
     * <pre>{@code
     * { "bool": {
     *     "filter":    [ { "terms": { "spaceId": [...] } } ],   // omitted for wildcard resource
     *     "must_not":  [ { "term":  { "managed": true  } } ]
     * }}
     * }</pre>
     * {@code must_not managed:true} (not {@code term managed:false}) is required: the field is
     * absent on most existing documents, and a term-false filter would exclude them.
     */
    static String buildGrant1DlsQuery(Set<String> resources) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.startObject("bool");

            if (resources.contains(ALL_RESOURCES) == false) {
                final Set<String> spaceIds = resources.stream()
                    .filter(r -> r.startsWith(RESOURCE_PREFIX))
                    .map(r -> r.substring(RESOURCE_PREFIX.length()))
                    .collect(Collectors.toSet());
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

    /**
     * Grant 2 DLS query: all executions (managed and user-authored) in the granted spaces.
     * <pre>{@code
     * { "terms": { "spaceId": [...] } }   // omitted for wildcard resource → match-all
     * }</pre>
     * No {@code managed} clause: the managed privilege is the union of all executions, not just
     * managed ones.
     */
    static String buildGrant2DlsQuery(Set<String> resources) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            if (resources.contains(ALL_RESOURCES)) {
                builder.startObject();
                builder.startObject("match_all");
                builder.endObject();
                builder.endObject();
            } else {
                final Set<String> spaceIds = resources.stream()
                    .filter(r -> r.startsWith(RESOURCE_PREFIX))
                    .map(r -> r.substring(RESOURCE_PREFIX.length()))
                    .collect(Collectors.toSet());
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
