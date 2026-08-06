/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under
 * one or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.privilege;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ImplicitPrivilegesProvider;
import org.elasticsearch.xpack.core.security.support.Automatons;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * Grants read access to Kibana's workflow execution analytics index when a role
 * grants the corresponding Kibana Workflows application privileges. The emitted
 * index privilege remains scoped by DLS so users only see executions from the
 * spaces where their Kibana role grants execution-read access.
 */
public class KibanaWorkflowsImplicitPrivilegesProvider implements ImplicitPrivilegesProvider {

    public static final String WORKFLOWS_EXECUTIONS_INDEX = ".workflows-executions";
    public static final String WORKFLOWS_READ_EXECUTION_ACTION = "api:workflowsManagement:readExecution";
    public static final String WORKFLOWS_READ_MANAGED_EXECUTION_ACTION = "api:workflowsManagement:managed:readExecution";

    private static final String KIBANA_APPLICATION_SAMPLE = "kibana-.kibana";
    private static final String GLOBAL_RESOURCE = "*";
    private static final String SPACE_RESOURCE_PREFIX = "space:";

    @Override
    public Collection<RoleDescriptor.IndicesPrivileges> getImplicitIndicesPrivileges(
        RoleDescriptor roleDescriptor,
        Collection<ApplicationPrivilegeDescriptor> storedApplicationPrivileges
    ) {
        final ResourceGrant readExecutions = collectResourcesForAction(
            roleDescriptor,
            storedApplicationPrivileges,
            WORKFLOWS_READ_EXECUTION_ACTION
        );
        if (readExecutions.isEmpty()) {
            return List.of();
        }

        final ResourceGrant readManagedExecutions = collectResourcesForAction(
            roleDescriptor,
            storedApplicationPrivileges,
            WORKFLOWS_READ_MANAGED_EXECUTION_ACTION
        );
        final String dlsQuery = buildDlsQuery(readExecutions, readExecutions.intersection(readManagedExecutions));

        final RoleDescriptor.IndicesPrivileges.Builder builder = RoleDescriptor.IndicesPrivileges.builder()
            .indices(WORKFLOWS_EXECUTIONS_INDEX)
            .privileges("read");
        if (dlsQuery != null) {
            builder.query(new BytesArray(dlsQuery));
        }
        return List.of(builder.build());
    }

    private static ResourceGrant collectResourcesForAction(
        RoleDescriptor roleDescriptor,
        Collection<ApplicationPrivilegeDescriptor> storedApplicationPrivileges,
        String action
    ) {
        final ResourceGrant grant = new ResourceGrant();
        for (RoleDescriptor.ApplicationResourcePrivileges applicationPrivileges : roleDescriptor.getApplicationPrivileges()) {
            if (isKibanaApplication(applicationPrivileges.getApplication()) == false) {
                continue;
            }
            if (grantsAction(applicationPrivileges, storedApplicationPrivileges, action) == false) {
                continue;
            }
            for (String resource : applicationPrivileges.getResources()) {
                if (GLOBAL_RESOURCE.equals(resource)) {
                    grant.all = true;
                } else if (resource.startsWith(SPACE_RESOURCE_PREFIX)) {
                    grant.spaces.add(resource.substring(SPACE_RESOURCE_PREFIX.length()));
                }
            }
        }
        return grant;
    }

    private static boolean isKibanaApplication(String application) {
        if (application.contains("*")) {
            return Automatons.predicate(application).test(KIBANA_APPLICATION_SAMPLE);
        }
        return application.startsWith("kibana-");
    }

    private static boolean grantsAction(
        RoleDescriptor.ApplicationResourcePrivileges applicationPrivileges,
        Collection<ApplicationPrivilegeDescriptor> storedApplicationPrivileges,
        String action
    ) {
        for (String privilege : applicationPrivileges.getPrivileges()) {
            if (Automatons.predicate(List.of(privilege)).test(action)) {
                return true;
            }
            for (ApplicationPrivilegeDescriptor descriptor : storedApplicationPrivileges) {
                if (privilege.equals(descriptor.getName())
                    && applicationMatches(applicationPrivileges.getApplication(), descriptor.getApplication())
                    && Automatons.predicate(descriptor.getActions()).test(action)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean applicationMatches(String applicationPattern, String application) {
        if (applicationPattern.contains("*")) {
            return Automatons.predicate(applicationPattern).test(application);
        }
        return applicationPattern.equals(application);
    }

    private static String buildDlsQuery(ResourceGrant readExecutions, ResourceGrant readManagedExecutions) {
        if (readExecutions.all && readManagedExecutions.all) {
            return null;
        }

        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.startObject("bool");
            builder.startArray("should");

            if (readExecutions.all) {
                writeUnmanagedExecutionsClause(builder);
            } else {
                writeUnmanagedExecutionsClause(builder, readExecutions.spaces);
            }

            if (readManagedExecutions.all == false && readManagedExecutions.spaces.isEmpty() == false) {
                writeSpacesClause(builder, readManagedExecutions.spaces);
            }

            builder.endArray();
            builder.field("minimum_should_match", 1);
            builder.endObject();
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new IllegalStateException("failed to build workflow execution DLS query", e);
        }
    }

    private static void writeUnmanagedExecutionsClause(XContentBuilder builder) throws IOException {
        builder.startObject();
        builder.startObject("bool");
        writeManagedMustNotClause(builder);
        builder.endObject();
        builder.endObject();
    }

    private static void writeUnmanagedExecutionsClause(XContentBuilder builder, Set<String> spaces) throws IOException {
        if (spaces.isEmpty()) {
            return;
        }
        builder.startObject();
        builder.startObject("bool");
        builder.startArray("filter");
        writeSpacesClause(builder, spaces);
        builder.endArray();
        writeManagedMustNotClause(builder);
        builder.endObject();
        builder.endObject();
    }

    private static void writeManagedMustNotClause(XContentBuilder builder) throws IOException {
        builder.startArray("must_not");
        builder.startObject();
        builder.startObject("term");
        builder.field("managed", true);
        builder.endObject();
        builder.endObject();
        builder.endArray();
    }

    private static void writeSpacesClause(XContentBuilder builder, Set<String> spaces) throws IOException {
        builder.startObject();
        builder.startObject("terms");
        builder.array("spaceId", spaces.toArray(Strings.EMPTY_ARRAY));
        builder.endObject();
        builder.endObject();
    }

    private static class ResourceGrant {
        private boolean all;
        private final TreeSet<String> spaces = new TreeSet<>();

        private boolean isEmpty() {
            return all == false && spaces.isEmpty();
        }

        private ResourceGrant intersection(ResourceGrant other) {
            final ResourceGrant result = new ResourceGrant();
            if (all) {
                result.all = other.all;
                result.spaces.addAll(other.spaces);
            } else if (other.all) {
                result.spaces.addAll(spaces);
            } else {
                result.spaces.addAll(spaces);
                result.spaces.retainAll(other.spaces);
            }
            return result;
        }
    }
}
