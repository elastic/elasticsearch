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
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Implicitly grants read access to Kibana Alerting V2 indices ({@code .alert-actions*} and
 * {@code .rule-events*}) for users whose roles include Kibana application privileges with the
 * {@code alerts:read} action.
 * <p>
 * The index patterns are wildcards so ES|QL queries like {@code FROM .rule-events*} or
 * {@code FROM .ds-.rule-events-*} match. The data streams' backing indices and any future
 * sibling indices owned by the Alerting team that share the prefix are covered.
 * <p>
 * When the user has access to specific spaces, a DLS query restricts visibility to documents
 * whose top-level {@code space_id} field matches one of those spaces. When the user has the
 * wildcard resource ({@code *}), full document access is granted with no DLS restriction.
 */
public class KibanaAlertsImplicitPrivilegesProvider implements ImplicitPrivilegesProvider {

    static final String KIBANA_APPLICATION = "kibana-.kibana";
    static final String ALERTS_ACTION = "alerts:read";
    // Index/data-stream names mirror the Kibana-side definitions in:
    // x-pack/platform/plugins/shared/alerting_v2/server/resources/datastreams/alert_actions.ts
    // x-pack/platform/plugins/shared/alerting_v2/server/resources/datastreams/alert_events.ts
    // Keep this list in sync if those definitions change.
    static final String[] ALERTING_V2_INDICES = { ".alert-actions*", ".rule-events*" };
    static final String RESOURCE_PREFIX = "space:";
    static final String ALL_RESOURCES = "*";
    static final String INDEX_READ_PRIVILEGE = "read";
    static final String SPACE_ID_FIELD = "space_id";

    @Override
    public Collection<RoleDescriptor.IndicesPrivileges> getImplicitIndicesPrivileges(
        Collection<ResolvedApplicationPrivilege> applicationPrivileges
    ) {
        Set<String> resources = collectResources(applicationPrivileges);
        if (resources.isEmpty()) {
            return List.of();
        }

        if (resources.contains(ALL_RESOURCES)) {
            return List.of(
                RoleDescriptor.IndicesPrivileges.builder().indices(ALERTING_V2_INDICES).privileges(INDEX_READ_PRIVILEGE).build()
            );
        }

        Set<String> spaceIds = resources.stream()
            .filter(r -> r.startsWith(RESOURCE_PREFIX))
            .map(r -> r.substring(RESOURCE_PREFIX.length()))
            .collect(Collectors.toSet());
        if (spaceIds.isEmpty()) {
            return List.of();
        }

        return List.of(
            RoleDescriptor.IndicesPrivileges.builder()
                .indices(ALERTING_V2_INDICES)
                .privileges(INDEX_READ_PRIVILEGE)
                .query(buildSpaceIdsDlsQuery(spaceIds))
                .build()
        );
    }

    /**
     * Union of resources from every resolved application-privilege grant that targets the Kibana application
     * <i>and</i> authorizes {@link #ALERTS_ACTION}.
     * <p>
     * Each {@link ResolvedApplicationPrivilege} carries a resolved {@link ApplicationPrivilege} whose
     * {@link ApplicationPrivilege#predicate() predicate} already matches every action the grant authorizes — both the
     * actions of any stored privilege the role referenced by name <em>and</em> any raw action patterns written directly
     * under {@code privileges[]} (e.g. {@code "alerts:*"} or {@code "*"}) — so a single {@code predicate().test(...)}
     * settles whether the grant authorizes {@link #ALERTS_ACTION}.
     */
    private static Set<String> collectResources(Collection<ResolvedApplicationPrivilege> applicationPrivileges) {
        Set<String> resources = new HashSet<>();
        for (ResolvedApplicationPrivilege resolved : applicationPrivileges) {
            final ApplicationPrivilege privilege = resolved.privilege();
            if (applicationMatchesKibana(privilege.getApplication()) && privilege.predicate().test(ALERTS_ACTION)) {
                resources.addAll(resolved.resources());
            }
        }
        return resources;
    }

    /**
     * Whether a resolved privilege's application targets the Kibana application. Resolution expands wildcard application
     * names against the stored privileges, so the value is normally concrete and settled by equality; a residual wildcard
     * (e.g. {@code "kibana-*"} or {@code "*"} with no matching stored descriptor) is matched with an automaton.
     */
    private static boolean applicationMatchesKibana(String application) {
        return application.contains("*")
            ? Automatons.predicate(application).test(KIBANA_APPLICATION)
            : KIBANA_APPLICATION.equals(application);
    }

    static String buildSpaceIdsDlsQuery(Set<String> spaceIds) {
        // Hand-rolled rather than QueryBuilders.termsQuery so the stored/surfaced DLS stays free of
        // the default "boost":1.0 that the query builder always serializes.
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.startObject("terms");
            builder.array(SPACE_ID_FIELD, spaceIds.toArray(new String[0]));
            builder.endObject();
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
