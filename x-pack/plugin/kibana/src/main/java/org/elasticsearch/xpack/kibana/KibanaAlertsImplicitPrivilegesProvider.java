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
import org.elasticsearch.xpack.core.security.authz.privilege.ApplicationPrivilegeDescriptor;
import org.elasticsearch.xpack.core.security.authz.privilege.ImplicitPrivilegesProvider;
import org.elasticsearch.xpack.core.security.support.StringMatcher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.Collections;
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
        RoleDescriptor roleDescriptor,
        Collection<ApplicationPrivilegeDescriptor> storedApplicationPrivileges
    ) {
        // This provider derives privileges from the role's application privileges, so there is
        // nothing to contribute (and no need to scan the stored privileges) when the role has none.
        RoleDescriptor.ApplicationResourcePrivileges[] applicationPrivileges = roleDescriptor.getApplicationPrivileges();
        if (applicationPrivileges.length == 0) {
            return List.of();
        }

        Set<String> resources = collectResources(applicationPrivileges, storedApplicationPrivileges);
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
     * Union of resources from every role application-privilege block that targets the Kibana
     * application <i>and</i> grants {@link #ALERTS_ACTION}. A block grants the action if either:
     *
     * <ol>
     *   <li><b>Resolved-name path</b>: the role's {@code privileges[]} names a stored Kibana
     *       {@link ApplicationPrivilegeDescriptor} (e.g. {@code feature_alerting_v2_alerts.read})
     *       whose {@code actions} set matches the alerts action. The {@code actions} set may be
     *       a list of concrete entries or contain wildcard patterns; either form qualifies.</li>
     *   <li><b>Raw-pattern path</b>: the role's {@code privileges[]} entries are themselves
     *       action patterns rather than stored privilege names, and at least one of them matches
     *       the alerts action (e.g. {@code "alerts:*"} or {@code "*"} written directly under
     *       {@code privileges[]}). {@code NativePrivilegeStore#getPrivileges} returns no
     *       descriptors for entries that do not name a stored privilege, so the role descriptor
     *       itself is the only signal we have for this case.</li>
     * </ol>
     *
     * Both paths honor wildcard application names on the role descriptor
     * (e.g. {@code "kibana-*"}, {@code "*"}).
     */
    private static Set<String> collectResources(
        RoleDescriptor.ApplicationResourcePrivileges[] applicationPrivileges,
        Collection<ApplicationPrivilegeDescriptor> storedApplicationPrivileges
    ) {
        Set<String> kibanaPrivilegesGrantingAlerts = storedApplicationPrivileges.stream()
            .filter(d -> KIBANA_APPLICATION.equals(d.getApplication()))
            .filter(d -> StringMatcher.of(d.getActions()).test(ALERTS_ACTION))
            .map(ApplicationPrivilegeDescriptor::getName)
            .collect(Collectors.toSet());

        Set<String> resources = new HashSet<>();
        for (RoleDescriptor.ApplicationResourcePrivileges arp : applicationPrivileges) {
            // Application field may be a literal ("kibana-.kibana") or a wildcard ("kibana-*", "*");
            // StringMatcher handles both: it matches a literal with an exact-string predicate and only
            // builds an automaton for entries that actually contain wildcard characters.
            if (StringMatcher.of(arp.getApplication()).test(KIBANA_APPLICATION) == false) {
                continue;
            }

            // Short-circuit on the resolved-name path (cheap set lookup) before matching the
            // raw-pattern path with StringMatcher.
            boolean grantsAlertsByName = false;
            for (String privilege : arp.getPrivileges()) {
                if (kibanaPrivilegesGrantingAlerts.contains(privilege)) {
                    grantsAlertsByName = true;
                    break;
                }
            }

            if (grantsAlertsByName || StringMatcher.of(arp.getPrivileges()).test(ALERTS_ACTION)) {
                Collections.addAll(resources, arp.getResources());
            }
        }

        return resources;
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
