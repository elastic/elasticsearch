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
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Implicitly grants read access to every Kibana Significant Events data stream ({@code .significant_events-*}, e.g.
 * detections and knowledge indicators) for users whose roles include a Kibana application privilege granting the
 * Nightshift {@code api:read_nightshift} action.
 * <p>
 * Kibana guards the Significant Events read routes with {@code read_nightshift}, but reads these hidden
 * data streams as the end user. Roles such as {@code viewer} hold that Kibana privilege without any
 * index privilege on dot-prefixed data streams, so the routes silently returned no data. This provider
 * aligns the Elasticsearch-level access with the Kibana-level authorization. The prefix pattern is
 * deliberate: Significant Events owns the whole {@code .significant_events-} namespace, so data streams
 * added or removed on the Kibana side are covered without changes here.
 * <p>
 * Space-aware documents carry the {@code kibana.space_ids} field that Kibana's data streams client writes.
 * When the user has access to specific spaces, a DLS query restricts visibility to documents in those spaces,
 * plus documents without {@code kibana.space_ids}. Kibana treats those as visible in every space (knowledge
 * indicators, for instance, are never space-scoped), and this provider mirrors that so both layers agree.
 * When the user has the wildcard resource ({@code *}), full document access is granted with no DLS restriction.
 * <p>
 * It also grants access to the ES|QL source views Kibana creates per space, named
 * {@code $.nightshift.sources.<spaceId>.<slug>}: {@code read} and {@code read_view_metadata} for {@code api:read_nightshift},
 * {@code manage_view} for {@code api:manage_nightshift}. Each action takes its own resources, so read in one space and
 * manage in another yields the right grant per space.
 */
public class KibanaNightshiftImplicitPrivilegesProvider implements ImplicitPrivilegesProvider {

    static final String KIBANA_APPLICATION = "kibana-.kibana";
    // Mirrors NIGHTSHIFT_API_PRIVILEGES.read in Kibana's
    // x-pack/solutions/observability/packages/kbn-nightshift-shared/index.ts; Kibana prefixes API privileges with "api:".
    static final String READ_NIGHTSHIFT_ACTION = "api:read_nightshift";
    // Prefix shared by every data stream the Kibana significant_events plugin registers; see the DataStreamDefinitions under
    // x-pack/solutions/observability/plugins/significant_events/server/lib in the Kibana repo.
    static final String[] SIGNIFICANT_EVENTS_INDICES = { ".significant_events-*" };
    // Mirrors NIGHTSHIFT_API_PRIVILEGES.manage in the same Kibana file.
    static final String MANAGE_NIGHTSHIFT_ACTION = "api:manage_nightshift";
    // Mirrors NIGHTSHIFT_SOURCE_VIEW_PREFIX in Kibana's src/sources/view_name.ts; views are named "<prefix><spaceId>.<slug>".
    static final String SOURCE_VIEW_PREFIX = "$.nightshift.sources.";
    // Index privileges from IndexPrivilege: read_view_metadata allows the view get action, manage_view covers put, get and delete.
    static final String READ_VIEW_METADATA_PRIVILEGE = "read_view_metadata";
    static final String MANAGE_VIEW_PRIVILEGE = "manage_view";
    // Mirrors SPACE_ID_REGEX in Kibana. Resource text comes from admin-authored roles, so ids that could act as wildcards or
    // contain dots (e.g. "space:*") are rejected rather than widening the view pattern to other spaces.
    private static final Pattern VALID_SPACE_ID = Pattern.compile("^[a-z0-9_\\-]+$");
    static final String RESOURCE_PREFIX = "space:";
    static final String ALL_RESOURCES = "*";
    static final String INDEX_READ_PRIVILEGE = "read";
    private static final String[] READ_VIEW_PRIVILEGES = { INDEX_READ_PRIVILEGE, READ_VIEW_METADATA_PRIVILEGE };
    private static final String[] MANAGE_VIEW_PRIVILEGES = { MANAGE_VIEW_PRIVILEGE };
    // Written by Kibana's data streams client; see src/platform/packages/private/kbn-data-streams/src/space_utils.ts
    static final String SPACE_IDS_FIELD = "kibana.space_ids";

    @Override
    public Collection<RoleDescriptor.IndicesPrivileges> getImplicitIndicesPrivileges(
        Collection<ResolvedApplicationPrivilege> applicationPrivileges
    ) {
        final List<RoleDescriptor.IndicesPrivileges> grants = new ArrayList<>();

        final Set<String> readResources = collectResources(applicationPrivileges, READ_NIGHTSHIFT_ACTION);
        significantEventsGrant(readResources).ifPresent(grants::add);
        viewGrant(readResources, READ_VIEW_PRIVILEGES).ifPresent(grants::add);

        final Set<String> manageResources = collectResources(applicationPrivileges, MANAGE_NIGHTSHIFT_ACTION);
        viewGrant(manageResources, MANAGE_VIEW_PRIVILEGES).ifPresent(grants::add);
        return grants;
    }

    private static Optional<RoleDescriptor.IndicesPrivileges> significantEventsGrant(Set<String> readResources) {
        if (readResources.contains(ALL_RESOURCES)) {
            return Optional.of(
                RoleDescriptor.IndicesPrivileges.builder().indices(SIGNIFICANT_EVENTS_INDICES).privileges(INDEX_READ_PRIVILEGE).build()
            );
        }
        final Set<String> spaceIds = spaceIds(readResources);
        if (spaceIds.isEmpty()) {
            return Optional.empty();
        }
        return Optional.of(
            RoleDescriptor.IndicesPrivileges.builder()
                .indices(SIGNIFICANT_EVENTS_INDICES)
                .privileges(INDEX_READ_PRIVILEGE)
                .query(buildSpaceIdsDlsQuery(spaceIds))
                .build()
        );
    }

    /**
     * Grants {@code privileges} on the source views of every space in {@code resources}. No DLS query is attached because
     * views hold no documents; the space id embedded in the view name is the only scoping. Space ids cannot contain dots or
     * wildcards, so {@code $.nightshift.sources.<spaceId>.*} never matches another space's views.
     */
    private static Optional<RoleDescriptor.IndicesPrivileges> viewGrant(Set<String> resources, String[] privileges) {
        final String[] patterns = resources.contains(ALL_RESOURCES)
            ? new String[] { SOURCE_VIEW_PREFIX + "*" }
            : spaceIds(resources).stream()
                .filter(id -> VALID_SPACE_ID.matcher(id).matches())
                .map(id -> SOURCE_VIEW_PREFIX + id + ".*")
                .sorted()
                .toArray(String[]::new);
        if (patterns.length == 0) {
            return Optional.empty();
        }
        return Optional.of(RoleDescriptor.IndicesPrivileges.builder().indices(patterns).privileges(privileges).build());
    }

    /** Space ids of the {@code space:<id>} resources; resources without the prefix are ignored. */
    private static Set<String> spaceIds(Set<String> resources) {
        return resources.stream()
            .filter(r -> r.startsWith(RESOURCE_PREFIX))
            .map(r -> r.substring(RESOURCE_PREFIX.length()))
            .collect(Collectors.toSet());
    }

    /**
     * Union of resources from every resolved application-privilege grant that targets the Kibana application
     * <i>and</i> authorizes {@code action}. The resolved privilege's predicate already covers both
     * stored privileges referenced by name and raw action patterns (e.g. {@code "api:*"} or {@code "*"}).
     */
    private static Set<String> collectResources(Collection<ResolvedApplicationPrivilege> applicationPrivileges, String action) {
        Set<String> resources = new HashSet<>();
        for (ResolvedApplicationPrivilege resolved : applicationPrivileges) {
            final ApplicationPrivilege privilege = resolved.privilege();
            if (applicationMatchesKibana(privilege.getApplication()) && privilege.predicate().test(action)) {
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

    /**
     * Matches documents in any of {@code spaceIds}, or documents without {@code kibana.space_ids} (space-agnostic).
     * Hand-rolled rather than built with {@code QueryBuilders} so the stored/surfaced DLS stays free of the default
     * {@code "boost":1.0} that the query builders always serialize.
     */
    static String buildSpaceIdsDlsQuery(Set<String> spaceIds) {
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.startObject("bool");
            builder.startArray("should");

            builder.startObject();
            builder.startObject("terms");
            builder.array(SPACE_IDS_FIELD, spaceIds.toArray(new String[0]));
            builder.endObject();
            builder.endObject();

            builder.startObject();
            builder.startObject("bool");
            builder.startArray("must_not");
            builder.startObject();
            builder.startObject("exists");
            builder.field("field", SPACE_IDS_FIELD);
            builder.endObject();
            builder.endObject();
            builder.endArray();
            builder.endObject();
            builder.endObject();

            builder.endArray();
            builder.field("minimum_should_match", 1);
            builder.endObject();
            builder.endObject();
            return Strings.toString(builder);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
