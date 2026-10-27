/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.store.RoleKey;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Owns the role cache and the negative-lookup cache of the {@link CompositeRolesStore} and keeps them consistent with the role
 * sources across invalidations.
 * <p>
 * Every operation runs under this object's monitor, and nothing expensive runs there: a read is a hash lookup, and resolving
 * descriptors and building the role happen before {@link #putIfCurrent} is called. An invalidation removes the affected entries
 * from both caches and then advances a generation. A resolution captures the generation before it consults any role source, so
 * if it finds the generation unchanged when it offers its result to the cache, no invalidation overlapped it and the result is
 * current. A resolution that did overlap an invalidation finds the generation moved and caches nothing; the next resolution
 * rebuilds the role from the current sources.
 * <p>
 * There is one generation per project plus a global one, so an invalidation scoped to one project does not discard the results
 * of resolutions in flight for other projects.
 */
final class GuardedRoleCaches {

    /**
     * The state of the caches as observed by a resolution before it consulted any role source. Opaque to callers.
     */
    record Generation(long project, long global) {}

    /**
     * A cache key that scopes another key to a project.
     */
    record ProjectScoped<T>(ProjectId projectId, T value) {
        ProjectScoped {
            Objects.requireNonNull(projectId);
        }
    }

    // All fields below are guarded by this object's monitor
    private final Map<ProjectScoped<RoleKey>, Role> roles;
    private final Map<ProjectScoped<String>, Boolean> negativeLookups;
    private final Map<ProjectId, Long> projectGenerations = new HashMap<>();
    private long globalGeneration;

    /**
     * @param maximumRoles           the maximum number of cached roles, or a negative value for no limit
     * @param maximumNegativeLookups the maximum number of cached negative lookups, or a negative value for no limit
     */
    GuardedRoleCaches(int maximumRoles, int maximumNegativeLookups) {
        this.roles = leastRecentlyUsedMap(maximumRoles);
        this.negativeLookups = leastRecentlyUsedMap(maximumNegativeLookups);
    }

    /**
     * A map ordered by most recent access that drops its least recently used entry once it holds more than {@code maximumSize}
     * entries. A negative maximum means no limit; zero means nothing is ever retained.
     */
    private static <K, V> Map<K, V> leastRecentlyUsedMap(int maximumSize) {
        return new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                return maximumSize >= 0 && size() > maximumSize;
            }
        };
    }

    private static <T extends ProjectScoped<?>> Predicate<T> eq(ProjectId projectId) {
        return key -> key.projectId().equals(projectId);
    }

    @Nullable
    synchronized Role getRole(ProjectId projectId, RoleKey roleKey) {
        return roles.get(new ProjectScoped<>(projectId, roleKey));
    }

    synchronized boolean isKnownMissing(ProjectId projectId, String roleName) {
        // get rather than containsKey so that the lookup counts as an access for eviction purposes
        return negativeLookups.get(new ProjectScoped<>(projectId, roleName)) != null;
    }

    /**
     * Captures the current generation for the given project. Must be called before the resolution consults any role source,
     * including the negative-lookup cache, for the check in {@link #putIfCurrent} to be meaningful.
     */
    synchronized Generation generation(ProjectId projectId) {
        return new Generation(projectGenerations.getOrDefault(projectId, 0L), globalGeneration);
    }

    /**
     * Caches the built role and records the role names that were not found, unless an invalidation has moved the generation
     * since {@code observed} was captured.
     *
     * @return {@code true} if the entries were cached, {@code false} if they were discarded because of an invalidation
     */
    synchronized boolean putIfCurrent(Generation observed, ProjectId projectId, RoleKey roleKey, Role role, Set<String> missing) {
        if (observed.equals(generation(projectId)) == false) {
            return false;
        }
        roles.put(new ProjectScoped<>(projectId, roleKey), role);
        for (String name : missing) {
            negativeLookups.put(new ProjectScoped<>(projectId, name), Boolean.TRUE);
        }
        return true;
    }

    synchronized void invalidateAll() {
        roles.clear();
        negativeLookups.clear();
        globalGeneration++;
    }

    synchronized void invalidateProject(ProjectId projectId) {
        roles.keySet().removeIf(eq(projectId));
        negativeLookups.keySet().removeIf(eq(projectId));
        projectGenerations.merge(projectId, 1L, Long::sum);
    }

    synchronized void removeProject(ProjectId projectId) {
        roles.keySet().removeIf(eq(projectId));
        negativeLookups.keySet().removeIf(eq(projectId));
        projectGenerations.remove(projectId);
        // Resolutions still in flight for the removed project captured a per-project generation that is now forgotten;
        // advancing the global generation keeps them from repopulating the caches
        globalGeneration++;
    }

    synchronized void invalidateRole(ProjectId projectId, String role) {
        roles.keySet().removeIf(key -> key.projectId().equals(projectId) && key.value().getNames().contains(role));
        negativeLookups.remove(new ProjectScoped<>(projectId, role));
        projectGenerations.merge(projectId, 1L, Long::sum);
    }

    synchronized void invalidateClusterScopedRoles(Set<String> names) {
        roles.keySet().removeIf(key -> Sets.haveEmptyIntersection(key.value().getNames(), names) == false);
        negativeLookups.keySet().removeIf(key -> names.contains(key.value()));
        globalGeneration++;
    }

    // for testing
    synchronized List<ProjectScoped<RoleKey>> cachedRoleKeys() {
        return List.copyOf(roles.keySet());
    }
}
