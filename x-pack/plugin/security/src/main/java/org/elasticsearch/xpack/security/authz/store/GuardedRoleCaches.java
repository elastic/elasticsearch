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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Owns the role cache and the negative-lookup cache of the {@link CompositeRolesStore} and keeps them consistent with the role
 * sources across invalidations.
 * <p>
 * Every operation runs under this object's monitor, and nothing expensive runs there: resolving descriptors and building the
 * role happen before {@link #putIfCurrent} is called, and a read hashes and compares the role key, whose cost grows with the
 * number of role names, before taking the monitor. To make that possible, each cached role key is interned to an
 * {@link InternedKey} whose equality is identity, and the role cache is keyed by that token, so a lookup under the monitor costs
 * the same regardless of the size of the key. An invalidation removes the affected entries
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

    /**
     * The canonical token for a cached role key. It does not override {@code equals} or {@code hashCode}, so a lookup by token
     * under the monitor is an identity comparison. Two tokens for equal keys can only cost a cache miss, never serve the wrong role.
     */
    static final class InternedKey {
        private final ProjectScoped<RoleKey> key;

        private InternedKey(ProjectScoped<RoleKey> key) {
            this.key = key;
        }
    }

    /**
     * Maps each key in {@link #roles} to its token. Read without the monitor, but only modified under it, alongside {@link #roles},
     * so it holds exactly the keys of {@link #roles}. A reader can find a token whose role has just been removed; it then misses.
     */
    private final ConcurrentHashMap<ProjectScoped<RoleKey>, InternedKey> interned = new ConcurrentHashMap<>();

    // All fields below are guarded by this object's monitor
    private final Map<InternedKey, Role> roles;
    private final Map<ProjectScoped<String>, Boolean> negativeLookups;
    private final Map<ProjectId, Long> projectGenerations = new HashMap<>();
    private long globalGeneration;

    /**
     * @param maximumRoles           the maximum number of cached roles, or a negative value for no limit
     * @param maximumNegativeLookups the maximum number of cached negative lookups, or a negative value for no limit
     */
    GuardedRoleCaches(int maximumRoles, int maximumNegativeLookups) {
        this.roles = leastRecentlyUsedMap(maximumRoles, token -> interned.remove(token.key, token));
        this.negativeLookups = leastRecentlyUsedMap(maximumNegativeLookups, key -> {});
    }

    /**
     * A map ordered by most recent access that drops its least recently used entry once it holds more than {@code maximumSize}
     * entries, passing its key to {@code onEvict}. A negative maximum means no limit; zero means nothing is ever retained.
     */
    private static <K, V> Map<K, V> leastRecentlyUsedMap(int maximumSize, Consumer<K> onEvict) {
        return new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
                if (maximumSize >= 0 && size() > maximumSize) {
                    onEvict.accept(eldest.getKey());
                    return true;
                }
                return false;
            }
        };
    }

    private static <T extends ProjectScoped<?>> Predicate<T> eq(ProjectId projectId) {
        return key -> key.projectId().equals(projectId);
    }

    /**
     * Removes the cached roles whose keys match, together with their tokens.
     */
    private void removeRoles(Predicate<ProjectScoped<RoleKey>> predicate) {
        assert Thread.holdsLock(this);
        final Iterator<InternedKey> iterator = roles.keySet().iterator();
        while (iterator.hasNext()) {
            final InternedKey token = iterator.next();
            if (predicate.test(token.key)) {
                iterator.remove();
                interned.remove(token.key, token);
            }
        }
    }

    @Nullable
    Role getRole(ProjectId projectId, RoleKey roleKey) {
        // Hashing and comparing the key happen here, outside the monitor
        final InternedKey token = interned.get(new ProjectScoped<>(projectId, roleKey));
        if (token == null) {
            return null;
        }
        synchronized (this) {
            return roles.get(token);
        }
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
     * since {@code observed} was captured. If another resolution already cached a role, retains that instance and its warmed caches.
     *
     * @return {@code true} if the generation is still current, {@code false} if the entries were discarded because of an invalidation
     */
    synchronized boolean putIfCurrent(Generation observed, ProjectId projectId, RoleKey roleKey, Role role, Set<String> missing) {
        if (observed.equals(generation(projectId)) == false) {
            return false;
        }
        // Interned under the monitor so that the table never holds a token that is absent from the role cache
        roles.putIfAbsent(interned.computeIfAbsent(new ProjectScoped<>(projectId, roleKey), InternedKey::new), role);
        for (String name : missing) {
            negativeLookups.put(new ProjectScoped<>(projectId, name), Boolean.TRUE);
        }
        return true;
    }

    synchronized void invalidateAll() {
        roles.clear();
        interned.clear();
        negativeLookups.clear();
        globalGeneration++;
    }

    synchronized void invalidateProject(ProjectId projectId) {
        removeRoles(eq(projectId));
        negativeLookups.keySet().removeIf(eq(projectId));
        projectGenerations.merge(projectId, 1L, Long::sum);
    }

    synchronized void removeProject(ProjectId projectId) {
        removeRoles(eq(projectId));
        negativeLookups.keySet().removeIf(eq(projectId));
        projectGenerations.remove(projectId);
        // Resolutions still in flight for the removed project captured a per-project generation that is now forgotten;
        // advancing the global generation keeps them from repopulating the caches
        globalGeneration++;
    }

    synchronized void invalidateRole(ProjectId projectId, String role) {
        removeRoles(key -> key.projectId().equals(projectId) && key.value().getNames().contains(role));
        negativeLookups.remove(new ProjectScoped<>(projectId, role));
        projectGenerations.merge(projectId, 1L, Long::sum);
    }

    synchronized void invalidateClusterScopedRoles(Set<String> names) {
        removeRoles(key -> Sets.haveEmptyIntersection(key.value().getNames(), names) == false);
        negativeLookups.keySet().removeIf(key -> names.contains(key.value()));
        globalGeneration++;
    }

    // for testing
    synchronized List<ProjectScoped<RoleKey>> cachedRoleKeys() {
        return roles.keySet().stream().map(token -> token.key).toList();
    }

    // for testing
    int internedKeyCount() {
        return interned.size();
    }
}
