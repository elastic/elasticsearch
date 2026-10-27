/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.security.authz.store;

import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.authz.permission.Role;
import org.elasticsearch.xpack.core.security.authz.store.RoleKey;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class GuardedRoleCachesTests extends ESTestCase {

    private static final int UNBOUNDED = -1;
    private static final RoleKey R1 = roleKey("r1");
    private static final RoleKey R1_R2 = roleKey("r1", "r2");
    private static final RoleKey R2 = roleKey("r2");

    public void testWriteWithUnchangedGenerationIsCachedAndServed() {
        final GuardedRoleCaches caches = new GuardedRoleCaches(UNBOUNDED, UNBOUNDED);
        final ProjectId project = randomUniqueProjectId();
        final Role role = role("r1", "r2");

        final GuardedRoleCaches.Generation generation = caches.generation(project);
        assertThat(caches.getRole(project, R1_R2), nullValue());
        assertThat(caches.putIfCurrent(generation, project, R1_R2, role, Set.of("m1")), is(true));

        assertThat(caches.getRole(project, R1_R2), sameInstance(role));
        assertThat(caches.getRole(project, R1), nullValue());
        assertThat(caches.getRole(randomUniqueProjectId(), R1_R2), nullValue());
        assertThat(caches.isKnownMissing(project, "m1"), is(true));
        assertThat(caches.isKnownMissing(project, "r1"), is(false));
        assertThat(caches.isKnownMissing(randomUniqueProjectId(), "m1"), is(false));
        // the generation only moves on invalidation, so a token captured earlier is still current
        assertThat(caches.putIfCurrent(generation, project, R2, role("r2"), Set.of()), is(true));
        assertThat(caches.getRole(project, R2), notNullValue());
    }

    // Every kind of invalidation must move the generation of the project it affects. One case per kind so that a failure
    // names the kind. Each case starts from a fresh instance, so the project has never been invalidated before.

    public void testWriteAfterInvalidateAllIsRejected() {
        assertWriteAfterInvalidationIsRejected((caches, project) -> caches.invalidateAll());
    }

    public void testWriteAfterInvalidateProjectIsRejected() {
        assertWriteAfterInvalidationIsRejected(GuardedRoleCaches::invalidateProject);
    }

    /**
     * Removing a project forgets its generation, so a resolution that captured that generation before the removal would compare
     * equal to the default again if the project had never been invalidated, as is the case here. Advancing the global generation
     * on removal is what keeps such a resolution from caching a role for a project that no longer exists.
     */
    public void testWriteAfterRemoveProjectIsRejected() {
        assertWriteAfterInvalidationIsRejected(GuardedRoleCaches::removeProject);
    }

    public void testWriteAfterInvalidateRoleIsRejected() {
        assertWriteAfterInvalidationIsRejected((caches, project) -> caches.invalidateRole(project, "r1"));
    }

    /** The generation is tracked per project, not per role, so invalidating an unrelated role in the project also rejects. */
    public void testWriteAfterInvalidateUnrelatedRoleIsRejected() {
        assertWriteAfterInvalidationIsRejected((caches, project) -> caches.invalidateRole(project, "unrelated"));
    }

    public void testWriteAfterInvalidateClusterScopedRolesIsRejected() {
        assertWriteAfterInvalidationIsRejected((caches, project) -> caches.invalidateClusterScopedRoles(Set.of("r1")));
    }

    public void testWriteAfterInvalidateUnrelatedClusterScopedRolesIsRejected() {
        assertWriteAfterInvalidationIsRejected((caches, project) -> caches.invalidateClusterScopedRoles(Set.of("unrelated")));
    }

    private static void assertWriteAfterInvalidationIsRejected(BiConsumer<GuardedRoleCaches, ProjectId> invalidation) {
        final GuardedRoleCaches caches = new GuardedRoleCaches(UNBOUNDED, UNBOUNDED);
        final ProjectId project = randomUniqueProjectId();

        final GuardedRoleCaches.Generation generation = caches.generation(project);
        invalidation.accept(caches, project);
        assertThat(caches.putIfCurrent(generation, project, R1, role("r1"), Set.of("m1")), is(false));

        assertThat(caches.getRole(project, R1), nullValue());
        assertThat(caches.isKnownMissing(project, "m1"), is(false));
        // a generation captured after the invalidation is current again
        assertThat(caches.putIfCurrent(caches.generation(project), project, R1, role("r1"), Set.of("m1")), is(true));
        assertThat(caches.getRole(project, R1), notNullValue());
        assertThat(caches.isKnownMissing(project, "m1"), is(true));
    }

    // An invalidation scoped to another project must not move this project's generation.

    public void testWriteAfterInvalidateProjectOfAnotherProjectIsAccepted() {
        assertWriteAfterInvalidationOfAnotherProjectIsAccepted(GuardedRoleCaches::invalidateProject);
    }

    public void testWriteAfterInvalidateRoleInAnotherProjectIsAccepted() {
        assertWriteAfterInvalidationOfAnotherProjectIsAccepted((caches, otherProject) -> caches.invalidateRole(otherProject, "r1"));
    }

    private static void assertWriteAfterInvalidationOfAnotherProjectIsAccepted(BiConsumer<GuardedRoleCaches, ProjectId> invalidation) {
        final GuardedRoleCaches caches = new GuardedRoleCaches(UNBOUNDED, UNBOUNDED);
        final ProjectId project = randomUniqueProjectId();
        final ProjectId otherProject = randomUniqueProjectId();
        final Role role = role("r1");

        final GuardedRoleCaches.Generation generation = caches.generation(project);
        invalidation.accept(caches, otherProject);
        assertThat(caches.putIfCurrent(generation, project, R1, role, Set.of()), is(true));
        assertThat(caches.getRole(project, R1), sameInstance(role));
    }

    public void testInvalidateRoleRemovesEntriesNamingTheRoleInThatProjectOnly() {
        final ProjectId project = randomUniqueProjectId();
        final ProjectId otherProject = randomUniqueProjectId();
        final GuardedRoleCaches caches = populatedCaches(project, otherProject);

        caches.invalidateRole(project, "r1");
        assertThat(caches.getRole(project, R1), nullValue());
        assertThat(caches.getRole(project, R1_R2), nullValue());
        assertThat(caches.getRole(project, R2), notNullValue());
        assertThat(caches.getRole(otherProject, R1), notNullValue());
        assertThat(caches.isKnownMissing(project, "m1"), is(true));

        caches.invalidateRole(project, "m1");
        assertThat(caches.isKnownMissing(project, "m1"), is(false));
        assertThat(caches.isKnownMissing(project, "m2"), is(true));
        assertThat(caches.isKnownMissing(otherProject, "m1"), is(true));
    }

    public void testInvalidateClusterScopedRolesRemovesEntriesNamingTheRolesInEveryProject() {
        final ProjectId project = randomUniqueProjectId();
        final ProjectId otherProject = randomUniqueProjectId();
        final GuardedRoleCaches caches = populatedCaches(project, otherProject);

        caches.invalidateClusterScopedRoles(Set.of("r1", "m1"));

        assertThat(caches.getRole(project, R1), nullValue());
        assertThat(caches.getRole(project, R1_R2), nullValue());
        assertThat(caches.getRole(project, R2), notNullValue());
        assertThat(caches.getRole(otherProject, R1), nullValue());
        assertThat(caches.isKnownMissing(project, "m1"), is(false));
        assertThat(caches.isKnownMissing(otherProject, "m1"), is(false));
        assertThat(caches.isKnownMissing(project, "m2"), is(true));
    }

    public void testInvalidateProjectRemovesEntriesOfThatProjectOnly() {
        assertProjectInvalidationRemovesEntriesOfThatProjectOnly(GuardedRoleCaches::invalidateProject);
    }

    public void testRemoveProjectRemovesEntriesOfThatProjectOnly() {
        assertProjectInvalidationRemovesEntriesOfThatProjectOnly(GuardedRoleCaches::removeProject);
    }

    private static void assertProjectInvalidationRemovesEntriesOfThatProjectOnly(BiConsumer<GuardedRoleCaches, ProjectId> invalidation) {
        final ProjectId project = randomUniqueProjectId();
        final ProjectId otherProject = randomUniqueProjectId();
        final GuardedRoleCaches caches = populatedCaches(project, otherProject);

        invalidation.accept(caches, project);

        assertThat(caches.getRole(project, R1), nullValue());
        assertThat(caches.getRole(project, R1_R2), nullValue());
        assertThat(caches.getRole(project, R2), nullValue());
        assertThat(caches.getRole(otherProject, R1), notNullValue());
        assertThat(caches.isKnownMissing(project, "m1"), is(false));
        assertThat(caches.isKnownMissing(project, "m2"), is(false));
        assertThat(caches.isKnownMissing(otherProject, "m1"), is(true));
    }

    public void testInvalidateAllRemovesEverything() {
        final ProjectId project = randomUniqueProjectId();
        final ProjectId otherProject = randomUniqueProjectId();
        final GuardedRoleCaches caches = populatedCaches(project, otherProject);

        caches.invalidateAll();

        assertThat(caches.getRole(project, R1), nullValue());
        assertThat(caches.getRole(project, R1_R2), nullValue());
        assertThat(caches.getRole(project, R2), nullValue());
        assertThat(caches.getRole(otherProject, R1), nullValue());
        assertThat(caches.isKnownMissing(project, "m1"), is(false));
        assertThat(caches.isKnownMissing(project, "m2"), is(false));
        assertThat(caches.isKnownMissing(otherProject, "m1"), is(false));
    }

    public void testLeastRecentlyUsedRoleIsEvictedBeyondMaximum() {
        final GuardedRoleCaches caches = new GuardedRoleCaches(2, UNBOUNDED);
        final ProjectId project = randomUniqueProjectId();
        final Role roleA = role("a");
        final Role roleC = role("c");
        assertThat(caches.putIfCurrent(caches.generation(project), project, roleKey("a"), roleA, Set.of()), is(true));
        assertThat(caches.putIfCurrent(caches.generation(project), project, roleKey("b"), role("b"), Set.of()), is(true));
        // reading a makes b the least recently used entry
        assertThat(caches.getRole(project, roleKey("a")), sameInstance(roleA));
        assertThat(caches.putIfCurrent(caches.generation(project), project, roleKey("c"), roleC, Set.of()), is(true));

        assertThat(caches.getRole(project, roleKey("b")), nullValue());
        assertThat(caches.getRole(project, roleKey("a")), sameInstance(roleA));
        assertThat(caches.getRole(project, roleKey("c")), sameInstance(roleC));
    }

    public void testLeastRecentlyUsedNegativeLookupIsEvictedBeyondMaximum() {
        final GuardedRoleCaches caches = new GuardedRoleCaches(UNBOUNDED, 2);
        final ProjectId project = randomUniqueProjectId();
        assertThat(caches.putIfCurrent(caches.generation(project), project, roleKey("a"), role("a"), Set.of("m1")), is(true));
        assertThat(caches.putIfCurrent(caches.generation(project), project, roleKey("b"), role("b"), Set.of("m2")), is(true));
        // checking m1 makes m2 the least recently used entry
        assertThat(caches.isKnownMissing(project, "m1"), is(true));
        assertThat(caches.putIfCurrent(caches.generation(project), project, roleKey("c"), role("c"), Set.of("m3")), is(true));

        assertThat(caches.isKnownMissing(project, "m2"), is(false));
        assertThat(caches.isKnownMissing(project, "m1"), is(true));
        assertThat(caches.isKnownMissing(project, "m3"), is(true));
    }

    public void testZeroMaximumRetainsNothing() {
        final GuardedRoleCaches caches = new GuardedRoleCaches(0, 0);
        final ProjectId project = randomUniqueProjectId();
        assertThat(caches.putIfCurrent(caches.generation(project), project, R1, role("r1"), Set.of("m1")), is(true));

        assertThat(caches.getRole(project, R1), nullValue());
        assertThat(caches.isKnownMissing(project, "m1"), is(false));
    }

    public void testNegativeMaximumMeansNoLimit() {
        final GuardedRoleCaches caches = new GuardedRoleCaches(UNBOUNDED, UNBOUNDED);
        final ProjectId project = randomUniqueProjectId();
        final int count = 500;
        for (int i = 0; i < count; i++) {
            final String name = "role-" + i;
            assertThat(
                caches.putIfCurrent(caches.generation(project), project, roleKey(name), role(name), Set.of("missing-" + i)),
                is(true)
            );
        }
        for (int i = 0; i < count; i++) {
            assertThat(caches.getRole(project, roleKey("role-" + i)), notNullValue());
            assertThat(caches.isKnownMissing(project, "missing-" + i), is(true));
        }
    }

    /**
     * Every operation runs under the monitor, so a read that arrives while an invalidation is scanning the caches waits for
     * the invalidation to finish and observes its result. This rules out, by construction, a concurrent read disturbing the scan.
     * <p>
     * The scan evaluates its predicate once per cached role, and for {@link GuardedRoleCaches#invalidateClusterScopedRoles} the
     * predicate iterates the caller-supplied set of changed role names. Instrumenting that set's iterator is therefore a
     * deterministic way to start the read at a precise point of the scan.
     */
    public void testReadWaitsForInvalidationInProgress() throws Exception {
        final GuardedRoleCaches caches = new GuardedRoleCaches(UNBOUNDED, UNBOUNDED);
        final ProjectId project = randomUniqueProjectId();
        final RoleKey keyA = roleKey("role-a");
        final RoleKey keyB = roleKey("role-b");
        assertThat(caches.putIfCurrent(caches.generation(project), project, keyA, role("role-a"), Set.of()), is(true));
        assertThat(caches.putIfCurrent(caches.generation(project), project, keyB, role("role-b"), Set.of()), is(true));

        final AtomicReference<Role> observed = new AtomicReference<>();
        final Thread reader = new Thread(() -> observed.set(caches.getRole(project, keyA)), "reader");
        final AtomicInteger scanVisits = new AtomicInteger();
        final Set<String> changedRoles = new HashSet<>(Set.of("role-a")) {
            @Override
            public Iterator<String> iterator() {
                if (scanVisits.incrementAndGet() == 1) {
                    // The scan is on its first key and holds the monitor; the reader must block until the scan completes
                    reader.start();
                    try {
                        assertBusy(() -> assertThat(reader.getState(), is(Thread.State.BLOCKED)));
                    } catch (Exception e) {
                        throw new AssertionError(e);
                    }
                }
                return super.iterator();
            }
        };
        caches.invalidateClusterScopedRoles(changedRoles);
        reader.join(10_000);
        assertFalse("the reader must complete once the invalidation has released the monitor", reader.isAlive());

        assertThat(
            "the instrumented set must be iterated once per cached role for this test to exercise the scan",
            scanVisits.get(),
            is(2)
        );
        assertThat("the read must observe the completed invalidation", observed.get(), nullValue());
        assertThat(caches.getRole(project, keyB), notNullValue());
    }

    /**
     * Caches, in {@code project}, the roles {r1}, {r1, r2} and {r2} with the missing names m1 and m2, and in {@code otherProject}
     * the role {r1} with the missing name m1.
     */
    private static GuardedRoleCaches populatedCaches(ProjectId project, ProjectId otherProject) {
        final GuardedRoleCaches caches = new GuardedRoleCaches(UNBOUNDED, UNBOUNDED);
        assertThat(caches.putIfCurrent(caches.generation(project), project, R1, role("r1"), Set.of("m1")), is(true));
        assertThat(caches.putIfCurrent(caches.generation(project), project, R1_R2, role("r1", "r2"), Set.of("m2")), is(true));
        assertThat(caches.putIfCurrent(caches.generation(project), project, R2, role("r2"), Set.of()), is(true));
        assertThat(caches.putIfCurrent(caches.generation(otherProject), otherProject, R1, role("r1"), Set.of("m1")), is(true));
        return caches;
    }

    private static RoleKey roleKey(String... names) {
        return new RoleKey(Set.of(names), RoleKey.ROLES_STORE_SOURCE);
    }

    private static Role role(String... names) {
        return Role.builder(TestRestrictedIndices.RESTRICTED_INDICES, names).build();
    }
}
