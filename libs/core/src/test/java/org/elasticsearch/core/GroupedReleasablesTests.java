/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;

public class GroupedReleasablesTests extends ESTestCase {

    public void testAddReturnsItsArgument() {
        try (GroupedReleasables group = new GroupedReleasables()) {
            final Releasable resource = () -> {};
            assertThat(group.add(resource), sameInstance(resource));
            assertThat(group.size(), equalTo(1));
        }
    }

    public void testReleasesInAdditionOrder() {
        final List<String> released = new ArrayList<>();
        try (GroupedReleasables group = new GroupedReleasables(3)) {
            group.add(() -> released.add("first"));
            group.add(() -> released.add("second"));
            group.add(() -> released.add("third"));
            assertThat(released, equalTo(List.of()));
        }
        assertThat(released, contains("first", "second", "third"));
    }

    public void testCloseIsIdempotentAndEmptiesTheGroup() {
        final AtomicInteger releases = new AtomicInteger();
        final GroupedReleasables group = new GroupedReleasables();
        group.add(releases::incrementAndGet);
        group.close();
        assertThat(releases.get(), equalTo(1));
        assertThat(group.size(), equalTo(0));

        group.close();
        assertThat("a second close must not re-release anything", releases.get(), equalTo(1));
    }

    /**
     * A member that throws from its own {@code close()} must not strand the rest of the group, and the
     * failure must still surface to the caller — matching {@link Releasables#close(Iterable)}.
     */
    public void testAThrowingMemberStillReleasesTheRest() {
        final AtomicInteger releases = new AtomicInteger();
        final GroupedReleasables group = new GroupedReleasables();
        group.add(releases::incrementAndGet);
        group.add(() -> { throw new IllegalStateException("boom"); });
        group.add(releases::incrementAndGet);

        final IllegalStateException e = expectThrows(IllegalStateException.class, group::close);
        assertThat(e.getMessage(), equalTo("boom"));
        assertThat("the members either side of the failure are still released", releases.get(), equalTo(2));
        assertThat(group.size(), equalTo(0));
    }

    public void testEmptyGroupCloseIsANoop() {
        final GroupedReleasables group = new GroupedReleasables(0);
        group.close();
        assertThat(group.size(), equalTo(0));
    }
}
