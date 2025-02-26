/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release;

import org.elasticsearch.gradle.Version;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

public class TagVersionsTaskTests {

    @Test
    public void testAddLastRecord() {
        List<String> startingLines = List.of(
            "8.0.0,6100",
            "8.0.1,6100",
            "8.1.0,6204",
            "8.2.0,6234",
            "8.3.0,6239",
            "8.3.1,6260",
            "8.4.0,6301"
        );

        var modified = TagVersionsTask.addVersionRecord(new ArrayList<>(startingLines), Version.fromString("8.4.1"), 6305);
        assertThat(modified.isPresent(), is(true));

        List<String> expected = new ArrayList<>(startingLines);
        expected.add("8.4.1,6305");
        expected.sort(Comparator.naturalOrder());
        assertThat(modified.get(), contains(expected.toArray()));
    }

    @Test
    public void testAddMiddleRecord() {
        List<String> startingLines = List.of(
            "8.0.0,6100",
            "8.0.1,6100",
            "8.1.0,6204",
            "8.2.0,6234",
            "8.3.0,6239",
            "8.3.1,6260",
            "8.4.0,6301"
        );

        var modified = TagVersionsTask.addVersionRecord(new ArrayList<>(startingLines), Version.fromString("8.3.2"), 6280);
        assertThat(modified.isPresent(), is(true));

        List<String> expected = new ArrayList<>(startingLines);
        expected.add("8.3.2,6280");
        expected.sort(Comparator.naturalOrder());
        assertThat(modified.get(), contains(expected.toArray()));
    }

    @Test
    public void testIdempotent() {
        List<String> startingLines = List.of(
            "8.0.0,6100",
            "8.0.1,6100",
            "8.1.0,6204",
            "8.2.0,6234",
            "8.3.1,6260",
            "8.3.0,6239",
            "8.4.0,6301"
        );

        var modified = TagVersionsTask.addVersionRecord(new ArrayList<>(startingLines), Version.fromString("8.4.0"), 6301);
        assertThat(modified.isPresent(), is(false));
    }

    @Test
    public void testFailsConflictingId() {
        List<String> startingLines = List.of(
            "8.0.0,6100",
            "8.0.1,6100",
            "8.1.0,6204",
            "8.2.0,6234",
            "8.3.1,6260",
            "8.3.0,6239",
            "8.4.0,6301"
        );

        var ex = assertThrows(
            IllegalArgumentException.class,
            () -> TagVersionsTask.addVersionRecord(new ArrayList<>(startingLines), Version.fromString("8.4.0"), 6302)
        );
        assertThat(ex.getMessage(), is("Release [8.4.0] already recorded with version id [6301], cannot update to version [6302]"));
    }

    @Test
    public void testFailsIncorrectFormat() {
        List<String> lines = List.of("8.0.,f4d2");

        var ex = assertThrows(
            IllegalArgumentException.class,
            () -> TagVersionsTask.addVersionRecord(new ArrayList<>(lines), Version.fromString("1.0.0"), 1)
        );
        assertThat(ex.getMessage(), is("Incorrect format for line [8.0.,f4d2]"));
    }

    @Test
    public void testFailsDuplicateVersions() {
        List<String> lines = List.of("8.0.0,100", "8.0.0,101");

        var ex = assertThrows(
            IllegalStateException.class,
            () -> TagVersionsTask.addVersionRecord(new ArrayList<>(lines), Version.fromString("8.0.1"), 102)
        );
        assertThat(ex.getMessage(), is("Duplicate key 8.0.0 (attempted merging values 100 and 101)"));
    }
}
