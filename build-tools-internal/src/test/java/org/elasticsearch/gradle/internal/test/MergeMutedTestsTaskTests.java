/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import org.gradle.api.GradleException;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;

/**
 * Exercises the three-way entry merge behind {@code mergeMuted} on small file snippets. The git plumbing around it is covered by
 * the functional test.
 */
public class MergeMutedTestsTaskTests {

    private static final String A = """
        - class: org.acme.ATests
          method: testA
          issue: https://github.com/elastic/elasticsearch/issues/1
        """;
    private static final String B = """
        - class: org.acme.BTests
          method: testB {p0=true}
          issue: https://github.com/elastic/elasticsearch/issues/2
        """;
    private static final String C = """
        - class: org.acme.CTests
          tasks:
          - :server:test
          method: testC
          issue: https://github.com/elastic/elasticsearch/issues/3
        """;
    private static final String C_REORDERED = """
        - class: org.acme.CTests
          method: testC
          issue: https://github.com/elastic/elasticsearch/issues/3
          tasks:
          - :server:test
        """;
    private static final String C_OTHER_ISSUE = """
        - class: org.acme.CTests
          tasks:
          - :server:test
          method: testC
          issue: https://github.com/elastic/elasticsearch/issues/99
        """;
    private static final String D = """
        - class: org.acme.DTests
          issue: https://github.com/elastic/elasticsearch/issues/4
        """;
    private static final String COMMENT = """
        # Examples:
        #  - class: org.acme.ExampleTests
        #    method: testExample
        """;
    private static final String BASE = "tests:\n" + A + COMMENT + B;

    @Test
    public void testNoIncomingChangesLeavesReceivingUntouched() {
        MergeMutedTestsTask.Result result = MergeMutedTestsTask.merge(BASE, BASE + D, BASE);

        assertThat(result.content(), equalTo(BASE + D));
        assertThat(result.added(), equalTo(0));
        assertThat(result.removed(), equalTo(0));
    }

    @Test
    public void testAdditionIsAppendedAtTheEnd() {
        MergeMutedTestsTask.Result result = MergeMutedTestsTask.merge(BASE, BASE, BASE + C);

        assertThat(result.content(), equalTo(BASE + C));
        assertThat(result.added(), equalTo(1));
    }

    /** The real-world conflict: both sides appended after the same last entry. */
    @Test
    public void testBothSidesAppendDifferentEntries() {
        MergeMutedTestsTask.Result result = MergeMutedTestsTask.merge(BASE, BASE + D, BASE + C);

        assertThat(result.content(), equalTo(BASE + D + C));
        assertThat(result.added(), equalTo(1));
    }

    @Test
    public void testIdenticalAdditionWithDifferentKeyOrderIsIgnored() {
        MergeMutedTestsTask.Result result = MergeMutedTestsTask.merge(BASE, BASE + C_REORDERED, BASE + C);

        assertThat(result.content(), equalTo(BASE + C_REORDERED));
        assertThat(result.added(), equalTo(0));
    }

    @Test
    public void testAdditionConflictingWithReceivingFails() {
        GradleException e = assertThrows(GradleException.class, () -> MergeMutedTestsTask.merge(BASE, BASE + C_OTHER_ISSUE, BASE + C));

        assertThat(e.getMessage(), containsString("[org.acme.CTests testC] exists on both sides with different content"));
        assertThat(e.getMessage(), containsString("issues/99"));
        assertThat(e.getMessage(), containsString("issues/3"));
    }

    /** Removing an entry drops only its own lines; the comment block that followed it stays where it was. */
    @Test
    public void testRemovalInTheMiddleKeepsSurroundingLines() {
        MergeMutedTestsTask.Result result = MergeMutedTestsTask.merge(BASE, BASE + D, "tests:\n" + COMMENT + B);

        assertThat(result.content(), equalTo("tests:\n" + COMMENT + B + D));
        assertThat(result.removed(), equalTo(1));
    }

    /** Incoming deleted a mute whose tasks or issue the receiving branch changed. That is a modify/delete conflict. */
    @Test
    public void testRemovalOfEntryModifiedOnReceivingFails() {
        String changedIssue = A.replace("issues/1", "issues/100");
        GradleException issue = assertThrows(
            GradleException.class,
            () -> MergeMutedTestsTask.merge(BASE, "tests:\n" + changedIssue + COMMENT + B, "tests:\n" + COMMENT + B)
        );
        assertThat(issue.getMessage(), containsString("[org.acme.ATests testA] was modified on the receiving branch and removed"));
        assertThat(issue.getMessage(), containsString("issues/1"));
        assertThat(issue.getMessage(), containsString("issues/100"));

        String changedTasks = C.replace("- :server:test", "- :server:internalClusterTest");
        String base = "tests:\n" + C + B;
        GradleException tasks = assertThrows(
            GradleException.class,
            () -> MergeMutedTestsTask.merge(base, "tests:\n" + changedTasks + B, "tests:\n" + B)
        );
        assertThat(tasks.getMessage(), containsString("[org.acme.CTests testC] was modified on the receiving branch and removed"));
        assertThat(tasks.getMessage(), containsString(":server:test"));
        assertThat(tasks.getMessage(), containsString(":server:internalClusterTest"));
    }

    @Test
    public void testRemovalOfMissingEntryIsIgnored() {
        MergeMutedTestsTask.Result result = MergeMutedTestsTask.merge(BASE, "tests:\n" + COMMENT + B + D, "tests:\n" + COMMENT + B);

        assertThat(result.content(), equalTo("tests:\n" + COMMENT + B + D));
        assertThat(result.removed(), equalTo(0));
    }

    /** An entry modified only by the incoming change replaces the receiving branch's copy in place. */
    @Test
    public void testIncomingModificationIsApplied() {
        String modifiedA = A.replace("issues/1", "issues/100");

        MergeMutedTestsTask.Result result = MergeMutedTestsTask.merge(BASE, BASE + D, "tests:\n" + modifiedA + COMMENT + B);

        assertThat(result.content(), equalTo("tests:\n" + modifiedA + COMMENT + B + D));
        assertThat(result.modified(), equalTo(1));
    }

    @Test
    public void testSameModificationOnBothSidesIsIgnored() {
        String modified = "tests:\n" + A.replace("issues/1", "issues/100") + COMMENT + B;

        MergeMutedTestsTask.Result result = MergeMutedTestsTask.merge(BASE, modified + D, modified);

        assertThat(result.content(), equalTo(modified + D));
        assertThat(result.modified(), equalTo(0));
    }

    @Test
    public void testModificationConflictingWithReceivingFails() {
        String receivingA = A.replace("issues/1", "issues/200");
        String incomingA = A.replace("issues/1", "issues/100");

        GradleException e = assertThrows(
            GradleException.class,
            () -> MergeMutedTestsTask.merge(BASE, "tests:\n" + receivingA + COMMENT + B, "tests:\n" + incomingA + COMMENT + B)
        );

        assertThat(e.getMessage(), containsString("[org.acme.ATests testA] was modified differently on both sides"));
        assertThat(e.getMessage(), containsString("issues/200"));
        assertThat(e.getMessage(), containsString("issues/100"));
    }

    /** Incoming changed a mute that the receiving branch deleted. That is a modify/delete conflict. */
    @Test
    public void testModificationOfEntryRemovedOnReceivingFails() {
        String modifiedA = A.replace("issues/1", "issues/100");

        GradleException e = assertThrows(
            GradleException.class,
            () -> MergeMutedTestsTask.merge(BASE, "tests:\n" + COMMENT + B, "tests:\n" + modifiedA + COMMENT + B)
        );

        assertThat(e.getMessage(), containsString("[org.acme.ATests testA] was removed on the receiving branch and modified"));
        assertThat(e.getMessage(), containsString("issues/100"));
    }

    @Test
    public void testEntryWithoutClassFails() {
        GradleException e = assertThrows(
            GradleException.class,
            () -> MergeMutedTestsTask.merge(BASE, BASE, BASE + "- method: testNoClass\n  issue: https://example.com\n")
        );

        assertThat(e.getMessage(), containsString("has no class"));
    }

    /**
     * A blank line between properties is still that entry. Removing it must take the following {@code method}, {@code tasks},
     * and {@code issue} lines with it, rather than leaving them as a block with no class.
     */
    @Test
    public void testBlankLineInsideEntryIsRemovedWithTheEntry() {
        String entry = """
            - class: org.acme.CTests
              tasks:
              - :server:test

              method: testC
              issue: https://github.com/elastic/elasticsearch/issues/3
            """;
        String base = "tests:\n" + entry + B;

        MergeMutedTestsTask.Result result = MergeMutedTestsTask.merge(base, base + D, "tests:\n" + B);

        assertThat(result.content(), equalTo("tests:\n" + B + D));
        assertThat(result.removed(), equalTo(1));
    }

    @Test
    public void testBlankLineInsideAddedEntryIsKept() {
        String entry = """
            - class: org.acme.ETests
              method: testE

              issue: https://github.com/elastic/elasticsearch/issues/5
            """;

        MergeMutedTestsTask.Result result = MergeMutedTestsTask.merge(BASE, BASE, BASE + entry);

        assertThat(result.content(), equalTo(BASE + entry));
        assertThat(result.added(), equalTo(1));
    }

    /** Changing a property that follows an internal blank line is still a modification of that one entry. */
    @Test
    public void testChangeAfterInternalBlankLineIsAModification() {
        String entry = """
            - class: org.acme.CTests
              tasks:
              - :server:test

              method: testC
              issue: https://github.com/elastic/elasticsearch/issues/3
            """;
        String changed = entry.replace("issues/3", "issues/99");

        MergeMutedTestsTask.Result result = MergeMutedTestsTask.merge("tests:\n" + entry, "tests:\n" + entry, "tests:\n" + changed);

        assertThat(result.content(), equalTo("tests:\n" + changed));
        assertThat(result.modified(), equalTo(1));
        assertThat(result.added(), equalTo(0));
    }

    /** A blank line after an entry, before the next one, is a separator and stays when the preceding entry is removed. */
    @Test
    public void testBlankLineBetweenEntriesIsNotPartOfTheEntry() {
        String base = "tests:\n" + A + "\n" + B;

        MergeMutedTestsTask.Result result = MergeMutedTestsTask.merge(base, base, "tests:\n" + "\n" + B);

        assertThat(result.content(), equalTo("tests:\n" + "\n" + B));
        assertThat(result.removed(), equalTo(1));
    }
}
