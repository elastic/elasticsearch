/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import org.gradle.api.GradleException;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.Test;

import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;

class ValidateChangelogEntryTaskTest {

    @Test
    void test_prNumber_isRequired() {
        ChangelogEntry changelog = new ChangelogEntry();
        changelog.setType("enhancement");

        final String message = doValidate(changelog);

        assertThat(message, endsWith("must provide a [pr] number (only 'known-issue' and 'security' entries can omit this"));
    }

    @Test
    void test_prNumber_notRequired() {
        Stream.of("known-issue", "security").forEach(type -> {
            ChangelogEntry changelog = new ChangelogEntry();
            changelog.setType(type);

            // Should not throw an exception!
            ValidateChangelogEntryTask.validate("", changelog);
        });
    }

    @Test
    void test_area_isRequired() {
        final ChangelogEntry changelog = new ChangelogEntry();
        changelog.setType("enhancement");
        changelog.setPr(123);

        final String message = doValidate(changelog);

        assertThat(message, endsWith("must provide an [area] (only 'known-issue' and 'security' entries can omit this"));
    }

    @Test
    void test_breaking_requiresBreakingSection() {
        Stream.of("breaking", "breaking-java").forEach(type -> {
            final ChangelogEntry changelog = buildChangelog(type);

            final String message = doValidate(changelog);

            assertThat(message, endsWith("has type [" + type + "] and must supply a [breaking] section with further information"));
        });
    }

    @Test
    void test_breaking_rejectsTripleBackticksInDetails() {
        Stream.of("breaking", "breaking-java").forEach(type -> {
            final ChangelogEntry.Breaking breaking = new ChangelogEntry.Breaking();
            breaking.setDetails("""
                Some waffle.
                ```
                I AM CODE!
                ```
                """);

            final ChangelogEntry changelog = buildChangelog(type);
            changelog.setBreaking(breaking);

            final String message = doValidate(changelog);

            assertThat(message, containsString("uses a triple-backtick in the [breaking.details] section"));
        });
    }

    @Test
    void test_breaking_rejectsTripleBackticksInImpact() {
        Stream.of("breaking", "breaking-java").forEach(type -> {
            final ChangelogEntry.Breaking breaking = new ChangelogEntry.Breaking();
            breaking.setDetails("Waffle waffle");
            breaking.setImpact("""
                More waffle.
                ```
                THERE ARE WEASEL RAKING THROUGH MY GARBAGE!
                ```
                """);

            final ChangelogEntry changelog = buildChangelog(type);
            changelog.setBreaking(breaking);

            final String message = doValidate(changelog);

            assertThat(message, containsString("uses a triple-backtick in the [breaking.impact] section"));
        });
    }

    @Test
    void test_deprecation_rejectsTripleBackticksInImpact() {
        final ChangelogEntry.Deprecation deprecation = new ChangelogEntry.Deprecation();
        deprecation.setDetails("Waffle waffle");
        deprecation.setImpact("""
            More waffle.
            ```
            THERE ARE WEASEL RAKING THROUGH MY GARBAGE!
            ```
            """);

        final ChangelogEntry changelog = buildChangelog("deprecation");
        changelog.setDeprecation(deprecation);

        final String message = doValidate(changelog);

        assertThat(message, containsString("uses a triple-backtick in the [deprecation.impact] section"));
    }

    @Test
    void test_deprecation_rejectsTripleBackticksInDetails() {
        final ChangelogEntry.Deprecation deprecation = new ChangelogEntry.Deprecation();
        deprecation.setDetails("""
            Some waffle.
            ```
            I AM CODE!
            ```
            """);

        final ChangelogEntry changelog = buildChangelog("deprecation");
        changelog.setDeprecation(deprecation);

        final String message = doValidate(changelog);

        assertThat(message, containsString("uses a triple-backtick in the [deprecation.details] section"));
    }

    @Test
    void test_highlight_rejectsTripleBackticksInBody() {
        final ChangelogEntry.Highlight highlight = new ChangelogEntry.Highlight();
        highlight.setBody("""
            Some waffle.
            ```
            I AM CODE!
            ```
            """);

        final ChangelogEntry changelog = buildChangelog("enhancement");
        changelog.setHighlight(highlight);

        final String message = doValidate(changelog);

        assertThat(message, containsString("uses a triple-backtick in the [highlight.body] section"));
    }

    private static ChangelogEntry buildChangelog(String type) {
        final ChangelogEntry changelog = new ChangelogEntry();
        changelog.setType(type);
        changelog.setPr(123);
        changelog.setArea("Infra/Core");
        return changelog;
    }

    private String doValidate(ChangelogEntry entry) {
        try {
            ValidateChangelogEntryTask.validate("docs/123.yaml", entry);
            throw new AssertionError("No exception thrown!");
        } catch (Exception e) {
            assertThat(e, Matchers.instanceOf(GradleException.class));
            return e.getMessage();
        }
    }
}
