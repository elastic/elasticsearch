/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.release;

import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Generates the page that lists the breaking changes and deprecations for a minor version release.
 */
public class BreakingChangesGenerator implements Closeable {

    private final PrintStream out;

    public BreakingChangesGenerator(File outputFile) throws FileNotFoundException {
        this.out = new PrintStream(outputFile);
    }

    @Override
    public void close() throws IOException {
        this.out.close();
    }

    public void generate(List<ChangelogEntry> entries) {
        Version version = VersionProperties.getElasticsearchVersion();
        final VersionStrings versionStrings = new VersionStrings(version);

        List.of(
            "[[migrating-" + versionStrings.majorDotMinor + "]]",
            "== Migrating to " + versionStrings.majorDotMinor,
            "++++",
            "<titleabbrev>" + versionStrings.majorDotMinor + "</titleabbrev>",
            "++++",
            "",
            "This section discusses the changes that you need to be aware of when migrating",
            "your application to {es} " + versionStrings.majorDotMinor + ".",
            "",
            "See also <<release-highlights>> and <<es-release-notes>>."
        ).forEach(out::println);

        if (VersionProperties.isElasticsearchSnapshot()) {
            out.println();
            out.println("coming[" + version + "]");
        }

        out.println();
        out.println("//NOTE: The notable-breaking-changes tagged regions are re-used in the");
        out.println("//Installation and Upgrade Guide");

        generateBreakingChanges(entries, versionStrings);
        generateDeprecations(entries, versionStrings);
    }

    private static class VersionStrings {
        private final String majorMinor;
        private final String majorDotMinor;
        private final String nextMajor;

        private VersionStrings(Version version) {
            this.majorMinor = String.valueOf(version.getMajor()) + version.getMinor();
            this.majorDotMinor = version.getMajor() + "." + version.getMinor();
            this.nextMajor = (version.getMajor() + 1) + ".0";
        }
    }

    private void generateBreakingChanges(List<ChangelogEntry> entries, VersionStrings versionStrings) {
        final Map<String, List<ChangelogEntry.Breaking>> breakingChangesByArea = entries.stream()
            .map(ChangelogEntry::getBreaking)
            .filter(Objects::nonNull)
            .collect(Collectors.groupingBy(ChangelogEntry.Breaking::getArea, TreeMap::new, Collectors.toList()));

        if (breakingChangesByArea.isEmpty()) {
            return;
        }

        out.println();
        out.println();

        List.of(
            "[discrete]",
            "[[breaking-changes-" + versionStrings.majorDotMinor + "]]",
            "=== Breaking changes",
            "",
            "The following changes in {es} " + versionStrings.majorDotMinor + " might affect your applications",
            "and prevent them from operating normally.",
            "Before upgrading to " + versionStrings.majorDotMinor + " review these changes and take the described steps",
            "to mitigate the impact.",
            "",
            "NOTE: Breaking changes introduced in minor versions are",
            "normally limited to security and bug fixes.",
            "Significant changes in behavior are deprecated in a minor release and",
            "the old behavior is supported until the next major release.",
            "To find out if you are using any deprecated functionality,",
            "enable <<deprecation-logging, deprecation logging>>."
        ).forEach(out::println);

        breakingChangesByArea.forEach((area, breakingChanges) -> {
            out.println();

            final boolean hasNotableChanges = breakingChanges.stream().anyMatch(ChangelogEntry.Breaking::isNotable);
            if (hasNotableChanges) {
                out.println("// tag::notable-breaking-changes[]");
            }

            out.println("[discrete]");
            out.println(
                "[[breaking_" + versionStrings.majorMinor + "_" + area.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "_") + "]]"
            );
            out.println("==== " + area);

            breakingChanges.forEach(breaking -> {
                out.println();
                out.println("[[" + breaking.getAnchor() + "]]");
                out.println("." + breaking.getTitle());
                out.println("[%collapsible]");
                out.println("====");
                out.println("*Details* +");
                out.println(breaking.getDetails().trim());
                out.println();
                out.println("*Impact* +");
                out.println(breaking.getImpact().trim());
                out.println("====");
            });

            if (hasNotableChanges) {
                out.println("// end::notable-breaking-changes[]");
            }
        });
    }

    private void generateDeprecations(List<ChangelogEntry> entries, VersionStrings versionStrings) {
        final Map<String, List<ChangelogEntry.Deprecation>> deprecationsByArea = entries.stream()
            .map(ChangelogEntry::getDeprecation)
            .filter(Objects::nonNull)
            .collect(Collectors.groupingBy(ChangelogEntry.Deprecation::getArea, TreeMap::new, Collectors.toList()));

        if (deprecationsByArea.isEmpty()) {
            return;
        }

        out.println();
        out.println();

        List.of(
            "[discrete]",
            "[[deprecated-" + versionStrings.majorDotMinor + "]]",
            "=== Deprecations",
            "",
            "The following functionality has been deprecated in {es} " + versionStrings.majorDotMinor,
            "and will be removed in " + versionStrings.nextMajor + ".",
            "While this won't have an immediate impact on your applications,",
            "we strongly encourage you take the described steps to update your code",
            "after upgrading to " + versionStrings.majorDotMinor + ".",
            "",
            "NOTE: Significant changes in behavior are deprecated in a minor release and",
            "the old behavior is supported until the next major release.",
            "To find out if you are using any deprecated functionality,",
            "enable <<deprecation-logging, deprecation logging>>."
        ).forEach(out::println);

        deprecationsByArea.forEach((area, deprecations) -> {
            out.println();

            out.println("[discrete]");
            out.println(
                "[[deprecations-" + versionStrings.majorMinor + "_" + area.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "_") + "]]"
            );
            out.println("==== " + area + " deprecations");

            deprecations.forEach(deprecation -> {
                out.println();
                out.println("[[" + deprecation.getAnchor() + "]]");
                out.println("." + deprecation.getTitle());
                out.println("[%collapsible]");
                out.println("====");
                out.println("*Details* +");
                out.println(deprecation.getBody().trim());
                out.println("====");
            });
        });
    }
}
