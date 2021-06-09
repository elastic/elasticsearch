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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Generates the release notes i.e. list of changes that have gone into this release. They are grouped by the
 * type of change, then by team area.
 */
public class ReleaseNotesGenerator implements Closeable {
    /**
     * These mappings translate change types into the headings as they should appears in the release notes.
     */
    private static final Map<String, String> TYPE_LABELS = new HashMap<>();

    static {
        TYPE_LABELS.put("breaking", "Breaking changes");
        TYPE_LABELS.put("breaking-java", "Breaking Java changes");
        TYPE_LABELS.put("deprecation", "Deprecations");
        TYPE_LABELS.put("feature", "New features");
        TYPE_LABELS.put("enhancement", "Enhancements");
        TYPE_LABELS.put("bug", "Bug fixes");
        TYPE_LABELS.put("regression", "Regressions");
        TYPE_LABELS.put("upgrade", "Upgrades");
    }

    private final PrintStream out;

    public ReleaseNotesGenerator(File outputFile) throws FileNotFoundException {
        this.out = new PrintStream(outputFile);
    }

    @Override
    public void close() throws IOException {
        this.out.close();
    }

    public void generate(List<ChangelogEntry> changelogs) {
        final Version elasticsearchVersion = VersionProperties.getElasticsearchVersion();

        final Predicate<Version> includedInSameMinor = v -> v.getMajor() == elasticsearchVersion.getMajor()
            && v.getMinor() == elasticsearchVersion.getMinor();

        final Map<Version, List<ChangelogEntry>> changelogsByVersion = changelogs.stream()
            .collect(
                Collectors.groupingBy(
                    entry -> entry.getVersions()
                        .stream()
                        .map(v -> Version.fromString(v.replaceFirst("^v", "")))
                        .filter(includedInSameMinor)
                        .sorted()
                        .findFirst()
                        .get(),
                    Collectors.toList()
                )
            );

        final List<Version> changelogVersions = new ArrayList<>(changelogsByVersion.keySet());
        changelogVersions.sort(Version::compareTo);
        Collections.reverse(changelogVersions);

        for (Version version : changelogVersions) {
            generateForVersion(version, changelogsByVersion.get(version));
        }
    }

    private void generateForVersion(Version version, List<ChangelogEntry> changelogs) {
        Map<String, List<ChangelogEntry>> groupedChangelogs = changelogs.stream()
            .collect(
                Collectors.groupingBy(
                    // Breaking changes come first in the output
                    entry -> entry.getBreaking() == null ? entry.getType() : "breaking",
                    TreeMap::new,
                    Collectors.toList()
                )
            );

        generateHeader(version);

        groupedChangelogs.forEach((type, changelogsByType) -> {
            generateGroupHeader(version, type);

            final TreeMap<String, List<ChangelogEntry>> changelogsByArea = changelogsByType.stream()
                .collect(Collectors.groupingBy(ChangelogEntry::getArea, TreeMap::new, Collectors.toList()));

            changelogsByArea.forEach((area, perAreaChangelogs) -> {
                out.println(area + "::");

                // Generate the output lines first so that we can sort them
                perAreaChangelogs.stream().map(log -> {
                    final StringBuilder sb = new StringBuilder(
                        "* " + log.getSummary() + " {es-pull}" + log.getPr() + "[#" + log.getPr() + "]"
                    );
                    final List<Integer> issues = log.getIssues();
                    if (issues != null && issues.isEmpty() == false) {
                        sb.append(issues.size() == 1 ? " (issue: " : " (issues: ");
                        sb.append(issues.stream().map(i -> "{es-issue}" + i + "[#" + i + "]").collect(Collectors.joining(", ")));
                        sb.append(")");
                    }
                    return sb.toString();
                }).sorted().forEach(out::println);

                out.println();
            });

            out.println();
        });
    }

    private void generateHeader(Version version) {
        String branch = version.getMajor() + "." + version.getMinor();

        out.println("[[release-notes-" + version + "]]");
        out.println("== {es} version " + version);

        if (version.getQualifiers().contains("SNAPSHOT")) {
            out.println();
            out.println("coming[" + version + "]");
        }

        out.println();
        out.println("Also see <<breaking-changes-" + branch + ",Breaking changes in " + branch + ">>.");
        out.println();
    }

    private void generateGroupHeader(Version version, String type) {
        out.println("[[" + type + "-" + version + "]]");
        out.println("[discrete]");
        out.println("=== " + TYPE_LABELS.get(type));
        out.println();
    }
}
