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
        final Version version = VersionProperties.getElasticsearchVersion();

        String majorDotMinor = version.getMajor() + "." + version.getMinor();
        String majorMinor = String.valueOf(version.getMajor()) + version.getMinor();

        List.of(
            "[[breaking-changes-" + majorDotMinor + "]]",
            "== Breaking changes in " + majorDotMinor,
            "++++",
            "<titleabbrev>" + majorDotMinor + "</titleabbrev>",
            "++++",
            "",
            "This section discusses the changes that you need to be aware of when migrating",
            "your application to {es} " + majorDotMinor + ".",
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

        final Map<String, List<ChangelogEntry.Breaking>> breakingChangesByArea = entries.stream()
            .map(ChangelogEntry::getBreaking)
            .filter(Objects::nonNull)
            .collect(Collectors.groupingBy(ChangelogEntry.Breaking::getArea, TreeMap::new, Collectors.toList()));

        breakingChangesByArea.forEach((area, breakingChanges) -> {
            out.println();

            final boolean hasNotableChanges = breakingChanges.stream().anyMatch(ChangelogEntry.Breaking::isNotable);
            if (hasNotableChanges) {
                out.println("// tag::notable-breaking-changes[]");
            }

            out.println("[discrete]");
            out.println("[[breaking_" + majorMinor + "_" + area.toLowerCase(Locale.ROOT).replaceAll("[^a-z0-9]+", "_") + "]]");
            out.println("=== " + area);

            breakingChanges.forEach(breaking -> {
                out.println();
                out.println("[[" + breaking.getAnchor() + "]]");
                out.println("." + breaking.getTitle());
                out.println("[%collapsible]");
                out.println("====");
                out.println("*Details* +");
                out.println(breaking.getBody().trim());
                out.println("====");
            });

            if (hasNotableChanges) {
                out.println("// end::notable-breaking-changes[]");
            }
        });
    }
}
