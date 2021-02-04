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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Generates the release highlights notes, for changelog files that contain the <code>highlight</code> field.
 */
public class ReleaseHighlightsGenerator implements Closeable {

    private final PrintStream out;

    public ReleaseHighlightsGenerator(File outputFile) throws FileNotFoundException {
        this.out = new PrintStream(outputFile);
    }

    @Override
    public void close() throws IOException {
        this.out.close();
    }

    public void generate(List<ChangelogEntry> entries) {
        List.of(
            "[[release-highlights]]",
            "== What's new in {minor-version}",
            "",
            "coming[{minor-version}]",
            "",
            "Here are the highlights of what's new and improved in {es} {minor-version}!",
            "ifeval::[\"{release-state}\"!=\"unreleased\"]",
            "For detailed information about this release, see the",
            "<<release-notes-{elasticsearch_version}, Release notes >> and",
            "<<breaking-changes-{minor-version}, Breaking changes>>.",
            "endif::[]",
            ""
        ).forEach(this.out::println);

        Version version = VersionProperties.getElasticsearchVersion();

        if (version.getMinor() > 0) {
            this.out.println("// Add previous release to the list");
            this.out.println("Other versions:");

            List<String> priorVersions = new ArrayList<>();

            final int major = version.getMajor();
            for (int minor = version.getMinor(); minor >= 0; minor--) {
                String majorMinor = major + "." + minor;
                String fileSuffix = "";
                if (major == 7 && minor < 7) {
                    fileSuffix = "-" + majorMinor + ".0";
                }
                priorVersions.add("{ref-bare}/" + majorMinor + "/release-highlights" + fileSuffix + ".html[" + majorMinor + "]");
            }

            this.out.println(String.join("\n| ", priorVersions));
            this.out.println();
        }

        final Map<Boolean, List<ChangelogEntry.Highlight>> groupedHighlights = entries.stream()
            .map(ChangelogEntry::getHighlight)
            .filter(Objects::nonNull)
            .collect(Collectors.groupingBy(ChangelogEntry.Highlight::isNotable, Collectors.toList()));

        final List<ChangelogEntry.Highlight> notableHighlights = groupedHighlights.getOrDefault(true, List.of());
        final List<ChangelogEntry.Highlight> nonNotableHighlights = groupedHighlights.getOrDefault(false, List.of());

        if (notableHighlights.isEmpty() == false) {
            this.out.println("// tag::notable-highlights[]");

            for (ChangelogEntry.Highlight highlight : notableHighlights) {
                out.println("[discrete]");
                out.println("=== " + highlight.getTitle());
                out.println(highlight.getBody().trim());
                out.println();
            }

            this.out.println("// end::notable-highlights[]");
        }

        this.out.println();

        for (ChangelogEntry.Highlight highlight : nonNotableHighlights) {
            out.println("[discrete]");
            out.println("=== " + highlight.getTitle());
            out.println(highlight.getBody());
            out.println();
        }
    }
}
