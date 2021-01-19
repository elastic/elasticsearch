/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.release;

import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

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
        Map<String, List<ChangelogEntry>> groupedChangelogs = changelogs.stream()
            .collect(
                Collectors.groupingBy(
                    // Breaking changes come first in the output
                    entry -> entry.getBreaking() == null ? entry.getType() : "breaking",
                    TreeMap::new,
                    Collectors.toList()
                )
            );

        generateHeader();

        groupedChangelogs.forEach((type, changelogsByType) -> {
            generateGroupHeader(type);

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

    private void generateHeader() {
        final Version version = VersionProperties.getElasticsearchVersion();
        String branch = version.getMajor() + "." + version.getMinor();

        out.println("[[release-notes-" + version + "]]");
        out.println("== {es} version " + version);

        if (VersionProperties.isElasticsearchSnapshot()) {
            out.println();
            out.println("coming[" + version + "]");
        }

        out.println();
        out.println("Also see <<breaking-changes-" + branch + ",Breaking changes in " + branch + ">>.");
        out.println();
    }

    private void generateGroupHeader(String type) {
        out.println("[[" + type + "-" + VersionProperties.getElasticsearchVersion() + "]]");
        out.println("[float]");
        out.println("=== " + TYPE_LABELS.get(type));
        out.println();
    }
}
