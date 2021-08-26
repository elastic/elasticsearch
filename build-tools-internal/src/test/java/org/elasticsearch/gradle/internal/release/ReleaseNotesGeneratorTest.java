/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import org.junit.Test;

import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class ReleaseNotesGeneratorTest {

    /**
     * Check that the release notes can be correctly generated.
     */
    @Test
    public void generateFile_rendersCorrectMarkup() throws Exception {
        // given:
        final String template = getResource("/templates/release-notes.asciidoc");
        final String expectedOutput = getResource(
            "/org/elasticsearch/gradle/internal/release/ReleaseNotesGeneratorTest.generateFile.asciidoc"
        );
        final StringWriter writer = new StringWriter();
        final Map<QualifiedVersion, Set<ChangelogEntry>> entries = getEntries();

        // when:
        ReleaseNotesGenerator.generateFile(template, entries, writer);
        final String actualOutput = writer.toString();

        // then:
        assertThat(actualOutput, equalTo(expectedOutput));
    }

    private Map<QualifiedVersion, Set<ChangelogEntry>> getEntries() {
        final Set<ChangelogEntry> entries_8_2_0 = new HashSet<>();
        entries_8_2_0.addAll(buildEntries(1, 2));
        entries_8_2_0.addAll(buildEntries(2, 2));
        entries_8_2_0.addAll(buildEntries(3, 2));

        final Set<ChangelogEntry> entries_8_1_0 = new HashSet<>();
        entries_8_1_0.addAll(buildEntries(4, 2));
        entries_8_1_0.addAll(buildEntries(5, 2));
        entries_8_1_0.addAll(buildEntries(6, 2));

        final Set<ChangelogEntry> entries_8_0_0 = new HashSet<>();
        entries_8_0_0.addAll(buildEntries(7, 2));
        entries_8_0_0.addAll(buildEntries(8, 2));
        entries_8_0_0.addAll(buildEntries(9, 2));

        // Security issues are presented first in the notes
        final ChangelogEntry securityEntry = new ChangelogEntry();
        securityEntry.setArea("Security");
        securityEntry.setType("security");
        securityEntry.setSummary("Test security issue");
        entries_8_2_0.add(securityEntry);

        // known issues are presented after security issues
        final ChangelogEntry knownIssue = new ChangelogEntry();
        knownIssue.setArea("Search");
        knownIssue.setType("known-issue");
        knownIssue.setSummary("Test known issue");
        entries_8_1_0.add(knownIssue);

        final Map<QualifiedVersion, Set<ChangelogEntry>> result = new HashMap<>();

        result.put(QualifiedVersion.of("8.2.0-SNAPSHOT"), entries_8_2_0);
        result.put(QualifiedVersion.of("8.1.0"), entries_8_1_0);
        result.put(QualifiedVersion.of("8.0.0"), entries_8_0_0);

        return result;
    }

    private List<ChangelogEntry> buildEntries(int seed, int count) {
        // Sample of possible areas from `changelog-schema.json`
        final List<String> areas = List.of("Aggregation", "Cluster", "Indices", "Mappings", "Search", "Security");
        // Possible change types, with `breaking`, `breaking-java`, `known-issue` and `security` removed.
        final List<String> types = List.of("bug", "deprecation", "enhancement", "feature", "new-aggregation", "regression", "upgrade");

        final String area = areas.get(seed % areas.size());
        final String type = types.get(seed % types.size());

        final List<ChangelogEntry> entries = new ArrayList<>(count);

        int base = seed * 1000;

        for (int i = 0; i < count; i++) {

            final ChangelogEntry e = new ChangelogEntry();
            e.setPr(base++);
            e.setArea(area);
            e.setSummary("Test changelog entry " + seed + "_" + i);
            e.setType(type);

            List<Integer> issues = new ArrayList<>(count);
            for (int j = 0; j <= i; j++) {
                issues.add(base++);
            }
            e.setIssues(issues);

            entries.add(e);
        }

        return entries;
    }

    private String getResource(String name) throws Exception {
        return Files.readString(Paths.get(Objects.requireNonNull(this.getClass().getResource(name)).toURI()), StandardCharsets.UTF_8);
    }
}
