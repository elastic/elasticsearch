/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class ReleaseNotesGeneratorTest {

    private static final List<String> CHANGE_TYPES = List.of(
        "breaking",
        "breaking-java",
        "bug",
        "fixes",
        "deprecation",
        "enhancement",
        "feature",
        "features-enhancements",
        "new-aggregation",
        "regression",
        "upgrade"
    );

    @Test
    public void generateFile_index_rendersCorrectMarkup() throws Exception {
        testTemplate("index.md");
    }

    @Test
    public void generateFile_breakingChanges_rendersCorrectMarkup() throws Exception {
        testTemplate("breaking-changes.md");
    }

    @Test
    public void generateFile_deprecations_rendersCorrectMarkup() throws Exception {
        testTemplate("deprecations.md");
    }

    public void testTemplate(String templateFilename) throws Exception {
        // given:
        final String template = getResource("/templates/" + templateFilename);
        final String expectedOutput = getResource(
            "/org/elasticsearch/gradle/internal/release/ReleaseNotesGeneratorTest." + templateFilename
        );

        final Set<ChangelogEntry> entries = getEntries();

        // when:
        final String actualOutput = ReleaseNotesGenerator.generateFile(template, QualifiedVersion.of("8.2.0-SNAPSHOT"), entries);

        // then:
        assertThat(actualOutput, equalTo(expectedOutput));
    }

    private Set<ChangelogEntry> getEntries() {
        final Set<ChangelogEntry> entries = new HashSet<>();
        for (int i = 0; i < CHANGE_TYPES.size(); i++) {
            entries.addAll(buildEntries(i, 2));
        }

        return entries;
    }

    private List<ChangelogEntry> buildEntries(int seed, int count) {
        // Sample of possible areas from `changelog-schema.json`
        final List<String> areas = List.of("Aggregation", "Cluster", "Indices", "Mappings", "Search", "Security");

        final String area = areas.get(seed % areas.size());
        final String type = CHANGE_TYPES.get(seed % CHANGE_TYPES.size());

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
