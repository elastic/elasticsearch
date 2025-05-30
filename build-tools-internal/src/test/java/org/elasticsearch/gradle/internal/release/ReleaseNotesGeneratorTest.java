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
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.gradle.internal.release.GenerateReleaseNotesTask.getSortedBundlesWithUniqueChangelogs;
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
    public void generateFile_index_noHighlights_rendersCorrectMarkup() throws Exception {
        var bundles = getBundles();
        bundles = bundles.stream().filter(b -> false == b.version().equals("9.1.0")).toList();

        testTemplate("index.md", "index.no-highlights.md", bundles);
    }

    @Test
    public void generateFile_index_noChanges_rendersCorrectMarkup() throws Exception {
        var bundles = new ArrayList<ChangelogBundle>();

        testTemplate("index.md", "index.no-changes.md", bundles);
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
        testTemplate(templateFilename, templateFilename, null);
    }

    public void testTemplate(String templateFilename, String outputFilename) throws Exception {
        testTemplate(templateFilename, outputFilename, null);
    }

    public void testTemplate(String templateFilename, String outputFilename, List<ChangelogBundle> bundles) throws Exception {
        // given:
        final String template = getResource("/templates/" + templateFilename);
        final String expectedOutput = getResource("/org/elasticsearch/gradle/internal/release/ReleaseNotesGeneratorTest." + outputFilename);

        if (bundles == null) {
            bundles = getBundles();
        }

        bundles = getSortedBundlesWithUniqueChangelogs(bundles);

        // when:
        final String actualOutput = ReleaseNotesGenerator.generateFile(template, bundles);

        // then:
        assertThat(actualOutput, equalTo(expectedOutput));
    }

    private List<ChangelogBundle> getBundles() {
        List<ChangelogBundle> bundles = new ArrayList<>();

        for (int i = 0; i < CHANGE_TYPES.size(); i++) {
            bundles.add(
                new ChangelogBundle(
                    "9.0." + i,
                    i != CHANGE_TYPES.size() - 1,
                    "2025-05-16T00:00:" + String.format("%d", 10 + i),
                    buildEntries(i, 2)
                )
            );
        }

        final List<ChangelogEntry> entries = new ArrayList<>();
        entries.add(makeHighlightsEntry(51, false));
        entries.add(makeHighlightsEntry(50, true));
        entries.add(makeHighlightsEntry(52, true));

        bundles.add(new ChangelogBundle("9.1.0", false, "2025-05-17T00:00:00", entries));

        return bundles;
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

    private List<ChangelogEntry> getHighlightsEntries() {
        ChangelogEntry entry123 = makeHighlightsEntry(123, true);
        ChangelogEntry entry456 = makeHighlightsEntry(456, true);
        ChangelogEntry entry789 = makeHighlightsEntry(789, false);
        // Return unordered list, to test correct re-ordering
        return List.of(entry456, entry123, entry789);
    }

    private ChangelogEntry makeHighlightsEntry(int pr, boolean notable) {
        ChangelogEntry entry = new ChangelogEntry();
        entry.setPr(pr);
        ChangelogEntry.Highlight highlight = new ChangelogEntry.Highlight();
        entry.setHighlight(highlight);

        highlight.setNotable(notable);
        highlight.setTitle((notable ? "[Notable] " : "") + "Release highlight number " + pr);
        highlight.setBody("Release highlight body number " + pr);
        entry.setType("feature");
        entry.setArea("Search");
        entry.setSummary("");

        return entry;
    }

    private String getResource(String name) throws Exception {
        return Files.readString(Paths.get(Objects.requireNonNull(this.getClass().getResource(name)).toURI()), StandardCharsets.UTF_8);
    }
}
