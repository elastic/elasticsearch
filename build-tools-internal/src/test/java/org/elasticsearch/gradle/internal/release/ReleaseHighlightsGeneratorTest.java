/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class ReleaseHighlightsGeneratorTest {

    /**
     * Check that the release highlights can be correctly generated when there are no highlights.
     */
    @Test
    public void generateFile_withNoHighlights_rendersCorrectMarkup() throws Exception {
        // given:
        final String template = getResource("/templates/release-highlights.asciidoc");
        final String expectedOutput = getResource(
            "/org/elasticsearch/gradle/internal/release/ReleaseHighlightsGeneratorTest.noHighlights.generateFile.asciidoc"
        );

        // when:
        final String actualOutput = ReleaseHighlightsGenerator.generateFile(QualifiedVersion.of("8.4.0-SNAPSHOT"), template, List.of());

        // then:
        assertThat(actualOutput, equalTo(expectedOutput));
    }

    /**
     * Check that the release highlights can be correctly generated.
     */
    @Test
    public void generateFile_rendersCorrectMarkup() throws Exception {
        // given:
        final String template = getResource("/templates/release-highlights.asciidoc");
        final String expectedOutput = getResource(
            "/org/elasticsearch/gradle/internal/release/ReleaseHighlightsGeneratorTest.generateFile.asciidoc"
        );

        final List<ChangelogEntry> entries = getEntries();

        // when:
        final String actualOutput = ReleaseHighlightsGenerator.generateFile(QualifiedVersion.of("8.4.0-SNAPSHOT"), template, entries);

        // then:
        assertThat(actualOutput, equalTo(expectedOutput));
    }

    private List<ChangelogEntry> getEntries() {
        ChangelogEntry entry123 = makeChangelogEntry(123, true);
        ChangelogEntry entry456 = makeChangelogEntry(456, true);
        ChangelogEntry entry789 = makeChangelogEntry(789, false);
        // Return unordered list, to test correct re-ordering
        return List.of(entry456, entry123, entry789);
    }

    private ChangelogEntry makeChangelogEntry(int pr, boolean notable) {
        ChangelogEntry entry = new ChangelogEntry();
        entry.setPr(pr);
        ChangelogEntry.Highlight highlight = new ChangelogEntry.Highlight();
        entry.setHighlight(highlight);

        highlight.setNotable(notable);
        highlight.setTitle("Notable release highlight number " + pr);
        highlight.setBody("Notable release body number " + pr);

        return entry;
    }

    private String getResource(String name) throws Exception {
        return Files.readString(Paths.get(Objects.requireNonNull(this.getClass().getResource(name)).toURI()), StandardCharsets.UTF_8);
    }
}
