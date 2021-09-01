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
import java.util.List;
import java.util.Objects;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class BreakingChangesGeneratorTest {

    /**
     * Check that the breaking changes can be correctly generated.
     */
    @Test
    public void generateFile_rendersCorrectMarkup() throws Exception {
        // given:
        final String template = getResource("/templates/breaking-changes.asciidoc");
        final String expectedOutput = getResource(
            "/org/elasticsearch/gradle/internal/release/BreakingChangesGeneratorTest.generateFile.asciidoc"
        );
        final StringWriter writer = new StringWriter();
        final List<ChangelogEntry> entries = getEntries();

        // when:
        BreakingChangesGenerator.generateFile(QualifiedVersion.of("8.4.0-SNAPSHOT"), template, writer, entries);
        final String actualOutput = writer.toString();

        // then:
        assertThat(actualOutput, equalTo(expectedOutput));
    }

    private List<ChangelogEntry> getEntries() {
        ChangelogEntry entry1 = new ChangelogEntry();
        ChangelogEntry.Breaking breaking1 = new ChangelogEntry.Breaking();
        entry1.setBreaking(breaking1);

        breaking1.setNotable(true);
        breaking1.setTitle("Breaking change number 1");
        breaking1.setArea("API");
        breaking1.setDetails("Breaking change details 1");
        breaking1.setImpact("Breaking change impact description 1");

        ChangelogEntry entry2 = new ChangelogEntry();
        ChangelogEntry.Breaking breaking2 = new ChangelogEntry.Breaking();
        entry2.setBreaking(breaking2);

        breaking2.setNotable(true);
        breaking2.setTitle("Breaking change number 2");
        breaking2.setArea("Cluster");
        breaking2.setDetails("Breaking change details 2");
        breaking2.setImpact("Breaking change impact description 2");

        ChangelogEntry entry3 = new ChangelogEntry();
        ChangelogEntry.Breaking breaking3 = new ChangelogEntry.Breaking();
        entry3.setBreaking(breaking3);

        breaking3.setNotable(false);
        breaking3.setTitle("Breaking change number 3");
        breaking3.setArea("Transform");
        breaking3.setDetails("Breaking change details 3");
        breaking3.setImpact("Breaking change impact description 3");

        return List.of(entry1, entry2, entry3);
    }

    private String getResource(String name) throws Exception {
        return Files.readString(Paths.get(Objects.requireNonNull(this.getClass().getResource(name)).toURI()), StandardCharsets.UTF_8);
    }
}
