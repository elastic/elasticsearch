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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class BreakingChangesGeneratorTest {

    /**
     * Check that the breaking changes can be correctly generated.
     */
    @Test
    public void generateIndexFile_rendersCorrectMarkup() throws Exception {
        // given:
        final String template = getResource("/templates/breaking-changes.asciidoc");
        final String expectedOutput = getResource(
            "/org/elasticsearch/gradle/internal/release/BreakingChangesGeneratorTest.generateMigrationFile.asciidoc"
        );

        final List<ChangelogEntry> entries = getEntries();

        // when:
        final String actualOutput = BreakingChangesGenerator.generateMigrationFile(
            QualifiedVersion.of("8.4.0-SNAPSHOT"),
            template,
            entries
        );

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
        breaking2.setArea("Cluster and node setting");
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

        ChangelogEntry entry4 = new ChangelogEntry();
        ChangelogEntry.Breaking breaking4 = new ChangelogEntry.Breaking();
        entry4.setBreaking(breaking4);

        breaking4.setNotable(true);
        breaking4.setTitle("Breaking change number 4");
        breaking4.setArea("Cluster and node setting");
        breaking4.setDetails("Breaking change details 4");
        breaking4.setImpact("Breaking change impact description 4");
        breaking4.setEssSettingChange(true);

        ChangelogEntry entry5 = new ChangelogEntry();
        ChangelogEntry.Deprecation deprecation5 = new ChangelogEntry.Deprecation();
        entry5.setDeprecation(deprecation5);

        deprecation5.setNotable(true);
        deprecation5.setTitle("Deprecation change number 5");
        deprecation5.setArea("Cluster and node setting");
        deprecation5.setDetails("Deprecation change details 5");
        deprecation5.setImpact("Deprecation change impact description 5");
        deprecation5.setEssSettingChange(false);

        ChangelogEntry entry6 = new ChangelogEntry();
        ChangelogEntry.Deprecation deprecation6 = new ChangelogEntry.Deprecation();
        entry6.setDeprecation(deprecation6);

        deprecation6.setNotable(true);
        deprecation6.setTitle("Deprecation change number 6");
        deprecation6.setArea("Cluster and node setting");
        deprecation6.setDetails("Deprecation change details 6");
        deprecation6.setImpact("Deprecation change impact description 6");
        deprecation6.setEssSettingChange(false);

        ChangelogEntry entry7 = new ChangelogEntry();
        ChangelogEntry.Deprecation deprecation7 = new ChangelogEntry.Deprecation();
        entry7.setDeprecation(deprecation7);

        deprecation7.setNotable(false);
        deprecation7.setTitle("Deprecation change number 7");
        deprecation7.setArea("Cluster and node setting");
        deprecation7.setDetails("Deprecation change details 7");
        deprecation7.setImpact("Deprecation change impact description 7");
        deprecation7.setEssSettingChange(false);

        return List.of(entry1, entry2, entry3, entry4, entry5, entry6, entry7);
    }

    private String getResource(String name) throws Exception {
        return Files.readString(Paths.get(Objects.requireNonNull(this.getClass().getResource(name)).toURI()), StandardCharsets.UTF_8);
    }
}
