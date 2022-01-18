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
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

public class ReleaseNotesIndexGeneratorTest {

    /**
     * Check that a release notes index can be generated.
     */
    @Test
    public void generateFile_rendersCorrectMarkup() throws Exception {
        // given:
        final Set<QualifiedVersion> versions = Stream.of(
            "8.0.0-alpha1",
            "8.0.0-beta2",
            "8.0.0-rc3",
            "8.0.0",
            "8.0.1",
            "8.0.2",
            "8.1.0",
            "8.1.1",
            "8.2.0-SNAPSHOT"
        ).map(QualifiedVersion::of).collect(Collectors.toSet());

        final String template = getResource("/templates/release-notes-index.asciidoc");
        final String expectedOutput = getResource(
            "/org/elasticsearch/gradle/internal/release/ReleaseNotesIndexGeneratorTest.generateFile.asciidoc"
        );

        // when:
        final String actualOutput = ReleaseNotesIndexGenerator.generateFile(versions, template);

        // then:
        assertThat(actualOutput, equalTo(expectedOutput));
    }

    private String getResource(String name) throws Exception {
        return Files.readString(Paths.get(Objects.requireNonNull(this.getClass().getResource(name)).toURI()), StandardCharsets.UTF_8);
    }
}
