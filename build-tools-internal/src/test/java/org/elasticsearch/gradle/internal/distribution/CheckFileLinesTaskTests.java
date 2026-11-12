/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.gradle.api.GradleException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class CheckFileLinesTaskTests {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void acceptsFileStartingWithExpectedLines() throws IOException {
        Path file = file("Elasticsearch\nCopyright 2009-2024 Elasticsearch\n\nmore notices\n");
        CheckFileLinesTask.assertStartsWithLines(file, List.of("Elasticsearch", "Copyright 2009-2024 Elasticsearch"));
        CheckFileLinesTask.assertStartsWithLines(file, List.of());
    }

    @Test
    public void namesFirstDifferingLine() throws IOException {
        Path file = file("Elasticsearch\nCopyright 2009-2018 Acme\n");
        GradleException e = assertThrows(
            GradleException.class,
            () -> CheckFileLinesTask.assertStartsWithLines(file, List.of("Elasticsearch", "Copyright 2009-2024 Elasticsearch"))
        );
        assertEquals(
            "expected line [2] in [" + file + "] to be [Copyright 2009-2024 Elasticsearch] but was [Copyright 2009-2018 Acme]",
            e.getMessage()
        );
    }

    @Test
    public void failsOnFileShorterThanExpectedLines() throws IOException {
        Path file = file("Elasticsearch\n");
        GradleException e = assertThrows(
            GradleException.class,
            () -> CheckFileLinesTask.assertStartsWithLines(file, List.of("Elasticsearch", "Copyright 2009-2024 Elasticsearch"))
        );
        assertEquals("expected line [2] in [" + file + "] to be [Copyright 2009-2024 Elasticsearch] but the file ended", e.getMessage());
    }

    @Test
    public void failsOnMissingFile() {
        Path missing = temporaryFolder.getRoot().toPath().resolve("missing.txt");
        GradleException e = assertThrows(GradleException.class, () -> CheckFileLinesTask.assertStartsWithLines(missing, List.of("a")));
        assertEquals("Unable to read from file " + missing, e.getMessage());
    }

    private Path file(String content) throws IOException {
        Path file = temporaryFolder.newFile().toPath();
        Files.writeString(file, content);
        return file;
    }
}
