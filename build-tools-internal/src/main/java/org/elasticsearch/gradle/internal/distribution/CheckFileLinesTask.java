/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Checks that a file of an extracted distribution, such as its {@code LICENSE.txt} or {@code NOTICE.txt}, starts with
 * the expected lines. Used by the archive and the package distribution checks so that both report a mismatch the same
 * way. Lines after the expected ones are ignored.
 */
public abstract class CheckFileLinesTask extends DefaultTask {

    /**
     * The file to check.
     */
    @InputFile
    @PathSensitive(PathSensitivity.NONE)
    public abstract RegularFileProperty getCheckedFile();

    /**
     * The lines the checked file is expected to start with.
     */
    @Input
    public abstract ListProperty<String> getExpectedLines();

    @TaskAction
    public void checkLines() {
        assertStartsWithLines(getCheckedFile().get().getAsFile().toPath(), getExpectedLines().get());
    }

    /**
     * Fails if the file at {@code path} does not start with {@code expectedLines}, naming the first line that differs.
     */
    static void assertStartsWithLines(Path path, List<String> expectedLines) {
        final List<String> actualLines;
        try {
            actualLines = Files.readAllLines(path);
        } catch (IOException e) {
            throw new GradleException("Unable to read from file " + path, e);
        }
        for (int line = 0; line < expectedLines.size(); line++) {
            String expectedLine = expectedLines.get(line);
            if (line >= actualLines.size()) {
                throw new GradleException(
                    "expected line [" + (line + 1) + "] in [" + path + "] to be [" + expectedLine + "] but the file ended"
                );
            }
            String actualLine = actualLines.get(line);
            if (expectedLine.equals(actualLine) == false) {
                throw new GradleException(
                    "expected line [" + (line + 1) + "] in [" + path + "] to be [" + expectedLine + "] but was [" + actualLine + "]"
                );
            }
        }
    }
}
