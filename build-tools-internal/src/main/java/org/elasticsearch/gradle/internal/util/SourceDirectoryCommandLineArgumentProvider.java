/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.util;

import org.gradle.api.file.Directory;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.process.CommandLineArgumentProvider;

import java.util.Arrays;

public class SourceDirectoryCommandLineArgumentProvider implements CommandLineArgumentProvider {

    private final Directory sourceDirectory;

    public SourceDirectoryCommandLineArgumentProvider(Directory sourceDirectory) {
        this.sourceDirectory = sourceDirectory;
    }

    public Iterable<String> asArguments() {
        return Arrays.asList("-s", sourceDirectory.getAsFile().getAbsolutePath());
    }

    @InputDirectory
    @PathSensitive(PathSensitivity.RELATIVE)
    public Directory getSourceDirectory() {
        return sourceDirectory;
    }
}
