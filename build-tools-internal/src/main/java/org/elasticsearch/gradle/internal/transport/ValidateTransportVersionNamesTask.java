/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Validates that each transport version declaration has an associated data file.
 */
public abstract class ValidateTransportVersionNamesTask extends DefaultTask {

    @InputDirectory
    public abstract RegularFileProperty getDataFileDirectory();

    @InputFile
    public abstract RegularFileProperty getTransportVersionSetNamesFile();

    @TaskAction
    public void validateTransportVersions() throws IOException {
        var dataFileDirectory = getDataFileDirectory();
        var tvDataDir = dataFileDirectory.getAsFile().get();

        Set<String> tvNamesInDataFiles = new HashSet<>();
        for (var tvDataFile : Objects.requireNonNull(tvDataDir.listFiles())) {
            var data = TransportVersionUtils.readDataFile(tvDataFile);
            tvNamesInDataFiles.add(data.name());
        }

        var tvSetDeclaredNamesFile = getTransportVersionSetNamesFile().get().getAsFile();
        try (var reader = new BufferedReader(new FileReader(tvSetDeclaredNamesFile))) {
            reader.lines().forEach(declaredName -> {
                if (tvNamesInDataFiles.contains(declaredName) == false) {
                    throw new RuntimeException(
                        "TransportVersionSetData.get(\""
                            + declaredName
                            + "\") was used, but lacks a"
                            + " data file with a corresponding transport version. This can be generated with the <TODO> task" // todo
                    );
                }
            });
        }
    }
}
