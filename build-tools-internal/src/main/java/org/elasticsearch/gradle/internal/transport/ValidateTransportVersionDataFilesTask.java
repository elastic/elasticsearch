/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import groovy.json.JsonSlurper;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.rmi.RemoteException;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public abstract class ValidateTransportVersionDataFilesTask extends DefaultTask {

    @InputDirectory
    public abstract RegularFileProperty getDataFileDirectory();

    @InputFile
    public abstract RegularFileProperty getAggregateTransportVersionNamesFile();

    @TaskAction
    public void validateTransportVersionDataFiles() throws IOException {
        var dataFileDirectory = getDataFileDirectory();
        var tvDataDir = dataFileDirectory.getAsFile().get();
        var tvSetDeclaredNamesFile = getAggregateTransportVersionNamesFile().get().getAsFile();
        var names = Files.lines(Path.of(tvSetDeclaredNamesFile.getPath())).collect(Collectors.toUnmodifiableSet());
        for (var tvDataFile : Objects.requireNonNull(tvDataDir.listFiles())) {
            if (tvDataFile.getName().endsWith("-LATEST.json") == false) {
                var slurper = new JsonSlurper();
                if (slurper.parse(tvDataFile) instanceof Map json) {
                    String name = json.get("name").toString();
                    if (names.contains(name) == false) {
                        throw new RemoteException(
                            "Transport version data file '"
                                + tvDataFile.getName()
                                + "' contains a transport version name '"
                                + name
                                + "' that is not present in the list of TransportVersions declared throughout the codebase: '"
                                + tvSetDeclaredNamesFile.getName()
                        );
                    }
                } else {
                    throw new RuntimeException(tvDataFile + " is not a json file");
                }
            }
        }
    }
}
