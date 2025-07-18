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
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.FileWriter;
import java.io.IOException;

public abstract class GenerateTransportVersionManifestTask extends DefaultTask {

    @InputDirectory
    public abstract RegularFileProperty getManifestDirectory();

    @OutputFile
    public abstract RegularFileProperty getManifestFile();

    @TaskAction
    public void generateTransportVersionManifest() {
        // 1. Go through the directory returned by getManifestDirectory();
        // 2. Pick out the file names
        // 3. Write a file to the file provided by getManifestFile() containing all the filenames
        System.out.println("Potato: Generating transport version manifest...");
        var manifestDirectory = getManifestDirectory();
        var dir = manifestDirectory.getAsFile().get();
        System.out.println("Potato: manifest directory location" + dir.getAbsolutePath());
        var manifestFile = getManifestFile();
        System.out.println("Potato: manifest file location: " + manifestFile.getAsFile().get().getAbsolutePath());
        try (FileWriter writer = new FileWriter(manifestFile.getAsFile().get())) {
            for (var file : dir.listFiles()) {
                System.out.println("Potato GenerateTransportVersionManifestTask: " + file.getAbsolutePath());
                var fileName = file.getName();
                if (fileName.endsWith("-LATEST.json") == false) {
                    writer.write(fileName + "\n");
                }
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        System.out.println("Potato: Finished generating transport version manifest...");
    }

}
