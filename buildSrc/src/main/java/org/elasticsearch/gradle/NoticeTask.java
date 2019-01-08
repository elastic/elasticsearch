/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle;

import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.IOException;
import java.io.File;
import java.io.FileReader;
import java.io.BufferedReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * A task to create a notice file which includes dependencies' notices.
 */
public class NoticeTask extends DefaultTask {
    private File inputFile = getProject().getRootProject().file("NOTICE.txt");
    private File outputFile = new File(getProject().getBuildDir(), "notices/" + getName() + "/NOTICE.txt");
    /**
     * Directories to include notices from
     */
    private List<File> licensesDirs = new ArrayList<>();

    @InputFile
    public File getInputFile() {
        return inputFile;
    }

    public void setInputFile(File inputFile) {
        this.inputFile = inputFile;
    }

    @OutputFile
    public File getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(File outputFile) {
        this.outputFile = outputFile;
    }

    public void licensesDir(File licensesDir) {
        licensesDirs.add(licensesDir);
    }

    public List<File> getLicensesDirs() {
        return Collections.unmodifiableList(this.licensesDirs.stream()
                .map(file -> new File(file.toString())).collect(Collectors.toList()));
    }
    /**
     * Add notices from the specified directory.
     */

    public NoticeTask() {
        setDescription("Create a notice file from dependencies");
        // Default licenses directory is ${projectDir}/licenses (if it exists)
        File licensesDir = new File(getProject().getProjectDir(), "licenses");

        if (licensesDir.exists()) {
            licensesDirs.add(licensesDir);
        }
    }

    @TaskAction
    public void generateNotice() throws IOException {
        final StringBuilder output = new StringBuilder();


        try (Stream<String> lines = Files.lines(this.inputFile.toPath(), StandardCharsets.UTF_8)) {
            lines.forEach(s -> {
                output.append(s.trim());
                output.append("\n");
            });
        }
        output.append("\n\n");

        // This is a map rather than a set so that the sort order is the 3rd
        // party component names, unaffected by the full path to the various files
        final Map<String, File> seen = new TreeMap<>();

        licensesDirs.forEach(file -> {
            try (Stream<Path> pathStream = Files.walk(file.toPath())) {
                pathStream
//                    .filter(path -> path.endsWith("-NOTICE.txt"))
                    .filter(path -> path.toString().endsWith("-NOTICE.txt"))
                    .map(Path::toFile)
                    .forEach(licenseFile -> {
                        // Here we remove the "-NOTICE.txt" so the name can be used later for the -LICENSE.txt file
                        final String name =
                            licenseFile.getName().substring(0, licenseFile.getName().length() - "-NOTICE.txt".length());

                        if (seen.containsKey(name)) {
                            File prevLicenseFile = seen.get(name);
                            if (getText(prevLicenseFile).equals(getText(licenseFile)) == false) {
                                throw new RuntimeException("Two different notices exist for dependency '" +
                                    name + "': " + prevLicenseFile + " and " + licenseFile);
                            }
                        } else {
                            seen.put(name, licenseFile);
                        }
                    });
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });

        for (Map.Entry<String, File> entry : seen.entrySet()) {
            final String name = entry.getKey();
            final File file = entry.getValue();
            final File licenseFile = new File(file.getParentFile(), name + "-LICENSE.txt");

            appendFileToOutput(file, name, "NOTICE", output);
            appendFileToOutput(licenseFile, name, "LICENSE", output);
        }

        Files.write(outputFile.toPath(), output.toString().getBytes());
    }

    private static void appendFileToOutput(File file, final String name, final String type, StringBuilder output) {
        String text = getText(file);

        if (text.trim().isEmpty() == false) {
            output.append("================================================================================\n");
            output.append(name + " " + type + "\n");
            output.append("================================================================================\n");
            output.append(text);
            output.append("\n\n");
        }
    }


    private static String getText(File file) {
        final StringBuilder builder = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            char[] buffer = new char[8192];
            int read;

            while ((read = reader.read(buffer)) != -1) {
                builder.append(buffer, 0, read);
            }

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Unable to read file " + file.toString());
        }
        return builder.toString();
    }
}
