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
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

import java.io.InputStreamReader;
import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileInputStream;

import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;
import java.util.Collections;
import java.util.stream.Collectors;

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

    @InputFiles
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

        output.append(readFileToString(this.inputFile,StandardCharsets.UTF_8));
        output.append("\n\n");

        // This is a map rather than a set so that the sort order is the 3rd
        // party component names, unaffected by the full path to the various files
        final Map<String, File> seen = this.getFilesToAppend(licensesDirs);

        for (Map.Entry<String, File> entry : seen.entrySet()) {
            final String name = entry.getKey();
            final File file = entry.getValue();
            final File licenseFile = new File(file.getParentFile(), name + "-LICENSE.txt");

            appendFileToOutput(file, name, "NOTICE", output);
            appendFileToOutput(licenseFile, name, "LICENSE", output);
        }

        write(outputFile,output.toString(),StandardCharsets.UTF_8);
    }

    private static void appendFileToOutput(File file, final String name, final String type,
                                           StringBuilder output) throws IOException {
        String text = readFileToString(file,StandardCharsets.UTF_8);
        if (text.trim().isEmpty() == false) {
            output.append("================================================================================\n");
            output.append(name + " " + type + "\n");
            output.append("================================================================================\n");
            output.append(text);
            output.append("\n\n");
        }
    }

    private Map<String,File> getFilesToAppend(List<File> licensesDirectories) throws IOException{
        final Map<String,File> licensesSeen = new TreeMap<>();

        for (File directory: licensesDirectories) {
            try(DirectoryStream<Path> stream = Files.newDirectoryStream(directory.toPath())){
                for (Path path : stream){
                    if (Files.isRegularFile(path) && path.toString().endsWith("-NOTICE.txt")){
                        File licenseFile = path.toFile();

                        final String name =
                            licenseFile.getName().substring(0, licenseFile.getName().length() - "-NOTICE.txt".length());

                        if (licensesSeen.containsKey(name)) {
                            File prevLicenseFile = licensesSeen.get(name);

                            if (readFileToString(prevLicenseFile,StandardCharsets.UTF_8)
                                .equals(readFileToString(licenseFile,StandardCharsets.UTF_8)) == false) {
                                    throw new RuntimeException("Two different notices exist for dependency '" +
                                        name + "': " + prevLicenseFile + " and " + licenseFile);
                            }
                        } else {
                            licensesSeen.put(name, licenseFile);
                        }
                    }
                }
            }
        }
        return licensesSeen;
    }
    private static String readFileToString(File file, Charset charset) throws IOException{
        CharsetDecoder decoder = charset.newDecoder();
        decoder.onMalformedInput(CodingErrorAction.REPLACE);
        final StringBuilder builder = new StringBuilder();

        try(BufferedReader reader = new
            BufferedReader(new InputStreamReader(new FileInputStream(file),decoder))){
            char[] buffer = new char[8192];
            int read;

            while ((read = reader.read(buffer))!= -1){
                builder.append(buffer,0,read);
            }
        }
        return builder.toString();
    }

    private static void write(File outputFile, String output,Charset charset) throws IOException{
        List<String> lines = Arrays.asList(output.split("\\r?\\n"));
        Files.write(outputFile.toPath(),lines,charset, StandardOpenOption.CREATE);
    }
}
