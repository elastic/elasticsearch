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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;


/**
 * A task to create a notice file which includes dependencies' notices.
 */
public class NoticeTask extends DefaultTask {

    private static final Charset UTF8 = Charset.forName("UTF-8");
    @InputFile
    private File inputFile = getProject().getRootProject().file("NOTICE.txt");

    @OutputFile
    private File outputFile = new File(getProject().getBuildDir(), "notices/" + getName() + "/NOTICE.txt");

    /**
     * Directories to include notices from
     */
    private List<File> licensesDirs = new ArrayList<>();

    public NoticeTask() {
        setDescription("Create a notice file from dependencies");
        // Default licenses directory is ${projectDir}/licenses (if it exists)
        File licensesDir = new File(getProject().getProjectDir(), "licenses");
        if (licensesDir.exists()) {
            licensesDirs.add(licensesDir);
        }
    }

    /**
     * Add notices from the specified directory.
     */
    public void setLicensesDir(File licensesDir) {
        licensesDirs.add(licensesDir);
    }

    public void setInputFile(File inputFile) {
        this.inputFile = inputFile;
    }

    @TaskAction
    public void generateNotice() throws IOException {
        StringBuilder output = new StringBuilder();
        output.append(read(inputFile));
        output.append("\n\n");
        // This is a map rather than a set so that the sort order is the 3rd
        // party component names, unaffected by the full path to the various files
        Map<String, File> seen = new TreeMap<>();
        for (File licensesDir : licensesDirs) {
            File[] files = licensesDir.listFiles((file) -> file.getName().matches(".*-NOTICE\\.txt"));
            if (files == null) {
                continue;
            }
            for (File file : files) {
                String name = file.getName().substring(0, file.getName().length() - "-NOTICE.txt".length());
                if (seen.containsKey(name)) {
                    File prevFile = seen.get(name);
                    if (!read(prevFile).equals(read(file))) {
                        throw new RuntimeException("Two different notices exist for dependency '" +
                            name + "': " + prevFile + " and " + file);
                    }
                } else {
                    seen.put(name, file);
                }
            }
        }
        for (Map.Entry<String, File> entry : seen.entrySet()) {
            String name = entry.getKey();
            File file = entry.getValue();
            appendFile(file, name, "NOTICE", output);
            appendFile(new File(file.getParent(), name + "-LICENSE.txt"), name, "LICENSE", output);
        }
        outputFile.getParentFile().mkdirs();
        outputFile.createNewFile();
        write(outputFile, output.toString());
    }

    private String read(File file) throws IOException {
        return new String(Files.readAllBytes(file.toPath()), UTF8);
    }

    private void write(File file, String content) throws IOException {
        Files.write(file.toPath(), content.getBytes(UTF8));
    }

    private void appendFile(File file, String name, String type, StringBuilder output) throws IOException {
        String text = read(file);
        if (text.trim().isEmpty()) {
            return;
        }
        output.append("================================================================================\n");
        output.append(name).append(" ").append(type).append("\n");
        output.append("================================================================================\n");
        output.append(text).append("\n\n");
    }
}
