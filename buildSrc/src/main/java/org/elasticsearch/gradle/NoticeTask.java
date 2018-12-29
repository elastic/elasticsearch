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
import java.io.BufferedWriter;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.stream.Stream;

/**
 * A task to create a notice file which includes dependencies' notices.
 */
public class NoticeTask extends DefaultTask {

    @InputFile
    private File inputFile = getProject().getRootProject().file("NOTICE.txt");
    @OutputFile
    private File outputFile = new File(getProject().getBuildDir(), "notices/" + getName() + "/NOTICE.txt");

    /** Directories to include notices from */
    private List<File> licensesDirs = new ArrayList<>();

    public File getInputFile() {
        return inputFile;
    }

    public void setInputFile(File inputFile) {
        this.inputFile = inputFile;
    }

    public File getOutputFile() {
        return outputFile;
    }

    public void setOutputFile(File outputFile) {
        this.outputFile = outputFile;
    }

    public void licensesDir(File licensesDir) {
        licensesDirs.add(licensesDir);
    }

    public void addLicenseDir(File licenseDir){
        this.licensesDir(licenseDir);
    }
    public List<File> getLicensesDirs(){
        return Collections.unmodifiableList(this.licensesDirs);
    }

//    public static String getText(File file){
//        return getText(file, StandardCharsets.UTF_8);
//    }

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
    public void generateNotice() {
        final StringBuilder output = new StringBuilder();

        try{
            Files.lines(this.inputFile.toPath(), StandardCharsets.UTF_8)
                .forEach(s -> {
                    output.append(s.trim());
                    output.append("\n");
                });
            output.append("\n\n");

        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }

        // This is a map rather than a set so that the sort order is the 3rd
        // party component names, unaffected by the full path to the various files
        final Map<String, File> seen = new TreeMap<>();
        licensesDirs.stream()
            .flatMap(file -> {
                try {
                    return Files.walk(file.toPath());
                } catch (IOException e) {
                    e.printStackTrace();
//                    throw new RuntimeException("failed to walk for " + file.toString());
                    return Stream.empty();
                }
            })
            .filter(path -> path.toString().endsWith("-NOTICE.txt"))
            .map(Path::toFile)
            .forEach(licenseFile -> {
                // Here we remove the "-NOTICE.txt" so the name can be used later for the -LICENSE.txt file
                final String name =
                    licenseFile.getName().substring(0,licenseFile.getName().length() - "-NOTICE.txt".length());

                if (seen.containsKey(name)){
                    File prevLicenseFile = seen.get(name);
                    if (getText(prevLicenseFile).equals(getText(licenseFile)) == false){
                        throw new RuntimeException("Two different notices exist for dependency '" +
                            name + "': " + prevLicenseFile + " and " + licenseFile);
                    }
                }else {
                    seen.put(name,licenseFile);
                }
            });


        for (Map.Entry<String,File> entry : seen.entrySet()){
            final String name = entry.getKey();
            final File file = entry.getValue();
            final File licenseFile = new File(file.getParentFile(),name + "-LICENSE.txt");

            appendFileToOutput(file, name, "NOTICE", output);
            appendFileToOutput(licenseFile,name, "LICENSE", output);
        }

        writeToFile(outputFile,output.toString(),StandardCharsets.UTF_8);
    }

    public static void appendFileToOutput(File file, final String name, final String type, StringBuilder output) {
        String text = getText(file);

        if (text.trim().isEmpty()) {
            return;
        }

        output.append("================================================================================\n");
        output.append(name + " " + type + "\n");
        output.append("================================================================================\n");
        output.append(text);
        output.append("\n\n");
    }

    public static void writeToFile(File file, String output, Charset charset){

        try(BufferedWriter out = Files.newBufferedWriter(file.toPath(),charset)){
            out.write(output);
            out.flush();
        }catch (IOException exception){
            exception.printStackTrace();
            throw new RuntimeException("Unable to write to file " + file.getName());
        }

    }

    public static String getText(File file){
        final StringBuilder builder = new StringBuilder();
        try(BufferedReader reader = new BufferedReader(new FileReader(file))){
            char[] buffer = new char[8192];
            int read;

            while ((read = reader.read(buffer)) != -1){
                builder.append(buffer,0,read);
            }

        }catch (IOException e){
            e.printStackTrace();
            throw new RuntimeException("Unable to read file " + file.toString());
        }
        return builder.toString();
    }
}
