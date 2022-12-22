/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.TaskAction;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.JarFile;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;

public abstract class JarApiComparisonTask extends DefaultTask {

    @TaskAction
    public void compare() {
        JarScanner oldJS = new JarScanner(getOldJar().get().toString());
        JarScanner newJS = new JarScanner(getNewJar().get().toString());
        try {
            JarScanner.compareSignatures(oldJS.jarSignature(), newJS.jarSignature());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @InputFile
    public abstract RegularFileProperty getOldJar();

    @InputFile
    public abstract RegularFileProperty getNewJar();

    public static class JarScanner {

        private final String path;

        public JarScanner(String path) {
            this.path = path;
        }

        private String getPath() {
            return path;
        }

        List<String> classNames() throws IOException {
            Pattern classEnding = Pattern.compile(".*\\.class$");
            try (JarFile jf = new JarFile(this.path)) {
                return jf.stream()
                    .map(ZipEntry::getName)
                    .filter(classEnding.asMatchPredicate())
                    .collect(Collectors.toList());
            }
        }

        public List<String> disassembleFromJar(String filepath, String classpath) {
            String location = "jar:file://" + getPath() + "!/" + filepath;
            return disassemble(location, getPath(), classpath);
        }

        static List<String> disassemble(String location, String modulePath, String classpath) {
            ProcessBuilder pb = new ProcessBuilder();
            List<String> command = new ArrayList<>();
            command.add("javap");
            if (modulePath != null) {
                command.add("--module-path");
                command.add(modulePath);
            }
            if (classpath != null) {
                command.add("--class-path");
                command.add(classpath);
            }
            command.add(location);
            pb.command(command.toArray(new String[]{}));
            Process p;
            try {
                p = pb.start();
                p.onExit().get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            InputStream streamToRead = p.exitValue() == 0 ? p.getInputStream() : p.getErrorStream();

            try (BufferedReader br = new BufferedReader(new InputStreamReader(streamToRead))) {
                return br.lines().toList();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public static Set<String> signaturesSet(List<String> javapOutput) {
            return javapOutput.stream()
                .filter(s -> s.matches("^\\s*public.*"))
                .collect(Collectors.toSet());
        }

        public static Set<String> moduleInfoSignaturesSet(List<String> javapOutput) {
            return javapOutput.stream()
                .filter(s -> s.matches("^\\s*exports.*"))
                .filter(s -> s.matches(".* to$") == false)
                .collect(Collectors.toSet());
        }

        // NEXT: we have all the pieces, so we can create a signatures map of classname -> set of public elements
        public Map<String, Set<String>> jarSignature() throws IOException {
            return this.classNames().stream()
                .collect(Collectors.toMap(
                    s -> s,
                    s -> {
                        List<String> disassembled = disassembleFromJar(s, null);
                        if ("module-info.class".equals(s)) {
                            return moduleInfoSignaturesSet(disassembled);
                        }
                        return signaturesSet(disassembled);
                    }
                ));
        }

        public static void compareSignatures(Map<String, Set<String>> oldSignature, Map<String, Set<String>> newSignature) {
            Set<String> deletedClasses = new HashSet<>(oldSignature.keySet());
            deletedClasses.removeAll(newSignature.keySet());
            if (deletedClasses.size() > 0) {
                throw new IllegalStateException("Classes from a previous version not found: " + deletedClasses);
            }

            Map<String, Set<String>> deletedMembersMap = new HashMap<>();
            for (Map.Entry<String, Set<String>> entry : oldSignature.entrySet()) {
                Set<String> deletedMembers = new HashSet<>(entry.getValue());
                deletedMembers.removeAll(newSignature.get(entry.getKey()));
                if (deletedMembers.size() > 0) {
                    deletedMembersMap.put(entry.getKey(), Set.copyOf(deletedMembers));
                }
            }
            if (deletedMembersMap.size() > 0) {
                throw new IllegalStateException("Classes from a previous version have been modified, violating backwards compatibility: "
                    + deletedMembersMap);
            }
        }
    }
}

