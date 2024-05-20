/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitTask;
import org.gradle.api.file.FileCollection;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.CompileClasspath;
import org.gradle.api.tasks.TaskAction;
import org.gradle.internal.jvm.Jvm;

import java.io.BufferedReader;
import java.io.File;
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

/**
 * This implementation of a jar API comparison uses the "javap" tool to compare
 * the "signatures" of two different jars. We assume that calling out to javap
 * is not too expensive at this stage of the stable API project. We also assume
 * that for every public class, method, and field, javap will print a consistent
 * single line. This should let us make string comparisons, rather than having
 * to parse the output of javap.
 * <p>
 * While the above assumptions appear to hold, they are not guaranteed, and hence
 * brittle. We could overcome these problems with an ASM implementation of the
 * Jar Scanner.
 * <p>
 * We also assume that we will not be comparing multi-version JARs.
 * <p>
 * This "javap" approach has a few further drawbacks:
 * <ol>
 *     <li>We don't account for class visibility when examining fields and methods.</li>
 *     <li>We don't consider what is exported from the module. Is a public method from
 *     a non-exported package considered part of the stable api?</li>
 *     <li>Changing method types to their superclass or return types to an implementation
 *     class will be considered a change by this approach, even though that doesn't break
 *     an API.</li>
 *     <li>Finally, moving a method up the class hierarchy is not really a breaking change,
 *     but it will trip this test.</li>
 * </ol>
 */
@CacheableTask
public abstract class JarApiComparisonTask extends PrecommitTask {

    @TaskAction
    public void compare() {
        FileCollection fileCollection = getOldJar().get();
        File newJarFile = getNewJar().get().getSingleFile();

        Set<String> oldJarNames = fileCollection.getFiles().stream().map(File::getName).collect(Collectors.toSet());
        if (oldJarNames.size() > 1) {
            throw new IllegalStateException("Expected a single original jar, but found: " + oldJarNames);
        }
        if (oldJarNames.contains(newJarFile.getName())) {
            throw new IllegalStateException(
                "We should be comparing different jars, but original and new jars were both: " + newJarFile.getAbsolutePath()
            );
        }

        JarScanner oldJS = new JarScanner(getOldJar().get().getSingleFile().getPath());
        JarScanner newJS = new JarScanner(newJarFile.getPath());
        try {
            JarScanner.compareSignatures(oldJS.jarSignature(), newJS.jarSignature());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @CompileClasspath
    public abstract Property<FileCollection> getOldJar();

    @CompileClasspath
    public abstract Property<FileCollection> getNewJar();

    public static class JarScanner {

        private final String path;

        public JarScanner(String path) {
            this.path = path;
        }

        private String getPath() {
            return path;
        }

        /**
         * Get a list of class names contained in this jar by looking for file names
         * that end in ".class"
         */
        List<String> classNames() throws IOException {
            Pattern classEnding = Pattern.compile(".*\\.class$");
            try (JarFile jf = new JarFile(this.path)) {
                return jf.stream().map(ZipEntry::getName).filter(classEnding.asMatchPredicate()).collect(Collectors.toList());
            }
        }

        /**
         * Given a path to a file in the jar, get the output of javap as a list of strings.
         */
        public List<String> disassembleFromJar(String fileInJarPath, String classpath) {
            String location = "jar:file://" + getPath() + "!/" + fileInJarPath;
            return disassemble(location, getPath(), classpath);
        }

        /**
         * Invoke javap on a class file, optionally providing a module path or class path
         */
        static List<String> disassemble(String location, String modulePath, String classpath) {
            ProcessBuilder pb = new ProcessBuilder();
            List<String> command = new ArrayList<>();
            command.add(Jvm.current().getExecutable("javap").getPath());
            if (modulePath != null) {
                command.add("--module-path");
                command.add(modulePath);
            }
            if (classpath != null) {
                command.add("--class-path");
                command.add(classpath);
            }
            command.add(location);
            pb.command(command.toArray(new String[] {}));
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

        /**
         * Given the output of the javap command, that is, the disassembled class file,
         * return a set of signatures for all public classes, methods, and fields.
         */
        public static Set<String> signaturesSet(List<String> javapOutput) {
            return javapOutput.stream().filter(s -> s.matches("^\\s*public.*")).collect(Collectors.toSet());
        }

        /**
         * Given a disassembled module-info.class, return all unqualified exports.
         */
        public static Set<String> moduleInfoSignaturesSet(List<String> javapOutput) {
            return javapOutput.stream()
                .filter(s -> s.matches("^\\s*exports.*"))
                .filter(s -> s.matches(".* to$") == false)
                .collect(Collectors.toSet());
        }

        /**
         * Iterate over classes and gather signatures.
         */
        public Map<String, Set<String>> jarSignature() throws IOException {
            return this.classNames().stream().collect(Collectors.toMap(s -> s, s -> {
                List<String> disassembled = disassembleFromJar(s, null);
                if ("module-info.class".equals(s)) {
                    return moduleInfoSignaturesSet(disassembled);
                }
                return signaturesSet(disassembled);
            }));
        }

        /**
         * Comparison: The signatures are maps of class names to public class, field, or method
         * declarations.
         * </p>
         * First, we check that the new jar signature contains all the same classes
         * as the old jar signature. If not, we return an error.
         * </p>
         * Second, we iterate over the signature for each class. If a signature from the old
         * jar is absent in the new jar, we add it to our list of errors.
         * </p>
         * Note that it is fine for the new jar to have additional elements, as this
         * is backwards compatible.
         */
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
                throw new IllegalStateException(
                    "Classes from a previous version have been modified, violating backwards compatibility: " + deletedMembersMap
                );
            }
        }
    }
}
