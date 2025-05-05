/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.plugin;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.FileCollection;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ModuleVisitor;
import org.objectweb.asm.Opcodes;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class GenerateTestBuildInfoTask extends DefaultTask {

    public static final String DESCRIPTION = "generates plugin test dependencies file";
    public static final String PROPERTIES_FILENAME = "test-build-info.json";

    public GenerateTestBuildInfoTask() {
        setDescription(DESCRIPTION);
    }

    @Input
    public abstract Property<String> getComponentName();

    @InputFiles
    public abstract Property<FileCollection> getCodeLocations();

    @OutputDirectory
    public abstract DirectoryProperty getOutputDirectory();

    @TaskAction
    public void generatePropertiesFile() throws IOException {
        Map<String, String> classesToModules = new HashMap<>();
        for (File file : getCodeLocations().get().getFiles()) {
            if (file.exists()) {
                if (file.getName().endsWith(".jar")) {
                    try (JarFile jarFile = new JarFile(file)) {
                        String[] moduleName = new String[1];
                        JarEntry moduleEntry = jarFile.getJarEntry("module-info.class");
                        if (moduleEntry != null) {
                            try (InputStream miis = jarFile.getInputStream(moduleEntry)) {
                                ClassReader cr = new ClassReader(miis);
                                cr.accept(new ClassVisitor(Opcodes.ASM9) {
                                    @Override
                                    public ModuleVisitor visitModule(String name, int access, String version) {
                                        //getLogger().lifecycle("FOUND 0: " + name + " | " + file.getAbsolutePath());
                                        moduleName[0] = name;
                                        return super.visitModule(name, access, version);
                                    }
                                }, Opcodes.ASM9);
                            }
                        } else {
                            moduleEntry = jarFile.getJarEntry("META-INF/MANIFEST.MF");
                            if (moduleEntry != null) {
                                try (InputStream meis = jarFile.getInputStream(moduleEntry)) {
                                    Manifest manifest = new Manifest(meis);
                                    String mr = manifest.getMainAttributes().getValue(Attributes.Name.MULTI_RELEASE);
                                    if (Boolean.parseBoolean(mr)) {
                                        List<Integer> versions = jarFile.stream()
                                            .filter(
                                                je -> je.getName().startsWith("META-INF/versions/")
                                                    && je.getName().endsWith("/module-info.class")
                                            )
                                            .map(je -> Integer.parseInt(je.getName().substring(18, je.getName().length() - 18)))
                                            .toList();
                                        versions = new ArrayList<>(versions);
                                        versions.sort(Integer::compareTo);
                                        versions = versions.reversed();
                                        int major = Runtime.version().version().get(0);
                                        StringBuilder path = new StringBuilder("META-INF/versions/");
                                        for (int version : versions) {
                                            if (version <= major) {
                                                path.append(version);
                                                break;
                                            }
                                        }
                                        if (path.length() > 18) {
                                            path.append("/module-info.class");
                                            moduleEntry = jarFile.getJarEntry(path.toString());
                                            if (moduleEntry != null) {
                                                try (InputStream miis = jarFile.getInputStream(moduleEntry)) {
                                                    ClassReader cr = new ClassReader(miis);
                                                    cr.accept(new ClassVisitor(Opcodes.ASM9) {
                                                        @Override
                                                        public ModuleVisitor visitModule(String name, int access, String version) {
                                                            //getLogger().lifecycle("FOUND 1: " + name + " | " + file.getAbsolutePath());
                                                            moduleName[0] = name;
                                                            return super.visitModule(name, access, version);
                                                        }
                                                    }, Opcodes.ASM9);
                                                }
                                            }
                                        }
                                    }
                                    if (moduleName[0] == null) {
                                        String amn = manifest.getMainAttributes().getValue("Automatic-Module-Name");
                                        if (amn != null) {
                                            //getLogger().lifecycle("FOUND 2: " + amn + " | " + file.getAbsolutePath());
                                            moduleName[0] = amn;
                                        }
                                    }
                                }
                            }
                            if (moduleName[0] == null) {
                                String jn = file.getName().substring(0, file.getName().length() - 4);
                                Matcher matcher = Pattern.compile("-(\\d+(\\.|$))").matcher(jn);
                                if (matcher.find()) {
                                    jn = jn.substring(0, matcher.start());
                                }
                                jn = jn.replaceAll("[^A-Za-z0-9]", ".");
                                //getLogger().lifecycle("FOUND 3: " + jn + " | " + file.getAbsolutePath());
                                moduleName[0] = jn;
                            }
                        }
                        jarFile.stream()
                            .filter(
                                je -> je.getName().startsWith("META-INF") == false
                                    && je.getName().equals("module-info.class") == false
                                    && je.getName().endsWith(".class")
                            )
                            .findFirst()
                            .ifPresent(je -> classesToModules.put(je.getName(), moduleName[0]));
                    } catch (IOException ioe) {
                        throw new UncheckedIOException(ioe);
                    }
                } else if (file.isDirectory()) {
                    List<File> files = new ArrayList<>(List.of(file));
                    while (files.isEmpty() == false) {
                        File find = files.removeFirst();
                        if (find.exists()) {
                            if (find.isDirectory() && find.getName().equals("META_INF") == false) {
                                files.addAll(Arrays.asList(find.listFiles()));
                            } else if (find.getName().equals("module-info.class") == false && find.getName().contains("$") == false) {
                                classesToModules.put(
                                    find.getAbsolutePath().substring(file.getAbsolutePath().length() + 1),
                                    "module-goes-here"
                                );
                                break;
                            }
                        }
                    }
                } else {
                    throw new IllegalArgumentException("Unrecognized classpath file: " + file);
                }
            }
        }

        Path outputDirectory = getOutputDirectory().get().getAsFile().toPath();
        Files.createDirectories(outputDirectory);
        Path outputFile = outputDirectory.resolve(PROPERTIES_FILENAME);

        try (var writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
            writer.write("{\n");

            writer.write("    \"name\": \"");
            writer.write(getComponentName().get());
            writer.write("\",\n");

            writer.write("    \"locations\": [\n");
            if (classesToModules.isEmpty() == false) {
                StringBuilder sb = new StringBuilder();
                for (Map.Entry<String, String> entry : classesToModules.entrySet()) {
                    sb.append("        {\n");
                    sb.append("            \"class\": \"");
                    sb.append(entry.getKey());
                    sb.append("\",\n            \"module\": \"");
                    sb.append(entry.getValue());
                    sb.append("\"\n        },\n");
                }
                writer.write(sb.substring(0, sb.length() - 2));
            }
            writer.write("\n    ]\n}\n");
        }
    }
}
