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
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ModuleVisitor;
import org.objectweb.asm.Opcodes;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;

/**
 * This task generates a file with a class to module mapping
 * used to imitate modular behavior during unit tests so
 * entitlements can lookup correct policies.
 */
public abstract class GenerateTestBuildInfoTask extends DefaultTask {

    public static final String DESCRIPTION = "generates plugin test dependencies file";

    public GenerateTestBuildInfoTask() {
        setDescription(DESCRIPTION);
    }

    @Input
    @Optional
    public abstract Property<String> getModuleName();

    @Input
    public abstract Property<String> getComponentName();

    @InputFiles
    public abstract Property<FileCollection> getCodeLocations();

    @Input
    public abstract Property<String> getOutputFileName();

    @OutputDirectory
    public abstract DirectoryProperty getOutputDirectory();

    @TaskAction
    public void generatePropertiesFile() throws IOException {
        Map<String, String> classesToModules = buildClassesToModules();

        Path outputDirectory = getOutputDirectory().get().getAsFile().toPath();
        Files.createDirectories(outputDirectory);
        Path outputFile = outputDirectory.resolve(getOutputFileName().get());

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

    // build the association for a class to a module;
    // there are different methods for finding these depending on if the
    // classpath entry is a jar or a directory
    private Map<String, String> buildClassesToModules() throws IOException {
        Map<String, String> classesToModules = new HashMap<>();
        for (File file : getCodeLocations().get().getFiles()) {
            if (file.exists()) {
                if (file.getName().endsWith(".jar")) {
                    extractFromJar(file, classesToModules);
                } else if (file.isDirectory()) {
                    extractFromDirectory(file, classesToModules);
                } else {
                    throw new IllegalArgumentException("unrecognized classpath entry: " + file);
                }
            }
        }
        return classesToModules;
    }

    // find the first class and module when the class path entry is a jar
    private void extractFromJar(File file, Map<String, String> classesToModules) throws IOException {
        try (JarFile jarFile = new JarFile(file)) {
            String className = extractClassNameFromJar(jarFile);
            String moduleName = extractModuleNameFromJar(file, jarFile);

            if (className != null) {
                classesToModules.put(className, moduleName);
            }
        }
    }

    // look through the jar to find the first unique class that isn't
    // in META-INF (those may not be unique) and isn't module-info.class
    // (which is also not unique) and avoid anonymous classes
    private String extractClassNameFromJar(JarFile jarFile) {
        return jarFile.stream()
            .filter(
                je -> je.getName().startsWith("META-INF") == false
                    && je.getName().equals("module-info.class") == false
                    && je.getName().contains("$") == false
                    && je.getName().endsWith(".class")
            )
            .findFirst()
            .map(ZipEntry::getName)
            .orElse(null);
    }

    // look through the jar for the module name with
    // each step commented inline
    private String extractModuleNameFromJar(File file, JarFile jarFile) throws IOException {
        String moduleName = null;

        // if the jar is multi-release, there will be a set versions
        // under the path META-INF/versions/<version number>;
        // each version will have its own module-info.class if this is a modular jar;
        // look for the module name in the module-info from the latest version
        // fewer than or equal to the current JVM version
        if (jarFile.isMultiRelease()) {
            List<Integer> versions = jarFile.stream()
                .filter(je -> je.getName().startsWith("META-INF/versions/") && je.getName().endsWith("/module-info.class"))
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
                JarEntry moduleEntry = jarFile.getJarEntry(path.toString());
                if (moduleEntry != null) {
                    try (InputStream inputStream = jarFile.getInputStream(moduleEntry)) {
                        moduleName = extractModuleNameFromModuleInfo(inputStream);
                    }
                }
            }
        }

        // if the jar is *not* multi-release then first look in
        // module-info.class from the top-level if it exists
        if (moduleName == null) {
            JarEntry moduleEntry = jarFile.getJarEntry("module-info.class");
            if (moduleEntry != null) {
                try (InputStream inputStream = jarFile.getInputStream(moduleEntry)) {
                    moduleName = extractModuleNameFromModuleInfo(inputStream);
                }
            }
        }

        // if the jar does *not* contain module-info.class
        // check the manifest file for the module name
        if (moduleName == null) {
            JarEntry manifestEntry = jarFile.getJarEntry("META-INF/MANIFEST.MF");
            if (manifestEntry != null) {
                try (InputStream inputStream = jarFile.getInputStream(manifestEntry)) {
                    Manifest manifest = new Manifest(inputStream);
                    String amn = manifest.getMainAttributes().getValue("Automatic-Module-Name");
                    if (amn != null) {
                        moduleName = amn;
                    }
                }
            }
        }

        // if the jar does not have module-info.class and no module name in the manifest
        // default to the jar name without .jar and no versioning
        if (moduleName == null) {
            String jn = file.getName().substring(0, file.getName().length() - 4);
            Matcher matcher = Pattern.compile("-(\\d+(\\.|$))").matcher(jn);
            if (matcher.find()) {
                jn = jn.substring(0, matcher.start());
            }
            jn = jn.replaceAll("[^A-Za-z0-9]", ".");
            moduleName = jn;
        }

        return moduleName;
    }

    // find the first class and module when the class path entry is a directory
    private void extractFromDirectory(File file, Map<String, String> classesToModules) throws IOException {
        String className = extractClassNameFromDirectory(file);
        String moduleName = extractModuleNameFromDirectory(file);

        if (className != null && moduleName != null) {
            classesToModules.put(className, moduleName);
        }
    }

    // look through the directory to find the first unique class that isn't
    // module-info.class (which may not be unique) and avoid anonymous classes
    private String extractClassNameFromDirectory(File file) {
        List<File> files = new ArrayList<>(List.of(file));
        while (files.isEmpty() == false) {
            File find = files.removeFirst();
            if (find.exists()) {
                if (find.getName().endsWith(".class")
                    && find.getName().equals("module-info.class") == false
                    && find.getName().contains("$") == false) {
                    return find.getAbsolutePath().substring(file.getAbsolutePath().length() + 1);
                } else if (find.isDirectory()) {
                    files.addAll(Arrays.asList(find.listFiles()));
                }
            }

        }
        return null;
    }

    // look through the directory to find the module name in either module-info.class
    // if it exists or the preset one derived from the jar task
    private String extractModuleNameFromDirectory(File file) throws IOException {
        List<File> files = new ArrayList<>(List.of(file));
        while (files.isEmpty() == false) {
            File find = files.removeFirst();
            if (find.exists()) {
                if (find.getName().equals("module-info.class")) {
                    try (InputStream inputStream = new FileInputStream(find)) {
                        return extractModuleNameFromModuleInfo(inputStream);
                    }
                } else if (find.isDirectory()) {
                    files.addAll(Arrays.asList(find.listFiles()));
                }
            }
        }
        return getModuleName().isPresent() ? getModuleName().get() : null;
    }

    // a helper method to extract the module name from module-info.class
    // using an ASM ClassVisitor
    private String extractModuleNameFromModuleInfo(InputStream inputStream) throws IOException {
        String[] moduleName = new String[1];
        ClassReader cr = new ClassReader(inputStream);
        cr.accept(new ClassVisitor(Opcodes.ASM9) {
            @Override
            public ModuleVisitor visitModule(String name, int access, String version) {
                moduleName[0] = name;
                return super.visitModule(name, access, version);
            }
        }, Opcodes.ASM9);
        return moduleName[0];
    }
}
