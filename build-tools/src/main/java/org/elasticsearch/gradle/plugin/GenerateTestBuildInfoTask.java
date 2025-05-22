/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.plugin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.fasterxml.jackson.databind.SerializationFeature;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.jetbrains.annotations.NotNull;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.ModuleVisitor;
import org.objectweb.asm.Opcodes;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.CodeSource;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.Manifest;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.TERMINATE;

/**
 * This task generates a file with a class to module mapping
 * used to imitate modular behavior during unit tests so
 * entitlements can lookup correct policies.
 */
@CacheableTask
public abstract class GenerateTestBuildInfoTask extends DefaultTask {

    public static final String DESCRIPTION = "generates plugin test dependencies file";

    public static final String META_INF_VERSIONS_PREFIX = "META-INF/versions/";
    public static final String JAR_DESCRIPTOR_SUFFIX = ".jar";

    public GenerateTestBuildInfoTask() {
        setDescription(DESCRIPTION);
    }

    @Input
    @Optional
    public abstract Property<String> getModuleName();

    @Input
    public abstract Property<String> getComponentName();

    @Classpath
    public abstract Property<FileCollection> getCodeLocations();

    @OutputFile
    public abstract RegularFileProperty getOutputFile();

    @TaskAction
    public void generatePropertiesFile() throws IOException {
        Path outputFile = getOutputFile().get().getAsFile().toPath();
        Files.createDirectories(outputFile.getParent());

        try (var writer = Files.newBufferedWriter(outputFile, StandardCharsets.UTF_8)) {
            ObjectMapper mapper = new ObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, true)
                .setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
            mapper.writeValue(writer, new OutputFileContents(getComponentName().get(), buildLocationList()));
        }
    }

    /**
     * The output of this task is a JSON file formatted according to this record.
     * @param component the entitlements <em>component</em> name of the artifact we're describing
     * @param locations a {@link Location} for each code directory/jar in this artifact
     */
    record OutputFileContents(String component, List<Location> locations) {}

    /**
     * Our analog of a single {@link CodeSource#getLocation()}.
     * All classes in any single <em>location</em> (a directory or jar)
     * are considered to be part of the same Java module for entitlements purposes.
     * Since tests run without Java modules, and entitlements are all predicated on modules,
     * this info lets us determine what the module <em>would have been</em>
     * so we can look up the appropriate entitlements.
     *
     * @param module              the name of the Java module corresponding to this {@code Location}.
     * @param representativeClass an example of any <code>.class</code> file within this {@code Location}
     *                            whose name will be unique within its {@link ClassLoader} at run time.
     */
    record Location(String module, String representativeClass) {}

    /**
     * Build the list of {@link Location}s for all {@link #getCodeLocations() code locations}.
     * There are different methods for finding these depending on if the
     * classpath entry is a jar or a directory
     */
    private List<Location> buildLocationList() throws IOException {
        List<Location> locations = new ArrayList<>();
        for (File file : getCodeLocations().get().getFiles()) {
            if (file.exists()) {
                if (file.getName().endsWith(JAR_DESCRIPTOR_SUFFIX)) {
                    extractLocationsFromJar(file, locations);
                } else if (file.isDirectory()) {
                    extractLocationsFromDirectory(file, locations);
                } else {
                    throw new IllegalArgumentException("unrecognized classpath entry: " + file);
                }
            }
        }
        return List.copyOf(locations);
    }

    /**
     * find the first class and module when the class path entry is a jar
     */
    private void extractLocationsFromJar(File file, List<Location> locations) throws IOException {
        try (JarFile jarFile = new JarFile(file)) {
            var className = extractClassNameFromJar(jarFile);

            if (className.isPresent()) {
                String moduleName = extractModuleNameFromJar(file, jarFile);
                locations.add(new Location(moduleName, className.get()));
            }
        }
    }

    /**
     * look through the jar to find the first unique class that isn't
     * in META-INF (those may not be unique) and isn't module-info.class
     * (which is also not unique) and avoid anonymous classes
     */
    private java.util.Optional<String> extractClassNameFromJar(JarFile jarFile) {
        return jarFile.stream()
            .filter(
                je -> je.getName().startsWith("META-INF") == false
                    && je.getName().equals("module-info.class") == false
                    && je.getName().contains("$") == false
                    && je.getName().endsWith(".class")
            )
            .findFirst()
            .map(ZipEntry::getName);
    }

    /**
     * Look through the jar for the module name using a succession of techniques corresponding
     * to how the JDK itself determines module names,
     * as documented in {@link java.lang.module.ModuleFinder#of}.
     */
    private String extractModuleNameFromJar(File file, JarFile jarFile) throws IOException {
        String moduleName = null;

        if (jarFile.isMultiRelease()) {
            StringBuilder dir = versionDirectoryIfExists(jarFile);
            if (dir != null) {
                dir.append("/module-info.class");
                moduleName = getModuleNameFromModuleInfoFile(dir.toString(), jarFile);
            }
        }

        if (moduleName == null) {
            moduleName = getModuleNameFromModuleInfoFile("module-info.class", jarFile);
        }

        if (moduleName == null) {
            moduleName = getAutomaticModuleNameFromManifest(jarFile);
        }

        if (moduleName == null) {
            moduleName = deriveModuleNameFromJarFileName(file);
        }

        return moduleName;
    }

    /**
     * if the jar is multi-release, there will be a set versions
     * under the path META-INF/versions/<version number>;
     * each version will have its own module-info.class if this is a modular jar;
     * look for the module name in the module-info from the latest version
     * fewer than or equal to the current JVM version
     *
     * @return a {@link StringBuilder} with the {@code META-INF/versions/<version number>} if it exists; otherwise null
     */
    private static StringBuilder versionDirectoryIfExists(JarFile jarFile) {
        List<Integer> versions = jarFile.stream()
            .filter(je -> je.getName().startsWith(META_INF_VERSIONS_PREFIX) && je.getName().endsWith("/module-info.class"))
            .map(
                je -> Integer.parseInt(
                    je.getName().substring(META_INF_VERSIONS_PREFIX.length(), je.getName().length() - META_INF_VERSIONS_PREFIX.length())
                )
            )
            .toList();
        versions = new ArrayList<>(versions);
        versions.sort(Integer::compareTo);
        versions = versions.reversed();
        int major = Runtime.version().feature();
        StringBuilder path = new StringBuilder(META_INF_VERSIONS_PREFIX);
        for (int version : versions) {
            if (version <= major) {
                return path.append(version);
            }
        }
        return null;
    }

    /**
     * Looks into the specified {@code module-info.class} file, if it exists, and extracts the declared name of the module.
     * @return the module name, or null if there is no such {@code module-info.class} file.
     */
    private String getModuleNameFromModuleInfoFile(String moduleInfoFileName, JarFile jarFile) throws IOException {
        JarEntry moduleEntry = jarFile.getJarEntry(moduleInfoFileName);
        if (moduleEntry != null) {
            try (InputStream inputStream = jarFile.getInputStream(moduleEntry)) {
                return extractModuleNameFromModuleInfo(inputStream);
            }
        }
        return null;
    }

    /**
     * Looks into the {@code MANIFEST.MF} file and returns the {@code Automatic-Module-Name} value if there is one.
     * @return the module name, or null if the manifest is nonexistent or has no {@code Automatic-Module-Name} value
     */
    private static String getAutomaticModuleNameFromManifest(JarFile jarFile) throws IOException {
        JarEntry manifestEntry = jarFile.getJarEntry("META-INF/MANIFEST.MF");
        if (manifestEntry != null) {
            try (InputStream inputStream = jarFile.getInputStream(manifestEntry)) {
                Manifest manifest = new Manifest(inputStream);
                String amn = manifest.getMainAttributes().getValue("Automatic-Module-Name");
                if (amn != null) {
                    return amn;
                }
            }
        }
        return null;
    }

    /**
     * Compose a module name from the given {@code jarFile} name,
     * as documented in {@link java.lang.module.ModuleFinder#of}.
     */
    private static @NotNull String deriveModuleNameFromJarFileName(File jarFile) {
        String jn = jarFile.getName().substring(0, jarFile.getName().length() - JAR_DESCRIPTOR_SUFFIX.length());
        Matcher matcher = Pattern.compile("-(\\d+(\\.|$))").matcher(jn);
        if (matcher.find()) {
            jn = jn.substring(0, matcher.start());
        }
        jn = jn.replaceAll("[^A-Za-z0-9]", ".");
        return jn;
    }

    /**
     * find the first class and module when the class path entry is a directory
     */
    private void extractLocationsFromDirectory(File dir, List<Location> locations) throws IOException {
        String className = extractClassNameFromDirectory(dir);
        String moduleName = extractModuleNameFromDirectory(dir);

        if (className != null && moduleName != null) {
            locations.add(new Location(moduleName, className));
        }
    }

    /**
     * look through the directory to find the first unique class that isn't
     * module-info.class (which may not be unique) and avoid anonymous classes
     */
    private String extractClassNameFromDirectory(File dir) throws IOException {
        var visitor = new SimpleFileVisitor<Path>() {
            String result = null;

            @Override
            public @NotNull FileVisitResult visitFile(@NotNull Path candidate, @NotNull BasicFileAttributes attrs) {
                String name = candidate.getFileName().toString(); // Just the part after the last dir separator
                if (name.endsWith(".class") && (name.equals("module-info.class") || name.contains("$")) == false) {
                    result = candidate.toAbsolutePath().toString().substring(dir.getAbsolutePath().length() + 1);
                    return TERMINATE;
                } else {
                    return CONTINUE;
                }
            }
        };
        Files.walkFileTree(dir.toPath(), visitor);
        return visitor.result;
    }

    /**
     * look through the directory to find the module name in either module-info.class
     * if it exists or the preset one derived from the jar task
     */
    private String extractModuleNameFromDirectory(File dir) throws IOException {
        var visitor = new SimpleFileVisitor<Path>() {
            private String result = getModuleName().getOrNull();

            @Override
            public @NotNull FileVisitResult visitFile(@NotNull Path candidate, @NotNull BasicFileAttributes attrs) throws IOException {
                String name = candidate.getFileName().toString(); // Just the part after the last dir separator
                if (name.equals("module-info.class")) {
                    try (InputStream inputStream = new FileInputStream(candidate.toFile())) {
                        result = extractModuleNameFromModuleInfo(inputStream);
                        return TERMINATE;
                    }
                } else {
                    return CONTINUE;
                }
            }
        };
        Files.walkFileTree(dir.toPath(), visitor);
        return visitor.result;
    }

    /**
     * a helper method to extract the module name from module-info.class
     * using an ASM ClassVisitor
     */
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
