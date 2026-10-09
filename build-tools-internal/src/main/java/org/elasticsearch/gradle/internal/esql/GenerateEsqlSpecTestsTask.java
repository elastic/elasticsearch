/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.esql;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import javax.inject.Inject;

/**
 * Generates test classes for each {@code *.csv-spec} file found in
 * {@link #getSpecFilesDir()}.  For every registered variant (prefix + base-class pair)
 * the task emits one {@code <prefix><PascalName>IT.java} that extends the named base
 * class.  All base classes must live in the same package in hand-written sources.
 *
 * <p>This task is registered and wired by {@link EsqlCsvSpecTestsPlugin}; use that plugin
 * rather than registering this task directly.
 */
@CacheableTask
public abstract class GenerateEsqlSpecTestsTask extends DefaultTask {

    @InputDirectory
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract DirectoryProperty getSpecFilesDir();

    @Input
    public abstract Property<String> getPackageName();

    /**
     * Variant class-name prefixes, parallel to {@link #getVariantBaseClasses()}.
     * Each entry produces one generated class per spec file.
     */
    @Input
    public abstract ListProperty<String> getVariantPrefixes();

    /**
     * Variant base-class simple names, parallel to {@link #getVariantPrefixes()}.
     */
    @Input
    public abstract ListProperty<String> getVariantBaseClasses();

    /**
     * Per-variant filename glob patterns, parallel to {@link #getVariantPrefixes()}.
     * Each entry is a comma-separated list of glob patterns for one variant; an empty
     * string means "generate for all spec files".  Patterns are matched against the
     * spec file name only (not the full path) using {@link java.nio.file.PathMatcher}
     * glob syntax.
     */
    @Input
    public abstract ListProperty<String> getVariantSpecFilePatterns();

    @OutputDirectory
    public abstract DirectoryProperty getOutputDirectory();

    @Inject
    public abstract FileSystemOperations getFileSystemOperations();

    @TaskAction
    public void generate() throws IOException {
        File outputDir = getOutputDirectory().getAsFile().get();
        getFileSystemOperations().delete(spec -> spec.delete(outputDir));

        String packageName = getPackageName().get();
        String packagePath = packageName.replace('.', '/');
        File packageDir = new File(outputDir, packagePath);
        if (packageDir.mkdirs() == false && packageDir.exists() == false) {
            throw new IOException("Could not create directory: " + packageDir);
        }

        List<String> prefixes = getVariantPrefixes().get();
        List<String> baseClasses = getVariantBaseClasses().get();
        List<String> allEncodedPatterns = getVariantSpecFilePatterns().get();
        if (prefixes.size() != baseClasses.size() || prefixes.size() != allEncodedPatterns.size()) {
            throw new IllegalStateException("variantPrefixes, variantBaseClasses, and variantSpecFilePatterns must have the same length");
        }

        File specDir = getSpecFilesDir().getAsFile().get();
        File[] specFiles = specDir.listFiles((dir, name) -> name.endsWith(".csv-spec"));
        if (specFiles == null) {
            return;
        }
        Arrays.sort(specFiles);
        for (File specFile : specFiles) {
            String specFileName = specFile.getName();
            String baseName = specFileName.substring(0, specFileName.length() - ".csv-spec".length());
            String pascalName = toPascalCase(baseName);
            for (int i = 0; i < prefixes.size(); i++) {
                String encoded = allEncodedPatterns.get(i);
                List<String> patterns = encoded.isEmpty() ? List.of() : Arrays.asList(encoded.split(","));
                if (patterns.isEmpty() == false && matchesAnyPattern(specFile, patterns) == false) {
                    continue;
                }
                String className = prefixes.get(i) + pascalName + "IT";
                File javaFile = new File(packageDir, className + ".java");
                Files.writeString(
                    javaFile.toPath(),
                    buildClassSource(packageName, className, baseClasses.get(i), specFileName),
                    StandardCharsets.UTF_8
                );
                getLogger().info("Generated {}", javaFile.getName());
            }
        }
    }

    private static boolean matchesAnyPattern(File file, List<String> patterns) {
        for (String pattern : patterns) {
            if (FileSystems.getDefault().getPathMatcher("glob:" + pattern).matches(Paths.get(file.getName()))) {
                return true;
            }
        }
        return false;
    }

    private static String toPascalCase(String name) {
        String[] parts = name.split("[^a-zA-Z0-9]+");
        StringBuilder sb = new StringBuilder();
        for (String part : parts) {
            if (part.isEmpty() == false) {
                sb.append(Character.toUpperCase(part.charAt(0)));
                sb.append(part.substring(1));
            }
        }
        return sb.toString();
    }

    private static String buildClassSource(String packageName, String className, String baseClassName, String specFileName) {
        return """
            /*
             * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
             * or more contributor license agreements. Licensed under the Elastic License
             * 2.0; you may not use this file except in compliance with the Elastic License
             * 2.0.
             */

            package PACKAGE_NAME;

            // THIS FILE IS AUTO-GENERATED by the generateEsqlSpecTests Gradle task.
            // DO NOT EDIT BY HAND. Source: SPEC_FILE_NAME

            import java.util.List;

            import org.elasticsearch.xpack.esql.CsvSpecReader.CsvTestCase;

            import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

            public class CLASS_NAME extends BASE_CLASS_NAME {
                public CLASS_NAME(
                    String fileName,
                    String groupName,
                    String testName,
                    Integer lineNumber,
                    CsvTestCase testCase,
                    String instructions
                ) {
                    super(fileName, groupName, testName, lineNumber, testCase, instructions);
                }

                @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s")
                public static List<Object[]> readScriptSpec() throws Exception {
                    return readScriptSpec("/SPEC_FILE_NAME");
                }
            }
            """.replace("SPEC_FILE_NAME", specFileName)
            .replace("PACKAGE_NAME", packageName)
            .replace("BASE_CLASS_NAME", baseClassName)
            .replace("CLASS_NAME", className);
    }
}
