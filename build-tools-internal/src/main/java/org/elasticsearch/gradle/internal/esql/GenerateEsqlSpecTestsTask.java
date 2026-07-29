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
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.yaml.snakeyaml.Yaml;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

/**
 * Generates test classes for each {@code *.csv-spec} file found in
 * {@link #getSpecFilesDir()}.  For every registered variant (prefix + base-class pair)
 * the task emits:
 * <ul>
 *   <li>one {@code <prefix><PascalName>IT.java} per spec file that extends the named base class, and</li>
 *   <li>one {@code <prefix>SuiteIT.java} that uses {@code @Suite.SuiteClasses} to run all per-file
 *       classes in category-execution order (ascending index count per {@link #getSpecDataYml()}).</li>
 * </ul>
 *
 * <p>Running only the Suite class (via a {@code filter { includeTestsMatching '*.*SuiteIT' }} on the
 * Gradle test task) and setting {@code forkEvery = 0} keeps all classes in a single JVM, so
 * {@code EsqlSpecTestCase}'s static {@code loadedCategory} field survives class boundaries and the
 * cluster data is only swapped when the category actually changes.  JUnit attributes failures to
 * the per-file child class, not to the Suite, so muting granularity is preserved.
 *
 * <p>This task is registered and wired by {@link EsqlCsvSpecTestsPlugin}; use that plugin
 * rather than registering this task directly.
 */
@CacheableTask
public abstract class GenerateEsqlSpecTestsTask extends DefaultTask {

    @InputDirectory
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract DirectoryProperty getSpecFilesDir();

    /**
     * The {@code spec_data.yml} manifest file.  Parsed to determine which category each spec file
     * belongs to and how many indices each category loads, so Suite classes can list per-file
     * classes in ascending-index-count order.
     */
    @InputFile
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract RegularFileProperty getSpecDataYml();

    @Input
    public abstract Property<String> getPackageName();

    /**
     * Variant class-name prefixes, parallel to {@link #getVariantBaseClasses()}.
     * Each entry produces one generated class per spec file plus one Suite class.
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

        // Parse spec_data.yml for category ordering.
        Map<String, String> fileToCategory = parseFileToCategory();
        Map<String, Integer> categoryIndexCount = parseCategoryIndexCounts();

        List<String> prefixes = getVariantPrefixes().get();
        List<String> baseClasses = getVariantBaseClasses().get();
        List<String> allEncodedPatterns = getVariantSpecFilePatterns().get();
        if (prefixes.size() != baseClasses.size() || prefixes.size() != allEncodedPatterns.size()) {
            throw new IllegalStateException("variantPrefixes, variantBaseClasses, and variantSpecFilePatterns must have the same length");
        }

        // Collect all spec files sorted for Suite generation (ascending index count, then category, then file name).
        File specDir = getSpecFilesDir().getAsFile().get();
        File[] specFiles = specDir.listFiles((dir, name) -> name.endsWith(".csv-spec"));
        if (specFiles == null) {
            return;
        }
        Arrays.sort(specFiles, buildSpecFileComparator(fileToCategory, categoryIndexCount));

        for (int i = 0; i < prefixes.size(); i++) {
            String prefix = prefixes.get(i);
            String baseClass = baseClasses.get(i);
            String encoded = allEncodedPatterns.get(i);
            List<String> patterns = encoded.isEmpty() ? List.of() : Arrays.asList(encoded.split(","));

            // Generate per-file classes and collect names in Suite order.
            List<String> suiteClasses = new ArrayList<>();
            for (File specFile : specFiles) {
                String specFileName = specFile.getName();
                if (patterns.isEmpty() == false && matchesAnyPattern(specFile, patterns) == false) {
                    continue;
                }
                String baseName = specFileName.substring(0, specFileName.length() - ".csv-spec".length());
                String pascalName = toPascalCase(baseName);
                String className = prefix + pascalName + "IT";
                File javaFile = new File(packageDir, className + ".java");
                Files.writeString(
                    javaFile.toPath(),
                    buildClassSource(packageName, className, baseClass, specFileName),
                    StandardCharsets.UTF_8
                );
                suiteClasses.add(className);
                getLogger().info("Generated {}", javaFile.getName());
            }

            // Generate the Suite class that runs all per-file classes in category-execution order.
            String suiteClassName = prefix + "SuiteIT";
            File suiteFile = new File(packageDir, suiteClassName + ".java");
            Files.writeString(suiteFile.toPath(), buildSuiteSource(packageName, suiteClassName, suiteClasses), StandardCharsets.UTF_8);
            getLogger().info("Generated suite {}", suiteFile.getName());
        }
    }

    /** Builds a comparator that orders spec files by ascending index count, then category name, then file name. */
    private static Comparator<File> buildSpecFileComparator(Map<String, String> fileToCategory, Map<String, Integer> categoryIndexCount) {
        return (a, b) -> {
            String baseA = a.getName().replace(".csv-spec", "");
            String baseB = b.getName().replace(".csv-spec", "");
            String catA = fileToCategory.getOrDefault(baseA, "core");
            String catB = fileToCategory.getOrDefault(baseB, "core");
            int countA = categoryIndexCount.getOrDefault(catA, 0);
            int countB = categoryIndexCount.getOrDefault(catB, 0);
            if (countA != countB) {
                return Integer.compare(countA, countB);
            }
            int catCmp = catA.compareTo(catB);
            if (catCmp != 0) {
                return catCmp;
            }
            return a.getName().compareTo(b.getName());
        };
    }

    @SuppressWarnings("unchecked")
    private Map<String, String> parseFileToCategory() throws IOException {
        Yaml yaml = new Yaml();
        try (FileInputStream is = new FileInputStream(getSpecDataYml().getAsFile().get())) {
            Map<String, Object> root = yaml.load(is);
            Map<String, Object> files = (Map<String, Object>) root.get("files");
            if (files == null) {
                return Map.of();
            }
            Map<String, String> result = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : files.entrySet()) {
                result.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
            return result;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Integer> parseCategoryIndexCounts() throws IOException {
        Yaml yaml = new Yaml();
        try (FileInputStream is = new FileInputStream(getSpecDataYml().getAsFile().get())) {
            Map<String, Object> root = yaml.load(is);
            Map<String, Object> categories = (Map<String, Object>) root.get("categories");
            if (categories == null) {
                return Map.of();
            }
            Map<String, Integer> result = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : categories.entrySet()) {
                Map<String, Object> catDef = (Map<String, Object>) entry.getValue();
                List<?> indices = catDef != null ? (List<?>) catDef.get("indices") : null;
                result.put(entry.getKey(), indices != null ? indices.size() : 0);
            }
            return result;
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

                @ParametersFactory(argumentFormatting = "csv-spec:%2$s.%3$s", shuffle = false)
                public static List<Object[]> readScriptSpec() throws Exception {
                    return readScriptSpec("/SPEC_FILE_NAME");
                }
            }
            """.replace("SPEC_FILE_NAME", specFileName)
            .replace("PACKAGE_NAME", packageName)
            .replace("BASE_CLASS_NAME", baseClassName)
            .replace("CLASS_NAME", className);
    }

    private static String buildSuiteSource(String packageName, String suiteClassName, List<String> classNames) {
        StringBuilder classesList = new StringBuilder();
        for (int i = 0; i < classNames.size(); i++) {
            if (i > 0) {
                classesList.append(",\n    ");
            }
            classesList.append(classNames.get(i)).append(".class");
        }
        return """
            /*
             * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
             * or more contributor license agreements. Licensed under the Elastic License
             * 2.0; you may not use this file except in compliance with the Elastic License
             * 2.0.
             */

            package PACKAGE_NAME;

            // THIS FILE IS AUTO-GENERATED by the generateEsqlSpecTests Gradle task.
            // DO NOT EDIT BY HAND.
            //
            // Classes are listed in category-execution order (ascending index count) so that
            // EsqlSpecTestCase's category-delta loading only swaps data when the category changes.
            // Run this suite with forkEvery = 0 to keep the shared static loadedCategory across classes.

            import org.junit.runner.RunWith;
            import org.junit.runners.Suite;

            @RunWith(Suite.class)
            @Suite.SuiteClasses({
                CLASSES_LIST
            })
            public class SUITE_CLASS_NAME {
                // intentionally empty — the Suite runner drives all child classes
            }
            """.replace("PACKAGE_NAME", packageName)
            .replace("SUITE_CLASS_NAME", suiteClassName)
            .replace("CLASSES_LIST", classesList.toString());
    }
}
