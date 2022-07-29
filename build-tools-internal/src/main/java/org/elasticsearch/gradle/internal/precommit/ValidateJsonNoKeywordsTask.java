/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.work.ChangeType;
import org.gradle.work.Incremental;
import org.gradle.work.InputChanges;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

/**
 * Incremental task to validate that the API names in set of JSON files do not contain
 * programming language keywords.
 * <p>
 * The keywords are defined in a JSON file, although it's worth noting that what is and isn't a
 * keyword depends on the language and sometimes the context in which a keyword is used. For example,
 * `delete` is an operator in JavaScript, but it isn't in the keywords list for JavaScript or
 * TypeScript because it's OK to use `delete` as a method name.
 */
public class ValidateJsonNoKeywordsTask extends DefaultTask {

    private File jsonKeywords;
    private File report;
    private FileCollection inputFiles;

    @Incremental
    @InputFiles
    public FileCollection getInputFiles() {
        return inputFiles;
    }

    public void setInputFiles(FileCollection inputFiles) {
        this.inputFiles = inputFiles;
    }

    @InputFile
    public File getJsonKeywords() {
        return jsonKeywords;
    }

    public void setJsonKeywords(File jsonKeywords) {
        this.jsonKeywords = jsonKeywords;
    }

    public void setReport(File report) {
        this.report = report;
    }

    @OutputFile
    public File getReport() {
        return report;
    }

    @TaskAction
    public void validate(InputChanges inputChanges) {
        final ObjectMapper mapper = new ObjectMapper().configure(JsonParser.Feature.ALLOW_COMMENTS, true);
        final Map<File, Set<String>> errors = new LinkedHashMap<>();

        getLogger().debug("Loading keywords from {}", jsonKeywords.getName());

        final Map<String, Set<String>> languagesByKeyword = loadKeywords(mapper);

        // incrementally evaluate input files
        StreamSupport.stream(inputChanges.getFileChanges(getInputFiles()).spliterator(), false)
            .filter(f -> f.getChangeType() != ChangeType.REMOVED)
            .forEach(fileChange -> {
                File file = fileChange.getFile();
                if (file.isDirectory()) {
                    return;
                }

                getLogger().debug("Checking {}", file.getName());

                try {
                    final JsonNode jsonNode = mapper.readTree(file);

                    if (jsonNode.isObject() == false) {
                        errors.put(file, Set.of("Expected an object, but found: " + jsonNode.getNodeType()));
                        return;
                    }

                    final ObjectNode rootNode = (ObjectNode) jsonNode;

                    if (rootNode.size() != 1) {
                        errors.put(file, Set.of("Expected an object with exactly 1 key, but found " + rootNode.size() + " keys"));
                        return;
                    }

                    final String apiName = rootNode.fieldNames().next();

                    for (String component : apiName.split("\\.")) {
                        if (languagesByKeyword.containsKey(component)) {
                            final Set<String> errorsForFile = errors.computeIfAbsent(file, _file -> new HashSet<>());
                            errorsForFile.add(
                                component + " is a reserved keyword in these languages: " + languagesByKeyword.get(component)
                            );
                        }
                    }
                } catch (IOException e) {
                    errors.put(file, Set.of("Failed to load file: " + e.getMessage()));
                }
            });

        if (errors.isEmpty()) {
            return;
        }

        try {
            try (PrintWriter pw = new PrintWriter(getReport())) {
                pw.println("---------- Validation Report -----------");
                pw.println("Some API names were found that, when client code is generated for these APIS,");
                pw.println("could conflict with the reserved words in some programming languages. It may");
                pw.println("still be possible to use these API names, but you will need to verify whether");
                pw.println("the API name (and its components) can be used as method names, and update the");
                pw.println("list of keywords below. The safest action is to rename the API to avoid conflicts.");
                pw.println();
                pw.printf("Keywords source: %s%n", getJsonKeywords());
                pw.println();
                pw.println("---------- Validation Errors -----------");
                pw.println();
                errors.forEach((file, errorsForFile) -> {
                    pw.printf("File: %s%n", file);
                    errorsForFile.forEach(err -> pw.printf("\t%s%n", err));
                    pw.println();
                });
            }
        } catch (FileNotFoundException e) {
            throw new GradleException("Failed to write keywords report", e);
        }

        String message = String.format(
            Locale.ROOT,
            "Error validating JSON. See the report at: %s%s%s",
            getReport().toURI().toASCIIString(),
            System.lineSeparator(),
            String.format("Verification failed: %d files contained %d violations", errors.keySet().size(), errors.values().size())
        );
        throw new GradleException(message);
    }

    /**
     * Loads the known keywords. Although the JSON on disk maps from language to keywords, this method
     * inverts this to map from keyword to languages. This is because the same keywords are found in
     * multiple languages, so it is easier and more useful to have a single map of keywords.
     *
     * @return a mapping from keyword to languages.
     */
    private Map<String, Set<String>> loadKeywords(ObjectMapper mapper) {
        Map<String, Set<String>> languagesByKeyword = new HashMap<>();

        try {
            final ObjectNode keywordsNode = ((ObjectNode) mapper.readTree(this.jsonKeywords));

            keywordsNode.fieldNames().forEachRemaining(eachLanguage -> {
                keywordsNode.get(eachLanguage).elements().forEachRemaining(e -> {
                    final String eachKeyword = e.textValue();
                    final Set<String> languages = languagesByKeyword.computeIfAbsent(eachKeyword, _keyword -> new HashSet<>());
                    languages.add(eachLanguage);
                });
            });
        } catch (IOException e) {
            throw new GradleException("Failed to load keywords JSON from " + jsonKeywords.getName() + " - " + e.getMessage(), e);
        }

        return languagesByKeyword;
    }
}
