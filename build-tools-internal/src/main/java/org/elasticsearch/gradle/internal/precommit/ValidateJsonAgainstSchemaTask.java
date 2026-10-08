/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.precommit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.Error;
import com.networknt.schema.Schema;
import com.networknt.schema.SchemaException;
import com.networknt.schema.SchemaRegistry;
import com.networknt.schema.SpecificationVersion;

import org.elasticsearch.gradle.internal.conventions.problems.ElasticsearchBuildProblems;
import org.gradle.api.DefaultTask;
import org.gradle.api.UncheckedIOException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.problems.Problem;
import org.gradle.api.problems.ProblemId;
import org.gradle.api.problems.ProblemReporter;
import org.gradle.api.problems.Problems;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.work.ChangeType;
import org.gradle.work.FileChange;
import org.gradle.work.Incremental;
import org.gradle.work.InputChanges;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;

import javax.inject.Inject;

/**
 * Incremental task to validate a set of JSON files against a schema.
 */
@CacheableTask
public class ValidateJsonAgainstSchemaTask extends DefaultTask {
    private File jsonSchema;
    private File report;
    private FileCollection inputFiles;
    private final ProblemReporter problemReporter;

    @Inject
    public ValidateJsonAgainstSchemaTask(Problems problems) {
        this.problemReporter = problems.getReporter();
    }

    @Incremental
    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public FileCollection getInputFiles() {
        return inputFiles;
    }

    public void setInputFiles(FileCollection inputFiles) {
        this.inputFiles = inputFiles;
    }

    @InputFile
    @PathSensitive(PathSensitivity.RELATIVE)
    public File getJsonSchema() {
        return jsonSchema;
    }

    public void setJsonSchema(File jsonSchema) {
        this.jsonSchema = jsonSchema;
    }

    public void setReport(File report) {
        this.report = report;
    }

    @OutputFile
    public File getReport() {
        return this.report;
    }

    @Internal
    protected ObjectMapper getMapper() {
        return new ObjectMapper();
    }

    @Internal
    protected String getFileType() {
        return "JSON";
    }

    @TaskAction
    public void validate(InputChanges inputChanges) throws IOException {
        final File jsonSchemaOnDisk = getJsonSchema();
        final Schema jsonSchema = buildSchemaObject(jsonSchemaOnDisk);

        final Map<File, Set<String>> errors = new LinkedHashMap<>();
        final List<Problem> problems = new ArrayList<>();
        final ObjectMapper mapper = this.getMapper();

        // incrementally evaluate input files
        // validate all files and hold on to errors for a complete report if there are failures
        StreamSupport.stream(inputChanges.getFileChanges(getInputFiles()).spliterator(), false)
            .filter(f -> f.getChangeType() != ChangeType.REMOVED)
            .map(FileChange::getFile)
            .filter(file -> file.isDirectory() == false)
            .forEach(file -> {
                try {
                    List<Error> validationErrors = jsonSchema.validate(mapper.readTree(file));
                    maybeLogAndCollectError(validationErrors, errors, problems, file);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        if (errors.isEmpty()) {
            Files.writeString(getReport().toPath(), "Success! No validation errors found.", StandardOpenOption.CREATE);
        } else {
            try (PrintWriter printWriter = new PrintWriter(getReport())) {
                printWriter.printf("Schema: %s%n", jsonSchemaOnDisk);
                printWriter.println("----------Validation Errors-----------");
                errors.values().stream().flatMap(Collection::stream).forEach(printWriter::println);
            }
            StringBuilder sb = new StringBuilder();
            sb.append("Verification failed. See the report at: ");
            sb.append(getReport().toURI().toASCIIString());
            sb.append(System.lineSeparator());
            sb.append(
                String.format(
                    "Error validating %s: %d files contained %d violations",
                    getFileType(),
                    errors.keySet().size(),
                    errors.values().size()
                )
            );
            throw problemReporter.throwing(new SchemaException(sb.toString()), problems);
        }
    }

    private Schema buildSchemaObject(File jsonSchemaOnDisk) throws IOException {
        final ObjectMapper jsonMapper = new ObjectMapper();
        final SchemaRegistry factory = SchemaRegistry.withDefaultDialect(SpecificationVersion.DRAFT_7);
        return factory.getSchema(jsonMapper.readTree(jsonSchemaOnDisk));
    }

    private void maybeLogAndCollectError(List<Error> messages, Map<File, Set<String>> errors, List<Problem> problems, File file) {
        final String fileType = getFileType();
        for (Error message : messages) {
            getLogger().error("[validate {}][ERROR][{}][{}]", fileType, file.getName(), message.toString());
            errors.computeIfAbsent(file, k -> new LinkedHashSet<>())
                .add(String.format("%s: %s", file.getAbsolutePath(), message.toString()));
            problems.add(
                problemReporter.create(
                    ProblemId.create("schema-violation", fileType + " schema violation", ElasticsearchBuildProblems.JSON_VALIDATION),
                    spec -> spec.contextualLabel(fileType + " validation error in " + file.getName() + ": " + message)
                        .fileLocation(file.getAbsolutePath())
                        .solution("Fix the " + fileType + " file to conform to the schema at " + getJsonSchema().getAbsolutePath())
                )
            );
        }
    }
}
