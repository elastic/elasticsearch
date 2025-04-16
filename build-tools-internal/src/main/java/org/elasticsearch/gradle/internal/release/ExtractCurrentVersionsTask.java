/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release;

import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.FieldDeclaration;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;
import org.gradle.initialization.layout.BuildLayout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import javax.inject.Inject;

public class ExtractCurrentVersionsTask extends AbstractVersionsTask {
    private static final Logger LOGGER = Logging.getLogger(ExtractCurrentVersionsTask.class);

    private Path outputFile;

    @Inject
    public ExtractCurrentVersionsTask(BuildLayout layout) {
        super(layout);
    }

    @Option(option = "output-file", description = "File to output tag information to")
    public void outputFile(String file) {
        this.outputFile = Path.of(file);
    }

    @TaskAction
    public void executeTask() throws IOException {
        if (outputFile == null) {
            throw new IllegalArgumentException("Output file not specified");
        }

        LOGGER.lifecycle("Extracting latest version information");

        List<String> output = new ArrayList<>();
        int transportVersion = readLatestVersion(rootDir.resolve(TRANSPORT_VERSIONS_FILE_PATH));
        LOGGER.lifecycle("Transport version: {}", transportVersion);
        output.add(TRANSPORT_VERSION_TYPE + ":" + transportVersion);

        int indexVersion = readLatestVersion(rootDir.resolve(INDEX_VERSIONS_FILE_PATH));
        LOGGER.lifecycle("Index version: {}", indexVersion);
        output.add(INDEX_VERSION_TYPE + ":" + indexVersion);

        LOGGER.lifecycle("Writing version information to {}", outputFile);
        Files.write(outputFile, output, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }

    static class FieldIdExtractor implements Consumer<FieldDeclaration> {
        private Integer highestVersionId;

        Integer highestVersionId() {
            return highestVersionId;
        }

        @Override
        public void accept(FieldDeclaration fieldDeclaration) {
            findSingleIntegerExpr(fieldDeclaration).ifPresent(id -> {
                if (highestVersionId != null && highestVersionId > id) {
                    LOGGER.warn("Version ids [{}, {}] out of order", highestVersionId, id);
                } else {
                    highestVersionId = id;
                }
            });
        }
    }

    private static int readLatestVersion(Path javaVersionsFile) throws IOException {
        CompilationUnit java = StaticJavaParser.parse(javaVersionsFile);

        FieldIdExtractor extractor = new FieldIdExtractor();
        java.walk(FieldDeclaration.class, extractor);   // walks in code file order
        if (extractor.highestVersionId == null) {
            throw new IllegalArgumentException("No version ids found in " + javaVersionsFile);
        }
        return extractor.highestVersionId;
    }
}
