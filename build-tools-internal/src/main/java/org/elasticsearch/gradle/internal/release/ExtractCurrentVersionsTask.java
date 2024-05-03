/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.expr.IntegerLiteralExpr;

import org.gradle.api.DefaultTask;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;
import org.gradle.initialization.layout.BuildLayout;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.function.Consumer;

import javax.inject.Inject;

public class ExtractCurrentVersionsTask extends DefaultTask {
    private static final Logger LOGGER = Logging.getLogger(ExtractCurrentVersionsTask.class);

    static final String SERVER_MODULE_PATH = "server/src/main/java/";
    static final String VERSION_FILE_PATH = SERVER_MODULE_PATH + "org/elasticsearch/Version.java";

    final Path rootDir;

    private Path outputFile;

    @Inject
    public ExtractCurrentVersionsTask(BuildLayout layout) {
        rootDir = layout.getRootDirectory().toPath();
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

        // get the current version from Version.java
        int version = readLatestVersion(rootDir.resolve(VERSION_FILE_PATH));
        LOGGER.lifecycle("Version: {}", version);

        LOGGER.lifecycle("Writing version information to {}", outputFile);
        Files.write(
            outputFile,
            List.of("Version:" + version),
            StandardOpenOption.CREATE,
            StandardOpenOption.WRITE,
            StandardOpenOption.TRUNCATE_EXISTING
        );
    }

    static class FieldIdExtractor implements Consumer<FieldDeclaration> {
        private Integer highestVersionId;

        Integer highestVersionId() {
            return highestVersionId;
        }

        @Override
        public void accept(FieldDeclaration fieldDeclaration) {
            var ints = fieldDeclaration.findAll(IntegerLiteralExpr.class);
            switch (ints.size()) {
                case 0 -> {
                    // No ints in the field declaration, ignore
                }
                case 1 -> {
                    int id = ints.get(0).asNumber().intValue();
                    if (highestVersionId != null && highestVersionId > id) {
                        LOGGER.warn("Version ids [{}, {}] out of order", highestVersionId, id);
                    } else {
                        highestVersionId = id;
                    }
                }
                default -> LOGGER.warn("Multiple integers found in version field declaration [{}]", fieldDeclaration); // and ignore it
            }
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
