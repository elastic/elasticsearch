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

import org.elasticsearch.gradle.Version;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.annotation.Nullable;
import javax.inject.Inject;

public class TagVersionsTask extends DefaultTask {
    private static final Logger LOGGER = Logging.getLogger(TagVersionsTask.class);

    static final String SERVER_MODULE_PATH = "server/src/main/java/";
    static final String TRANSPORT_VERSION_FILE_PATH = SERVER_MODULE_PATH + "org/elasticsearch/TransportVersions.java";
    static final String INDEX_VERSION_FILE_PATH = SERVER_MODULE_PATH + "org/elasticsearch/index/IndexVersions.java";

    static final String SERVER_RESOURCES_PATH = "server/src/main/resources/";
    static final String TRANSPORT_VERSIONS_RECORD = SERVER_RESOURCES_PATH + "org/elasticsearch/TransportVersions.csv";
    static final String INDEX_VERSIONS_RECORD = SERVER_RESOURCES_PATH + "org/elasticsearch/index/IndexVersions.csv";

    final Path rootDir;

    private Version tagVersion;

    @Nullable
    private Path outputFile;

    @Inject
    public TagVersionsTask(BuildLayout layout) {
        rootDir = layout.getRootDirectory().toPath();
    }

    @Option(option = "tag-version", description = "Specifies the release version to tag")
    public void tagVersion(String version) {
        this.tagVersion = Version.fromString(version);
    }

    @Option(option = "output-file", description = "File to output tag information to")
    public void outputFile(Path file) {
        this.outputFile = file;
    }

    @TaskAction
    public void executeTask() throws IOException {
        if (tagVersion == null) {
            throw new IllegalArgumentException("No version to tag specified");
        }

        LOGGER.lifecycle("Tagging version {} in all version records", tagVersion);
        var outputFile = this.outputFile;
        List<String> outputLines = outputFile != null ? new ArrayList<>() : null;

        LOGGER.lifecycle("Adding version record for TransportVersion to [{}]", TRANSPORT_VERSIONS_RECORD);
        int transportVersionId = processVersionFiles(
            rootDir.resolve(TRANSPORT_VERSION_FILE_PATH),
            rootDir.resolve(TRANSPORT_VERSIONS_RECORD)
        );
        recordTagInfo(outputLines, TRANSPORT_VERSIONS_RECORD, tagVersion, transportVersionId);

        LOGGER.lifecycle("Adding version record for IndexVersion to [{}]", INDEX_VERSIONS_RECORD);
        int indexVersionId = processVersionFiles(rootDir.resolve(INDEX_VERSION_FILE_PATH), rootDir.resolve(INDEX_VERSIONS_RECORD));
        recordTagInfo(outputLines, INDEX_VERSIONS_RECORD, tagVersion, indexVersionId);

        if (outputFile != null) {
            LOGGER.lifecycle("Writing tag information to [{}]", outputFile);
            Files.write(outputFile, outputLines, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        }
    }

    private int processVersionFiles(Path javaVersionsFile, Path versionRecordsFile) throws IOException {
        int versionId = readLatestVersion(javaVersionsFile);

        List<String> versionRecords = Files.readAllLines(versionRecordsFile);
        var modified = addVersionRecord(versionRecords, tagVersion, versionId);
        if (modified.isPresent()) {
            Files.write(versionRecordsFile, modified.get(), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
        }
        return versionId;
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

    private static class FieldIdExtractor implements Consumer<FieldDeclaration> {
        private Integer highestVersionId;

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

    private static final Pattern VERSION_LINE = Pattern.compile("(\\d+\\.\\d+\\.\\d+),(\\d+)(\\h*#.*)?");

    static Optional<List<String>> addVersionRecord(List<String> versionRecordLines, Version release, int id) {
        Map<Version, Integer> versions = versionRecordLines.stream().map(l -> {
            Matcher m = VERSION_LINE.matcher(l);
            if (m.matches() == false) throw new IllegalArgumentException(String.format("Incorrect format for line [%s]", l));
            return m;
        }).collect(Collectors.toMap(m -> Version.fromString(m.group(1)), m -> Integer.parseInt(m.group(2)), (k1, k2) -> {
            throw new IllegalArgumentException("Duplicate versions " + k1);
        }, TreeMap::new));

        Integer existing = versions.putIfAbsent(release, id);
        if (existing != null) {
            if (existing.equals(id)) {
                LOGGER.lifecycle("Version id [{}] for release [{}] already recorded", id, release);
                return Optional.empty();
            } else {
                throw new IllegalArgumentException(
                    String.format(
                        "Release [%s] already recorded with version id [%s], cannot update to version [%s]",
                        release,
                        existing,
                        id
                    )
                );
            }
        }

        LOGGER.lifecycle("Added version id [{}] record for release [{}]", id, release);
        return Optional.of(versions.entrySet().stream().map(e -> e.getKey() + "," + e.getValue()).toList());
    }

    private static void recordTagInfo(List<String> lines, String recordFile, Version version, int id) {
        if (lines != null) {
            lines.add(recordFile + ":");
            lines.add(version.toString() + "," + id);
        }
    }
}
