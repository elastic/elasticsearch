/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import groovy.text.SimpleTemplateEngine;

import com.google.common.annotations.VisibleForTesting;

import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class ensures that the release notes index page has the appropriate anchors and include directives
 * for the current repository version. It achieves this by parsing out the existing entries and writing
 * out the file again.
 */
public class ReleaseNotesIndexUpdater {

    static void update(File indexTemplate, File indexFile) throws IOException {
        final List<String> existingIndexLines = Files.readAllLines(indexFile.toPath());
        try (FileWriter indexFileWriter = new FileWriter(indexFile)) {
            generateFile(
                VersionProperties.getElasticsearchVersion(),
                existingIndexLines,
                Files.readString(indexTemplate.toPath()),
                indexFileWriter
            );
        }
    }

    @VisibleForTesting
    static void generateFile(Version version, List<String> existingIndexLines, String indexTemplate, Writer outputWriter)
        throws IOException {
        final List<String> existingVersions = existingIndexLines.stream()
            .filter(line -> line.startsWith("* <<release-notes-"))
            .map(line -> line.replace("* <<release-notes-", "").replace(">>", ""))
            .distinct()
            .collect(Collectors.toList());

        final List<String> existingIncludes = existingIndexLines.stream()
            .filter(line -> line.startsWith("include::"))
            .map(line -> line.replace("include::release-notes/", "").replace(".asciidoc[]", ""))
            .distinct()
            .collect(Collectors.toList());

        final String versionString = version.toString();

        if (existingVersions.contains(versionString) == false) {
            int insertionIndex = existingVersions.size() - 1;
            while (insertionIndex > 0 && Version.fromString(existingVersions.get(insertionIndex)).before(version)) {
                insertionIndex -= 1;
            }
            existingVersions.add(insertionIndex, versionString);
        }

        final String includeString = version.getMajor() + "." + version.getMinor();

        if (existingIncludes.contains(includeString) == false) {
            int insertionIndex = existingIncludes.size() - 1;
            while (insertionIndex > 0 && Version.fromString(ensurePatchVersion(existingIncludes.get(insertionIndex))).before(version)) {
                insertionIndex -= 1;
            }
            existingIncludes.add(insertionIndex, includeString);
        }

        final Map<String, Object> bindings = new HashMap<>();
        bindings.put("existingVersions", existingVersions);
        bindings.put("existingIncludes", existingIncludes);

        try {
            final SimpleTemplateEngine engine = new SimpleTemplateEngine();
            engine.createTemplate(indexTemplate).make(bindings).writeTo(outputWriter);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    private static String ensurePatchVersion(String version) {
        return version.matches("^\\d+\\.\\d+\\.\\d+.*$") ? version : version + ".0";
    }
}
