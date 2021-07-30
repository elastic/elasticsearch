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
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Generates the release highlights notes, for changelog files that contain the <code>highlight</code> field.
 */
public class ReleaseHighlightsGenerator {
    static void update(File templateFile, File outputFile, List<ChangelogEntry> entries) throws IOException {
        try (FileWriter output = new FileWriter(outputFile)) {
            generateFile(VersionProperties.getElasticsearchVersion(), Files.readString(templateFile.toPath()), entries, output);
        }
    }

    @VisibleForTesting
    static void generateFile(Version version, String templateFile, List<ChangelogEntry> entries, FileWriter outputWriter)
        throws IOException {
        final List<String> priorVersions = new ArrayList<>();

        if (version.getMinor() > 0) {
            final int major = version.getMajor();
            for (int minor = version.getMinor(); minor >= 0; minor--) {
                String majorMinor = major + "." + minor;
                String fileSuffix = "";
                if (major == 7 && minor < 7) {
                    fileSuffix = "-" + majorMinor + ".0";
                }
                priorVersions.add("{ref-bare}/" + majorMinor + "/release-highlights" + fileSuffix + ".html[" + majorMinor + "]");
            }
        }

        final Map<Boolean, List<ChangelogEntry.Highlight>> groupedHighlights = entries.stream()
            .map(ChangelogEntry::getHighlight)
            .filter(Objects::nonNull)
            .collect(Collectors.groupingBy(ChangelogEntry.Highlight::isNotable, Collectors.toList()));

        final List<ChangelogEntry.Highlight> notableHighlights = groupedHighlights.getOrDefault(true, List.of());
        final List<ChangelogEntry.Highlight> nonNotableHighlights = groupedHighlights.getOrDefault(false, List.of());

        final Map<String, Object> bindings = new HashMap<>();
        bindings.put("priorVersions", priorVersions);
        bindings.put("notableHighlights", notableHighlights);
        bindings.put("nonNotableHighlights", nonNotableHighlights);

        try {
            final SimpleTemplateEngine engine = new SimpleTemplateEngine();
            engine.createTemplate(templateFile).make(bindings).writeTo(outputWriter);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
