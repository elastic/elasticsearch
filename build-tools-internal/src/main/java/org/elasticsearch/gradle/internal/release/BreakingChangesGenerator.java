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

import org.elasticsearch.gradle.VersionProperties;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.groupingBy;

/**
 * Generates the page that lists the breaking changes and deprecations for a minor version release.
 */
public class BreakingChangesGenerator {

    static void update(File templateFile, File outputFile, List<ChangelogEntry> entries) throws IOException {
        try (FileWriter output = new FileWriter(outputFile)) {
            generateFile(
                QualifiedVersion.of(VersionProperties.getElasticsearch()),
                Files.readString(templateFile.toPath()),
                output,
                entries
            );
        }
    }

    @VisibleForTesting
    static void generateFile(QualifiedVersion version, String template, Writer outputWriter, List<ChangelogEntry> entries)
        throws IOException {

        final Map<Boolean, Map<String, List<ChangelogEntry.Breaking>>> breakingChangesByNotabilityByArea = entries.stream()
            .map(ChangelogEntry::getBreaking)
            .filter(Objects::nonNull)
            .sorted(comparing(ChangelogEntry.Breaking::getTitle))
            .collect(
                groupingBy(
                    ChangelogEntry.Breaking::isNotable,
                    groupingBy(ChangelogEntry.Breaking::getArea, TreeMap::new, Collectors.toList())
                )
            );

        final Map<String, List<ChangelogEntry.Deprecation>> deprecationsByArea = entries.stream()
            .map(ChangelogEntry::getDeprecation)
            .filter(Objects::nonNull)
            .sorted(comparing(ChangelogEntry.Deprecation::getTitle))
            .collect(groupingBy(ChangelogEntry.Deprecation::getArea, TreeMap::new, Collectors.toList()));

        final Map<String, Object> bindings = new HashMap<>();
        bindings.put("breakingChangesByNotabilityByArea", breakingChangesByNotabilityByArea);
        bindings.put("deprecationsByArea", deprecationsByArea);
        bindings.put("isElasticsearchSnapshot", version.isSnapshot());
        bindings.put("majorDotMinor", version.getMajor() + "." + version.getMinor());
        bindings.put("majorMinor", String.valueOf(version.getMajor()) + version.getMinor());
        bindings.put("nextMajor", (version.getMajor() + 1) + ".0");
        bindings.put("version", version);

        try {
            final SimpleTemplateEngine engine = new SimpleTemplateEngine();
            engine.createTemplate(template).make(bindings).writeTo(outputWriter);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
