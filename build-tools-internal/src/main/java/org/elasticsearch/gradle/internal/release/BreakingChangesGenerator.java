/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import com.google.common.annotations.VisibleForTesting;

import org.elasticsearch.gradle.VersionProperties;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

/**
 * Generates the page that contains breaking changes deprecations for a minor release series.
 */
public class BreakingChangesGenerator {

    static void update(File migrationTemplateFile, File migrationOutputFile, List<ChangelogEntry> entries) throws IOException {
        try (FileWriter output = new FileWriter(migrationOutputFile)) {
            output.write(
                generateMigrationFile(
                    QualifiedVersion.of(VersionProperties.getElasticsearch()),
                    Files.readString(migrationTemplateFile.toPath()),
                    entries
                )
            );
        }
    }

    @VisibleForTesting
    static String generateMigrationFile(QualifiedVersion version, String template, List<ChangelogEntry> entries) throws IOException {
        final Map<Boolean, Map<String, List<ChangelogEntry.Deprecation>>> deprecationsByNotabilityByArea = entries.stream()
            .map(ChangelogEntry::getDeprecation)
            .filter(Objects::nonNull)
            .sorted(comparing(ChangelogEntry.Deprecation::getTitle))
            .collect(
                groupingBy(
                    ChangelogEntry.Deprecation::isNotable,
                    TreeMap::new,
                    groupingBy(ChangelogEntry.Deprecation::getArea, TreeMap::new, toList())
                )
            );

        final Map<Boolean, Map<String, List<ChangelogEntry.Breaking>>> breakingByNotabilityByArea = entries.stream()
            .map(ChangelogEntry::getBreaking)
            .filter(Objects::nonNull)
            .sorted(comparing(ChangelogEntry.Breaking::getTitle))
            .collect(
                groupingBy(
                    ChangelogEntry.Breaking::isNotable,
                    TreeMap::new,
                    groupingBy(ChangelogEntry.Breaking::getArea, TreeMap::new, toList())
                )
            );

        final Map<String, Object> bindings = new HashMap<>();
        bindings.put("breakingByNotabilityByArea", breakingByNotabilityByArea);
        bindings.put("deprecationsByNotabilityByArea", deprecationsByNotabilityByArea);
        bindings.put("isElasticsearchSnapshot", version.isSnapshot());
        bindings.put("majorDotMinor", version.major() + "." + version.minor());
        bindings.put("majorDotMinorDotRevision", version.major() + "." + version.minor() + "." + version.revision());
        bindings.put("majorMinor", String.valueOf(version.major()) + version.minor());
        bindings.put("nextMajor", (version.major() + 1) + ".0");
        bindings.put("version", version);

        return TemplateUtils.render(template, bindings);
    }
}
