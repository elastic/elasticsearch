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
import org.gradle.api.GradleException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;

/**
 * Generates the page that contains an index into the breaking changes and lists deprecations for a minor version release,
 * and the individual pages for each breaking area.
 */
public class BreakingChangesGenerator {

    // Needs to match `changelog-schema.json`
    private static final List<String> BREAKING_AREAS = List.of(
        "Cluster and node setting",
        "Command line tool",
        "Index setting",
        "JVM option",
        "Java API",
        "Logging",
        "Mapping",
        "Packaging",
        "Painless",
        "REST API",
        "System requirement",
        "Transform"
    );

    static void update(
        File indexTemplateFile,
        File indexOutputFile,
        File outputDirectory,
        File areaTemplateFile,
        List<ChangelogEntry> entries
    ) throws IOException {
        if (outputDirectory.exists()) {
            if (outputDirectory.isDirectory() == false) {
                throw new GradleException("Path [" + outputDirectory + "] exists but isn't a directory!");
            }
        } else {
            Files.createDirectory(outputDirectory.toPath());
        }

        try (FileWriter output = new FileWriter(indexOutputFile)) {
            output.write(
                generateIndexFile(
                    QualifiedVersion.of(VersionProperties.getElasticsearch()),
                    Files.readString(indexTemplateFile.toPath()),
                    entries
                )
            );
        }

        String areaTemplate = Files.readString(areaTemplateFile.toPath());

        for (String breakingArea : BREAKING_AREAS) {
            final List<ChangelogEntry.Breaking> entriesForArea = entries.stream()
                .map(ChangelogEntry::getBreaking)
                .filter(entry -> entry != null && breakingArea.equals(entry.getArea()))
                .collect(Collectors.toList());

            if (entriesForArea.isEmpty()) {
                continue;
            }

            final String outputFilename = breakingArea.toLowerCase(Locale.ROOT).replaceFirst(" and", "").replaceAll(" ", "-")
                + "-changes.asciidoc";

            try (FileWriter output = new FileWriter(outputDirectory.toPath().resolve(outputFilename).toFile())) {
                output.write(
                    generateBreakingAreaFile(
                        QualifiedVersion.of(VersionProperties.getElasticsearch()),
                        areaTemplate,
                        breakingArea,
                        entriesForArea
                    )
                );
            }
        }
    }

    @VisibleForTesting
    static String generateIndexFile(QualifiedVersion version, String template, List<ChangelogEntry> entries) throws IOException {
        final Map<String, List<ChangelogEntry.Deprecation>> deprecationsByArea = entries.stream()
            .map(ChangelogEntry::getDeprecation)
            .filter(Objects::nonNull)
            .sorted(comparing(ChangelogEntry.Deprecation::getTitle))
            .collect(groupingBy(ChangelogEntry.Deprecation::getArea, TreeMap::new, Collectors.toList()));

        final List<String> breakingIncludeList = entries.stream()
            .filter(each -> each.getBreaking() != null)
            .map(each -> each.getBreaking().getArea().toLowerCase(Locale.ROOT).replaceFirst(" and", "").replaceAll(" ", "-"))
            .distinct()
            .sorted()
            .toList();

        final Map<String, Object> bindings = new HashMap<>();
        bindings.put("breakingIncludeList", breakingIncludeList);
        bindings.put("deprecationsByArea", deprecationsByArea);
        bindings.put("isElasticsearchSnapshot", version.isSnapshot());
        bindings.put("majorDotMinor", version.getMajor() + "." + version.getMinor());
        bindings.put("majorMinor", String.valueOf(version.getMajor()) + version.getMinor());
        bindings.put("nextMajor", (version.getMajor() + 1) + ".0");
        bindings.put("version", version);

        return TemplateUtils.render(template, bindings);
    }

    @VisibleForTesting
    static String generateBreakingAreaFile(
        QualifiedVersion version,
        String template,
        String breakingArea,
        List<ChangelogEntry.Breaking> entriesForArea
    ) throws IOException {
        final Map<Boolean, Set<ChangelogEntry.Breaking>> breakingEntriesByNotability = entriesForArea.stream()
            .collect(
                groupingBy(
                    ChangelogEntry.Breaking::isNotable,
                    toCollection(() -> new TreeSet<>(comparing(ChangelogEntry.Breaking::getTitle)))
                )
            );

        final Map<String, Object> bindings = new HashMap<>();
        bindings.put("breakingArea", breakingArea);
        bindings.put("breakingEntriesByNotability", breakingEntriesByNotability);
        bindings.put("breakingAreaAnchor", breakingArea.toLowerCase(Locale.ROOT).replaceFirst(" and", "").replaceAll(" ", "_"));
        bindings.put("majorMinor", String.valueOf(version.getMajor()) + version.getMinor());

        return TemplateUtils.render(template, bindings);
    }
}
