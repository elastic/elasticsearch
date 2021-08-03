/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import com.google.common.annotations.VisibleForTesting;
import groovy.text.SimpleTemplateEngine;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.gradle.api.GradleException;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Generates the release notes i.e. list of changes that have gone into this release. They are grouped by the
 * type of change, then by team area.
 */
public class ReleaseNotesGenerator {
    /**
     * These mappings translate change types into the headings as they should appears in the release notes.
     */
    private static final Map<String, String> TYPE_LABELS = new HashMap<>();

    static {
        TYPE_LABELS.put("breaking", "Breaking changes");
        TYPE_LABELS.put("breaking-java", "Breaking Java changes");
        TYPE_LABELS.put("bug", "Bug fixes");
        TYPE_LABELS.put("deprecation", "Deprecations");
        TYPE_LABELS.put("enhancement", "Enhancements");
        TYPE_LABELS.put("feature", "New features");
        TYPE_LABELS.put("regression", "Regressions");
        TYPE_LABELS.put("upgrade", "Upgrades");
    }

    static void update(File templateFile, File outputFile, List<ChangelogEntry> changelogs) throws IOException {
        final String templateString = Files.readString(templateFile.toPath());

        try (FileWriter output = new FileWriter(outputFile)) {
            generateFile(VersionProperties.getElasticsearchVersion(), templateString, changelogs, output);
        }
    }

    @VisibleForTesting
    static void generateFile(Version version, String template, List<ChangelogEntry> changelogs, Writer outputWriter) throws IOException {
        final var changelogsByVersionByTypeByArea = buildChangelogBreakdown(version, changelogs);

        final Map<String, Object> bindings = new HashMap<>();
        bindings.put("changelogsByVersionByTypeByArea", changelogsByVersionByTypeByArea);
        bindings.put("TYPE_LABELS", TYPE_LABELS);

        try {
            final SimpleTemplateEngine engine = new SimpleTemplateEngine();
            engine.createTemplate(template).make(bindings).writeTo(outputWriter);
        } catch (ClassNotFoundException e) {
            throw new GradleException("Failed to generate file from template", e);
        }
    }

    private static Map<Version, Map<String, Map<String, List<ChangelogEntry>>>> buildChangelogBreakdown(
        Version elasticsearchVersion,
        List<ChangelogEntry> changelogs
    ) {
        final Predicate<Version> includedInSameMinor = v -> v.getMajor() == elasticsearchVersion.getMajor()
            && v.getMinor() == elasticsearchVersion.getMinor();

        final Map<Version, Map<String, Map<String, List<ChangelogEntry>>>> changelogsByVersionByTypeByArea = changelogs.stream()
            .collect(
                Collectors.groupingBy(
                    // Key changelog entries by the earlier version in which they were released
                    entry -> entry.getVersions()
                        .stream()
                        .map(v -> Version.fromString(v.replaceFirst("^v", "")))
                        .filter(includedInSameMinor)
                        .sorted()
                        .findFirst()
                        .get(),

                    // Generate a reverse-ordered map. Despite the IDE saying the type can be inferred, removing it
                    // causes the compiler to complain.
                    () -> new TreeMap<Version, Map<String, Map<String, List<ChangelogEntry>>>>(Comparator.reverseOrder()),

                    // Group changelogs entries by their change type
                    Collectors.groupingBy(
                        // Entries with breaking info are always put in the breaking section
                        entry -> entry.getBreaking() == null ? entry.getType() : "breaking",
                        TreeMap::new,
                        // Group changelogs for each type by their team area
                        Collectors.groupingBy(
                            // `security` and `known-issue` areas don't need to supply an area
                            entry -> entry.getType().equals("known-issue") || entry.getType().equals("security")
                                ? "_all_"
                                : entry.getArea(),
                            TreeMap::new,
                            Collectors.toList()
                        )
                    )
                )
            );

        // Sort per-area changelogs by their summary text. Assumes that the underlying list is sortable
        changelogsByVersionByTypeByArea.forEach(
            (_version, byVersion) -> byVersion.forEach(
                (_type, byTeam) -> byTeam.forEach(
                    (_team, changelogsForTeam) -> changelogsForTeam.sort(Comparator.comparing(ChangelogEntry::getSummary))
                )
            )
        );

        return changelogsByVersionByTypeByArea;
    }
}
