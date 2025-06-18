/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release;

import com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static java.util.Comparator.comparing;
import static java.util.Comparator.comparingInt;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

/**
 * Generates the release notes i.e. list of changes that have gone into this release. They are grouped by the
 * type of change, then by team area.
 */
public class ReleaseNotesGenerator {

    private record ChangelogsBundleWrapper(
        QualifiedVersion version,
        ChangelogBundle bundle,
        Map<String, Map<String, List<ChangelogEntry>>> changelogsByTypeByArea,
        QualifiedVersion unqualifiedVersion,
        String versionWithoutSeparator,
        List<ChangelogEntry.Highlight> notableHighlights,
        List<ChangelogEntry.Highlight> nonNotableHighlights
    ) {}

    /**
     * These mappings translate change types into the headings as they should appear in the release notes.
     */
    private static final Map<String, String> TYPE_LABELS = new HashMap<>();

    static {
        TYPE_LABELS.put("breaking", "Breaking changes");
        TYPE_LABELS.put("breaking-java", "Breaking Java changes");
        TYPE_LABELS.put("bug", "Bug fixes");
        TYPE_LABELS.put("fixes", "Fixes");
        TYPE_LABELS.put("deprecation", "Deprecations");
        TYPE_LABELS.put("enhancement", "Enhancements");
        TYPE_LABELS.put("feature", "New features");
        TYPE_LABELS.put("features-enhancements", "Features and enhancements");
        TYPE_LABELS.put("new-aggregation", "New aggregation");
        TYPE_LABELS.put("regression", "Regressions");
        TYPE_LABELS.put("upgrade", "Upgrades");
    }

    /**
     * These are the types of changes that are considered "Features and Enhancements" in the release notes.
     */
    private static final List<String> FEATURE_ENHANCEMENT_TYPES = List.of("feature", "new-aggregation", "enhancement", "upgrade");

    static void update(File templateFile, File outputFile, List<ChangelogBundle> bundles) throws IOException {
        final String templateString = Files.readString(templateFile.toPath());

        try (FileWriter output = new FileWriter(outputFile)) {
            output.write(generateFile(templateString, bundles));
        }
    }

    @VisibleForTesting
    static String generateFile(String template, List<ChangelogBundle> bundles) throws IOException {
        var bundlesWrapped = new ArrayList<ChangelogsBundleWrapper>();

        for (var bundle : bundles) {
            var changelogs = bundle.changelogs();
            final var changelogsByTypeByArea = buildChangelogBreakdown(changelogs);

            final Map<Boolean, List<ChangelogEntry.Highlight>> groupedHighlights = changelogs.stream()
                .map(ChangelogEntry::getHighlight)
                .filter(Objects::nonNull)
                .sorted(comparingInt(ChangelogEntry.Highlight::getPr))
                .collect(groupingBy(ChangelogEntry.Highlight::isNotable, toList()));

            final var notableHighlights = groupedHighlights.getOrDefault(true, List.of());
            final var nonNotableHighlights = groupedHighlights.getOrDefault(false, List.of());

            final var version = QualifiedVersion.of(bundle.version());
            final var versionWithoutSeparator = version.withoutQualifier().toString().replaceAll("\\.", "");

            final var wrapped = new ChangelogsBundleWrapper(
                version,
                bundle,
                changelogsByTypeByArea,
                version.withoutQualifier(),
                versionWithoutSeparator,
                notableHighlights,
                nonNotableHighlights
            );

            bundlesWrapped.add(wrapped);
        }

        final Map<String, Object> bindings = new HashMap<>();
        bindings.put("TYPE_LABELS", TYPE_LABELS);
        bindings.put("changelogBundles", bundlesWrapped);

        return TemplateUtils.render(template, bindings);
    }

    /**
     * The new markdown release notes are grouping several of the old change types together.
     * This method maps the change type that developers use in the changelogs to the new type that the release notes cares about.
     */
    private static String getTypeFromEntry(ChangelogEntry entry) {
        if (entry.getBreaking() != null) {
            return "breaking";
        }

        if (FEATURE_ENHANCEMENT_TYPES.contains(entry.getType())) {
            return "features-enhancements";
        }

        if (entry.getType().equals("bug")) {
            return "fixes";
        }

        return entry.getType();
    }

    private static Map<String, Map<String, List<ChangelogEntry>>> buildChangelogBreakdown(Collection<ChangelogEntry> changelogs) {
        Map<String, Map<String, List<ChangelogEntry>>> changelogsByTypeByArea = changelogs.stream()
            .collect(
                groupingBy(
                    // Entries with breaking info are always put in the breaking section
                    entry -> getTypeFromEntry(entry),
                    TreeMap::new,
                    // Group changelogs for each type by their team area
                    groupingBy(
                        // `security` and `known-issue` areas don't need to supply an area
                        entry -> entry.getType().equals("known-issue") || entry.getType().equals("security") ? "_all_" : entry.getArea(),
                        TreeMap::new,
                        toList()
                    )
                )
            );

        // Sort per-area changelogs by their summary text. Assumes that the underlying list is sortable
        changelogsByTypeByArea.forEach(
            (_type, byTeam) -> byTeam.forEach((_team, changelogsForTeam) -> changelogsForTeam.sort(comparing(ChangelogEntry::getSummary)))
        );

        return changelogsByTypeByArea;
    }
}
