/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release;

import org.elasticsearch.gradle.Version;
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
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.inject.Inject;

public class TagVersionsTask extends AbstractVersionsTask {
    private static final Logger LOGGER = Logging.getLogger(TagVersionsTask.class);

    private Version releaseVersion;

    private Map<String, Integer> tagVersions = Map.of();

    @Inject
    public TagVersionsTask(BuildLayout layout) {
        super(layout);
    }

    @Option(option = "release", description = "The release version to be tagged")
    public void release(String version) {
        releaseVersion = Version.fromString(version);
    }

    @Option(option = "tag-version", description = "Version id to tag. Of the form <VersionType>:<id>.")
    public void tagVersions(List<String> version) {
        this.tagVersions = splitVersionIds(version);
    }

    @TaskAction
    public void executeTask() throws IOException {
        if (releaseVersion == null) {
            throw new IllegalArgumentException("Release version not specified");
        }
        if (tagVersions.isEmpty()) {
            throw new IllegalArgumentException("No version tags specified");
        }

        LOGGER.lifecycle("Tagging version {} component ids", releaseVersion);

        var versions = expandV7Version(tagVersions);

        for (var v : versions.entrySet()) {
            Path recordFile = switch (v.getKey()) {
                case TRANSPORT_VERSION_TYPE -> rootDir.resolve(TRANSPORT_VERSIONS_RECORD);
                case INDEX_VERSION_TYPE -> rootDir.resolve(INDEX_VERSIONS_RECORD);
                default -> throw new IllegalArgumentException("Unknown version type " + v.getKey());
            };

            LOGGER.lifecycle("Adding version record for {} to [{}]: [{},{}]", v.getKey(), recordFile, releaseVersion, v.getValue());

            Path file = rootDir.resolve(recordFile);
            List<String> versionRecords = Files.readAllLines(file);
            var modified = addVersionRecord(versionRecords, releaseVersion, v.getValue());
            if (modified.isPresent()) {
                Files.write(
                    file,
                    modified.get(),
                    StandardOpenOption.CREATE,
                    StandardOpenOption.WRITE,
                    StandardOpenOption.TRUNCATE_EXISTING
                );
            }
        }
    }

    /*
     * V7 just extracts a single Version. If so, this version needs to be applied to transport and index versions.
     */
    private static Map<String, Integer> expandV7Version(Map<String, Integer> tagVersions) {
        Integer v7Version = tagVersions.get("Version");
        if (v7Version == null) return tagVersions;

        return Map.of(TRANSPORT_VERSION_TYPE, v7Version, INDEX_VERSION_TYPE, v7Version);
    }

    private static final Pattern VERSION_LINE = Pattern.compile("(\\d+\\.\\d+\\.\\d+),(\\d+)");

    static Optional<List<String>> addVersionRecord(List<String> versionRecordLines, Version release, int id) {
        Map<Version, Integer> versions = versionRecordLines.stream().map(l -> {
            Matcher m = VERSION_LINE.matcher(l);
            if (m.matches() == false) throw new IllegalArgumentException(String.format("Incorrect format for line [%s]", l));
            return m;
        }).collect(Collectors.toMap(m -> Version.fromString(m.group(1)), m -> Integer.parseInt(m.group(2))));

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

        return Optional.of(
            versions.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(e -> e.getKey() + "," + e.getValue()).toList()
        );
    }
}
