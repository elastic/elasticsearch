/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.release;

import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

/**
 * This class ensures that the release notes index page has the appropriate anchor and include directive
 * for the current repository version. It achieves this by parsing out the existing entries and writing
 * out the file again.
 */
public class ReleaseNotesIndexUpdater {

    public static void update(File indexFile) throws IOException {
        final Version version = VersionProperties.getElasticsearchVersion();
        final List<String> indexLines = Files.readAllLines(indexFile.toPath());

        final List<String> existingVersions = indexLines.stream()
            .filter(line -> line.startsWith("* <<release-notes-"))
            .map(line -> line.replace("* <<release-notes-", "").replace(">>", ""))
            .collect(Collectors.toList());

        final List<String> existingIncludes = indexLines.stream()
            .filter(line -> line.startsWith("include::"))
            .map(line -> line.replace("include::release-notes/", "").replace(".asciidoc[]", ""))
            .collect(Collectors.toList());

        final String versionString = VersionProperties.getElasticsearch();

        if (existingVersions.contains(versionString) == false) {
            int insertionIndex = existingVersions.size() - 1;
            while (insertionIndex > 0 && Version.fromString(existingVersions.get(insertionIndex)).before(version)) {
                insertionIndex -= 1;
            }
            existingVersions.add(insertionIndex, version.toString());
        }

        final String includeString = version.getMajor() + "." + version.getMinor();

        if (existingIncludes.contains(includeString) == false) {
            int insertionIndex = existingIncludes.size() - 1;
            while (insertionIndex > 0 && Version.fromString(ensurePatchVersion(existingIncludes.get(insertionIndex))).before(version)) {
                insertionIndex -= 1;
            }
            existingIncludes.add(insertionIndex, includeString);
        }

        final PrintStream out = new PrintStream(indexFile);

        out.println("[[es-release-notes]]");
        out.println("= Release notes");
        out.println();
        out.println("[partintro]");
        out.println("--");
        out.println();
        out.println("This section summarizes the changes in each release.");
        out.println();
        existingVersions.forEach(v -> out.println("* <<release-notes-" + v + ">>"));
        out.println();
        out.println("--");
        out.println();
        existingIncludes.forEach(majorMinor -> out.println("include::release-notes/" + majorMinor + ".asciidoc[]"));
    }

    private static String ensurePatchVersion(String version) {
        return version.matches("^\\d+\\.\\d+\\.\\d+.*$") ? version : version + ".0";
    }
}
