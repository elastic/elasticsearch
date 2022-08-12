/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.release;

import com.google.common.annotations.VisibleForTesting;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Comparator.reverseOrder;

/**
 * This class ensures that the release notes index page has the appropriate anchors and include directives
 * for the current repository version.
 */
public class ReleaseNotesIndexGenerator {

    static void update(Set<QualifiedVersion> versions, File indexTemplate, File indexFile) throws IOException {
        try (FileWriter indexFileWriter = new FileWriter(indexFile)) {
            indexFileWriter.write(generateFile(versions, Files.readString(indexTemplate.toPath())));
        }
    }

    @VisibleForTesting
    static String generateFile(Set<QualifiedVersion> versionsSet, String template) throws IOException {
        final Set<QualifiedVersion> versions = new TreeSet<>(reverseOrder());

        // For the purpose of generating the index, snapshot versions are the same as released versions. Prerelease versions are not.
        versionsSet.stream().map(v -> v.isSnapshot() ? v.withoutQualifier() : v).forEach(versions::add);

        final List<String> includeVersions = versions.stream().map(QualifiedVersion::toString).collect(Collectors.toList());

        final Map<String, Object> bindings = new HashMap<>();
        bindings.put("versions", versions);
        bindings.put("includeVersions", includeVersions);

        return TemplateUtils.render(template, bindings);
    }
}
