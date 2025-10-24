/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;

import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

public record ChangelogBundle(String version, boolean released, String generated, List<ChangelogEntry> changelogs) {

    private static final Logger LOGGER = Logging.getLogger(GenerateReleaseNotesTask.class);
    private static final ObjectMapper yamlMapper = new ObjectMapper(
        new YAMLFactory().enable(YAMLGenerator.Feature.MINIMIZE_QUOTES).disable(YAMLGenerator.Feature.SPLIT_LINES)
    );

    public ChangelogBundle(String version, String generated, List<ChangelogEntry> changelogs) {
        this(version, false, generated, changelogs);
    }

    public static ChangelogBundle parse(File file) {
        try {
            return yamlMapper.readValue(file, ChangelogBundle.class);
        } catch (IOException e) {
            LOGGER.error("Failed to parse changelog bundle from " + file.getAbsolutePath(), e);
            throw new UncheckedIOException(e);
        }
    }

    public static ChangelogBundle copy(ChangelogBundle bundle) {
        List<ChangelogEntry> changelogs = bundle.changelogs().stream().toList();
        return new ChangelogBundle(bundle.version(), bundle.released(), bundle.generated(), changelogs);
    }

    public ChangelogBundle withChangelogs(List<ChangelogEntry> changelogs) {
        return new ChangelogBundle(version, released, generated, changelogs);
    }
}
