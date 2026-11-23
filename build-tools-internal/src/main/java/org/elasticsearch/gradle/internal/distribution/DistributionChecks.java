/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.distribution;

import org.gradle.api.file.ProjectLayout;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;

import java.util.List;

/**
 * The expectations shared by the sanity checks of the archive and the package distributions.
 *
 * <p>The expected content is deliberately written out here rather than derived from the files the distributions are
 * built from, so that a broken distribution build cannot make a check pass by changing both sides.
 */
public final class DistributionChecks {

    /**
     * The lines every distribution's {@code NOTICE.txt} starts with.
     */
    public static final List<String> EXPECTED_NOTICE_LINES = List.of("Elasticsearch", "Copyright 2009-2024 Elasticsearch");

    /**
     * The license file of the default distribution, relative to the {@code licenses} directory of the repository.
     */
    public static final String ELASTIC_LICENSE_FILE = "ELASTIC-LICENSE-2.0.txt";

    private DistributionChecks() {}

    /**
     * Lazily reads the lines of a license file in the {@code licenses} directory of the repository, e.g.
     * {@link #ELASTIC_LICENSE_FILE}. The file is read when the checking task runs, not at configuration time.
     */
    public static Provider<List<String>> licenseLines(ProviderFactory providers, ProjectLayout layout, String licenseFileName) {
        return providers.fileContents(layout.getSettingsDirectory().file("licenses/" + licenseFileName))
            .getAsText()
            .map(text -> text.lines().toList());
    }
}
