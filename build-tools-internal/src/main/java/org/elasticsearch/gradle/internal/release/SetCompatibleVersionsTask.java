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
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.options.Option;
import org.gradle.initialization.layout.BuildLayout;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;

public class SetCompatibleVersionsTask extends AbstractVersionsTask {

    private Version thisVersion;
    private Version releaseVersion;
    private Map<String, Integer> versionIds = Map.of();

    @Inject
    public SetCompatibleVersionsTask(BuildLayout layout) {
        super(layout);
    }

    public void setThisVersion(Version version) {
        thisVersion = version;
    }

    @Option(option = "version-id", description = "Version id used for the release. Of the form <VersionType>:<id>.")
    public void versionIds(List<String> version) {
        this.versionIds = splitVersionIds(version);
    }

    @Option(option = "release", description = "The version being released")
    public void releaseVersion(String version) {
        releaseVersion = Version.fromString(version);
    }

    @TaskAction
    public void executeTask() throws IOException {
        if (versionIds.isEmpty()) {
            throw new IllegalArgumentException("No version ids specified");
        }

        if (releaseVersion.getMajor() < thisVersion.getMajor()) {
            // don't need to update CCS version - this is for a different major
            return;
        }

        Integer transportVersion = versionIds.get(TRANSPORT_VERSION_TYPE);
        if (transportVersion == null) {
            throw new IllegalArgumentException("TransportVersion id not specified");
        }
    }
}
