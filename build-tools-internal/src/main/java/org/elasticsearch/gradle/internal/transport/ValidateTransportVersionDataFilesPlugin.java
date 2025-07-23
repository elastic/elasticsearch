/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.gradle.api.Plugin;
import org.gradle.api.Project;

public class ValidateTransportVersionDataFilesPlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        final var task = project.getTasks().register("validateTransportVersionDataFiles", ValidateTransportVersionsTask.class, t -> {
            t.getTransportVersionSetNamesFile()
                .set(
                    project.getLayout()
                        .getBuildDirectory()
                        .file(AggregateTransportVersionDeclarationsPlugin.ALL_TRANSPORT_VERSION_NAMES_FILE)
                );
            t.getDataFileDirectory().set(project.getLayout().getProjectDirectory().file("src/main/resources/org/elasticsearch/transport/"));
        });
    }
}
