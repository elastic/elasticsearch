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

import static org.elasticsearch.gradle.internal.transport.LocateTransportVersionsPlugin.TRANSPORT_VERSION_NAMES_FILE;

public class ValidateTransportVersionsPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getTasks().register("validateTransportVersions", ValidateTransportVersionsTask.class, t -> {
            var dir = project.getLayout().getProjectDirectory().file("src/main/resources/org/elasticsearch/transport/");
            t.getDataFileDirectory().set(dir);
            t.getTransportVersionSetNamesFile().set(project.getLayout().getBuildDirectory().file(TRANSPORT_VERSION_NAMES_FILE));
            // TODO is this correct? Needs to have both global/per-plugin versions and dependencies
            t.dependsOn(project.getTasks().withType(LocateTransportVersionsTask.class));
            t.setGroup("Transport Versions"); // TODO
            t.setDescription("Validates that the transport versions used in the project are correct and up to date."); // TODO
        });
    }
}
