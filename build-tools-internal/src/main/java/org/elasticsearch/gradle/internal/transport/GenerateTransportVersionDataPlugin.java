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

public class GenerateTransportVersionDataPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getTasks().register("generateTransportVersionData", GenerateTransportVersionDataTask.class, t -> {
            t.setDescription("Generate transport version data"); // todo update this to be more descriptive
            t.setGroup("Transport Versions"); // todo
            t.getDataFileDirectory().set(project.getLayout().getProjectDirectory().file("src/main/resources/org/elasticsearch/transport/"));
            t.getTVSetName().set("test"); // todo
            t.getReleaseVersionForTV().set("9.1"); // todo
        });
    }
}
