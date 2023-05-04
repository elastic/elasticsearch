/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.tasks.JavaExec;

public class NewTransportVersionPlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        project.getTasks().register("newTransportVersion", JavaExec.class).configure(task -> {
            task.getMainClass().set("org.elasticsearch.gradle.internal.BumpTransportVersion");
        });
    }

}
