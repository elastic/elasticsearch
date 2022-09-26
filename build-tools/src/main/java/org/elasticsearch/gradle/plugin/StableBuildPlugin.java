/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.plugin;

import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.RegularFile;
import org.gradle.api.provider.Provider;

public class StableBuildPlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        var extension = project.getExtensions()
            .create(BasePluginBuildPlugin.PLUGIN_EXTENSION_NAME, BasePluginPropertiesExtension.class, project);

        project.getPluginManager().apply(BasePluginBuildPlugin.class);

        project.getTasks().withType(GeneratePluginPropertiesTask.class).named("pluginProperties").configure(task -> {
            task.getIsStable().set(true);
            task.getTemplateFileName().set("stable-" + GeneratePluginPropertiesTask.PROPERTIES_FILENAME);

            Provider<RegularFile> file = project.getLayout()
                .getBuildDirectory()
                .file("generated-descriptor/" + GeneratePluginPropertiesTask.STABLE_PROPERTIES_FILENAME);
            task.getOutputFile().set(file);
        });
    }
}
