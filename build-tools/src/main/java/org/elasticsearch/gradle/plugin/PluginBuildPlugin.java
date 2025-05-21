/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.plugin;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.test.TestBuildInfoPlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.RegularFile;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.jvm.tasks.Jar;
import org.gradle.language.jvm.tasks.ProcessResources;

import java.util.concurrent.Callable;

import javax.inject.Inject;

/**
 * Encapsulates build configuration for an Elasticsearch plugin.
 */
public class PluginBuildPlugin implements Plugin<Project> {

    protected final ProviderFactory providerFactory;

    @Inject
    public PluginBuildPlugin(ProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    @Override
    public void apply(final Project project) {
        project.getPluginManager().apply(BasePluginBuildPlugin.class);
        project.getPluginManager().apply(TestBuildInfoPlugin.class);

        var dependencies = project.getDependencies();
        dependencies.add("compileOnly", "org.elasticsearch:elasticsearch:" + VersionProperties.getElasticsearch());
        dependencies.add("testImplementation", "org.elasticsearch.test:framework:" + VersionProperties.getElasticsearch());

        var extension = project.getExtensions().getByType(PluginPropertiesExtension.class);

        project.getTasks().withType(GeneratePluginPropertiesTask.class).named("pluginProperties").configure(task -> {
            task.getIsStable().set(false);

            task.getClassname().set(providerFactory.provider(extension::getClassname));

            Provider<RegularFile> file = project.getLayout()
                .getBuildDirectory()
                .file("generated-descriptor/" + GeneratePluginPropertiesTask.PROPERTIES_FILENAME);
            task.getOutputFile().set(file);
        });

        project.getTasks().withType(GenerateTestBuildInfoTask.class).named("generateTestBuildInfo").configure(task -> {
            var jarTask = project.getTasks().withType(Jar.class).named("jar").get();
            String moduleName = (String) jarTask.getManifest().getAttributes().get("Automatic-Module-Name");
            if (moduleName == null) {
                moduleName = jarTask.getArchiveBaseName().getOrNull();
            }
            if (moduleName != null) {
                task.getModuleName().set(moduleName);
            }
            var propertiesExtension = project.getExtensions().getByType(PluginPropertiesExtension.class);
            task.getComponentName().set(providerFactory.provider(propertiesExtension::getName));
            task.getOutputFile().set(project.getLayout().getBuildDirectory().file("generated-build-info/plugin-test-build-info.json"));
        });

        project.getTasks().withType(ProcessResources.class).named("processResources").configure(task -> {
            task.into(
                (Callable<String>) () -> "META-INF/es-plugins/"
                    + project.getExtensions().getByType(PluginPropertiesExtension.class).getName()
                    + "/",
                copy -> {
                    copy.from(project.getTasks().withType(GeneratePluginPropertiesTask.class).named("pluginProperties"));
                    copy.from(project.getLayout().getProjectDirectory().file("src/main/plugin-metadata/entitlement-policy.yaml"));
                }
            );
        });
    }
}
