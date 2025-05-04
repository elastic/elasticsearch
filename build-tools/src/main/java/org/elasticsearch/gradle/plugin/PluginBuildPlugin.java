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
import org.elasticsearch.gradle.dependencies.CompileOnlyResolvePlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.file.RegularFile;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.language.jvm.tasks.ProcessResources;

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

        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);

        // TODO: we no longer care about descriptor remove as inputs
        // TODO: we no longer care about policy-yaml file remove as inputs
        var testBuildInfoTask = project.getTasks().register("generateTestBuildInfo", GenerateTestBuildInfoTask.class, task -> {
            var pluginProperties = project.getTasks().withType(GeneratePluginPropertiesTask.class).named("pluginProperties");
            // task.getDescriptorFile().set(pluginProperties.flatMap(GeneratePluginPropertiesTask::getOutputFile));
            var propertiesExtension = project.getExtensions().getByType(PluginPropertiesExtension.class);
            task.getComponentName().set(providerFactory.provider(propertiesExtension::getName));
            var policy = project.getLayout().getProjectDirectory().file("src/main/plugin-metadata/entitlement-policy.yaml");
            if (policy.getAsFile().exists()) {
                // task.getPolicyFile().set(policy);
            }
            // TODO: get first class of each location in deterministic order
            // TODO: filter META_INF and module-info.class out of these

            task.getCodeLocations()
                .set(
                    project.getConfigurations()
                        .getByName("runtimeClasspath")
                        .minus(project.getConfigurations().getByName(CompileOnlyResolvePlugin.RESOLVEABLE_COMPILE_ONLY_CONFIGURATION_NAME))
                        .plus(sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME).getOutput().getClassesDirs())
                );

            Provider<Directory> directory = project.getLayout().getBuildDirectory().dir("generated-build-info/test");
            task.getOutputDirectory().set(directory);
        });

        sourceSets.named(SourceSet.TEST_SOURCE_SET_NAME).configure(sourceSet -> { sourceSet.getResources().srcDir(testBuildInfoTask); });

        project.getTasks().withType(ProcessResources.class).named("processResources").configure(task -> {
            // TODO: this is a copy task
            // TODO: do this for descriptor and policy-yaml file
            // TODO: use child copy spec
            // task.into()
        });
    }

}
