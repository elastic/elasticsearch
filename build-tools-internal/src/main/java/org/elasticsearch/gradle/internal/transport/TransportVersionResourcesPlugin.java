/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.transport;

import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.internal.ProjectSubscribeServicePlugin;
import org.elasticsearch.gradle.internal.conventions.VersionPropertiesPlugin;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitPlugin;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitTaskPlugin;
import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Copy;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import java.util.Map;
import java.util.Properties;

public class TransportVersionResourcesPlugin implements Plugin<Project> {

    public static final String TRANSPORT_REFERENCES_TOPIC = "transportReferences";

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(LifecycleBasePlugin.class);
        project.getPluginManager().apply(VersionPropertiesPlugin.class);
        project.getPluginManager().apply(PrecommitTaskPlugin.class);
        var psService = project.getPlugins().apply(ProjectSubscribeServicePlugin.class).getService();

        Properties versions = (Properties) project.getExtensions().getByName(VersionPropertiesPlugin.VERSIONS_EXT);
        Version currentVersion = Version.fromString(versions.getProperty("elasticsearch"));

        var resourceRoot = getResourceRoot(project);

        String taskGroup = "Transport Versions";

        project.getGradle()
            .getSharedServices()
            .registerIfAbsent("transportVersionResources", TransportVersionResourcesService.class, spec -> {
                Directory transportResources = project.getLayout().getProjectDirectory().dir("src/main/resources/" + resourceRoot);
                spec.getParameters().getTransportResourcesDirectory().set(transportResources);
                spec.getParameters().getRootDirectory().set(project.getLayout().getSettingsDirectory().getAsFile());
                Provider<String> upstreamRef = project.getProviders().gradleProperty("org.elasticsearch.transport.baseRef");
                if (upstreamRef.isPresent()) {
                    spec.getParameters().getBaseRefOverride().set(upstreamRef.get());
                }
            });

        var depsHandler = project.getDependencies();
        var tvReferencesConfig = project.getConfigurations().create("globalTvReferences", c -> {
            c.setCanBeConsumed(false);
            c.setCanBeResolved(true);
            c.attributes(TransportVersionReference::addArtifactAttribute);
            c.getDependencies()
                .addAllLater(
                    psService.flatMap(t -> t.getProjectsByTopic(TRANSPORT_REFERENCES_TOPIC))
                        .map(projectPaths -> projectPaths.stream().map(path -> depsHandler.project(Map.of("path", path))).toList())
                );
        });

        var validateTask = project.getTasks()
            .register("validateTransportVersionResources", ValidateTransportVersionResourcesTask.class, t -> {
                t.setGroup(taskGroup);
                t.setDescription("Validates that all transport version resources are internally consistent with each other");
                t.getReferencesFiles().setFrom(tvReferencesConfig);
                t.getShouldValidateDensity().convention(true);
                t.getShouldValidatePrimaryIdNotPatch().convention(true);
                t.getCurrentUpperBoundName().convention(currentVersion.getMajor() + "." + currentVersion.getMinor());
            });
        project.getTasks().named(PrecommitPlugin.PRECOMMIT_TASK_NAME).configure(t -> t.dependsOn(validateTask));

        var generateManifestTask = project.getTasks()
            .register("generateTransportVersionManifest", GenerateTransportVersionManifestTask.class, t -> {
                t.setGroup(taskGroup);
                t.setDescription("Generate a manifest resource for all transport version definitions");
                t.getManifestFile().set(project.getLayout().getBuildDirectory().file("generated-resources/manifest.txt"));
            });
        project.getTasks().named(JavaPlugin.PROCESS_RESOURCES_TASK_NAME, Copy.class).configure(t -> {
            t.into(resourceRoot + "/definitions", c -> c.from(generateManifestTask));
        });

        Action<GenerateTransportVersionDefinitionTask> generationConfiguration = t -> {
            t.setGroup(taskGroup);
            t.getReferencesFiles().setFrom(tvReferencesConfig);
            t.getIncrement().convention(1000);
            t.getCurrentUpperBoundName().convention(currentVersion.getMajor() + "." + currentVersion.getMinor());
        };

        var generateDefinitionsTask = project.getTasks()
            .register("generateTransportVersion", GenerateTransportVersionDefinitionTask.class, t -> {
                t.setDescription("(Re)generates a transport version definition file");
            });
        generateDefinitionsTask.configure(generationConfiguration);
        validateTask.configure(t -> t.mustRunAfter(generateDefinitionsTask));

        var resolveConflictTask = project.getTasks()
            .register("resolveTransportVersionConflict", GenerateTransportVersionDefinitionTask.class, t -> {
                t.setDescription("Resolve merge conflicts in transport version internal state files");
                t.getResolveConflict().set(true);
            });
        resolveConflictTask.configure(generationConfiguration);
        validateTask.configure(t -> t.mustRunAfter(resolveConflictTask));

        var generateInitialTask = project.getTasks()
            .register("generateInitialTransportVersion", GenerateInitialTransportVersionTask.class, t -> {
                t.setGroup(taskGroup);
                t.setDescription("(Re)generates an initial transport version for an Elasticsearch release version");
                t.getCurrentVersion().set(currentVersion);
            });
        validateTask.configure(t -> t.mustRunAfter(generateInitialTask));
    }

    private static String getResourceRoot(Project project) {
        Provider<String> resourceRootProperty = project.getProviders().gradleProperty("org.elasticsearch.transport.resourceRoot");
        return resourceRootProperty.isPresent() ? resourceRootProperty.get() : "transport";
    }
}
