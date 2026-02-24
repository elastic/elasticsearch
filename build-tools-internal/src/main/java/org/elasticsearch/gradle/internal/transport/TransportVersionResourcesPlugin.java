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
import org.elasticsearch.gradle.internal.info.BuildParameterExtension;
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.Copy;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import java.util.Map;
import java.util.Properties;

import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;

public class TransportVersionResourcesPlugin implements Plugin<Project> {

    public static final String TRANSPORT_REFERENCES_TOPIC = "transportReferences";

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(LifecycleBasePlugin.class);
        project.getPluginManager().apply(VersionPropertiesPlugin.class);
        project.getPluginManager().apply(PrecommitTaskPlugin.class);
        var psService = project.getPlugins().apply(ProjectSubscribeServicePlugin.class).getService();

        project.getRootProject().getPlugins().apply(GlobalBuildInfoPlugin.class);
        Property<BuildParameterExtension> buildParams = loadBuildParams(project);

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
                t.getCI().set(buildParams.get().getCi());
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

        var generateDefinitionsTask = project.getTasks()
            .register("generateTransportVersion", GenerateTransportVersionDefinitionTask.class, t -> {
                t.setDescription("(Re)generates a transport version definition file");
                t.getReferencesFiles().setFrom(tvReferencesConfig);
            });
        validateTask.configure(t -> t.mustRunAfter(generateDefinitionsTask));

        var resolveConflictTask = project.getTasks()
            .register("resolveTransportVersionConflict", ResolveTransportVersionConflictTask.class, t -> {
                t.setDescription("Resolve merge conflicts in transport version internal state files");
            });
        validateTask.configure(t -> t.mustRunAfter(resolveConflictTask));

        // common generation configuration
        project.getTasks().withType(AbstractGenerateTransportVersionDefinitionTask.class).configureEach(t -> {
            t.setGroup(taskGroup);
            t.getIncrement().convention(1000);
            t.getCurrentUpperBoundName().convention(currentVersion.getMajor() + "." + currentVersion.getMinor());
        });

        var generateInitialTask = project.getTasks()
            .register("generateInitialTransportVersion", GenerateInitialTransportVersionTask.class, t -> {
                t.setGroup(taskGroup);
                t.setDescription("Generates an initial transport version for an Elasticsearch release version");
                t.getCurrentVersion().set(currentVersion);
            });
        validateTask.configure(t -> t.mustRunAfter(generateInitialTask));

        var updateTransportVersionsTask = project.getTasks()
            .register("updateTransportVersionsCSV", UpdateTransportVersionsCSVTask.class, t -> {
                t.setGroup(taskGroup);
                t.setDescription("Updates TransportVersions.csv with a new stack version entry");
                t.getTransportVersionsFile()
                    .set(project.getLayout().getProjectDirectory().file("src/main/resources/org/elasticsearch/TransportVersions.csv"));
            });
        validateTask.configure(t -> t.mustRunAfter(updateTransportVersionsTask));
    }

    private static String getResourceRoot(Project project) {
        Provider<String> resourceRootProperty = project.getProviders().gradleProperty("org.elasticsearch.transport.resourceRoot");
        return resourceRootProperty.isPresent() ? resourceRootProperty.get() : "transport";
    }
}
