/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.plugin;

import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.dependencies.CompileOnlyResolvePlugin;
import org.elasticsearch.gradle.jarhell.JarHellPlugin;
import org.elasticsearch.gradle.test.GradleTestPolicySetupPlugin;
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.elasticsearch.gradle.testclusters.RunTask;
import org.elasticsearch.gradle.testclusters.TestClustersPlugin;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.Transformer;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.file.CopySpec;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.RegularFile;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.Sync;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Zip;

import java.io.File;
import java.util.Map;
import java.util.concurrent.Callable;

import javax.inject.Inject;

/**
 * Common logic for building ES plugins.
 * Requires plugin extension to be created before applying
 */
public class BasePluginBuildPlugin implements Plugin<Project> {

    public static final String PLUGIN_EXTENSION_NAME = "esplugin";
    public static final String BUNDLE_PLUGIN_TASK_NAME = "bundlePlugin";
    public static final String EXPLODED_BUNDLE_PLUGIN_TASK_NAME = "explodedBundlePlugin";
    public static final String EXPLODED_BUNDLE_CONFIG = "explodedBundleZip";

    protected final ProviderFactory providerFactory;

    @Inject
    public BasePluginBuildPlugin(ProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    @Override
    public void apply(final Project project) {
        project.getPluginManager().apply(BasePlugin.class);
        project.getPluginManager().apply(JavaPlugin.class);
        project.getPluginManager().apply(TestClustersPlugin.class);
        project.getPluginManager().apply(CompileOnlyResolvePlugin.class);
        project.getPluginManager().apply(JarHellPlugin.class);
        project.getPluginManager().apply(GradleTestPolicySetupPlugin.class);

        var extension = project.getExtensions()
            .create(BasePluginBuildPlugin.PLUGIN_EXTENSION_NAME, PluginPropertiesExtension.class, project);

        final var bundleTask = createBundleTasks(project, extension);
        project.getConfigurations().getByName("default").extendsFrom(project.getConfigurations().getByName("runtimeClasspath"));

        // allow running ES with this plugin in the foreground of a build
        var testClusters = testClusters(project, TestClustersPlugin.EXTENSION_NAME);
        var runCluster = testClusters.register("runTask", c -> {
            // TODO: use explodedPlugin here for modules
            if (GradleUtils.isModuleProject(project.getPath())) {
                c.module(bundleTask.flatMap((Transformer<Provider<RegularFile>, Zip>) zip -> zip.getArchiveFile()));
            } else {
                c.plugin(bundleTask.flatMap((Transformer<Provider<RegularFile>, Zip>) zip -> zip.getArchiveFile()));
            }
        });

        project.getTasks().register("run", RunTask.class, r -> {
            r.useCluster(runCluster);
            r.dependsOn(project.getTasks().named(BUNDLE_PLUGIN_TASK_NAME));
        });
    }

    @SuppressWarnings("unchecked")
    private static NamedDomainObjectContainer<ElasticsearchCluster> testClusters(Project project, String extensionName) {
        return (NamedDomainObjectContainer<ElasticsearchCluster>) project.getExtensions().getByName(extensionName);
    }

    /**
     * Adds bundle tasks which builds the dir and zip containing the plugin jars,
     * metadata, properties, and packaging files
     */
    private TaskProvider<Zip> createBundleTasks(final Project project, PluginPropertiesExtension extension) {
        final var pluginMetadata = project.file("src/main/plugin-metadata");

        final var buildProperties = project.getTasks().register("pluginProperties", GeneratePluginPropertiesTask.class, task -> {
            task.getPluginName().set(providerFactory.provider(extension::getName));
            task.getPluginDescription().set(providerFactory.provider(extension::getDescription));
            task.getPluginVersion().set(providerFactory.provider(extension::getVersion));
            task.getElasticsearchVersion().set(Version.fromString(VersionProperties.getElasticsearch()).toString());
            var javaExtension = project.getExtensions().getByType(JavaPluginExtension.class);
            task.getJavaVersion().set(providerFactory.provider(() -> javaExtension.getTargetCompatibility().toString()));
            task.getExtendedPlugins().set(providerFactory.provider(extension::getExtendedPlugins));
            task.getHasNativeController().set(providerFactory.provider(extension::isHasNativeController));
            task.getRequiresKeystore().set(providerFactory.provider(extension::isRequiresKeystore));
            task.getIsLicensed().set(providerFactory.provider(extension::isLicensed));

            var mainSourceSet = project.getExtensions().getByType(SourceSetContainer.class).getByName(SourceSet.MAIN_SOURCE_SET_NAME);
            FileCollection moduleInfoFile = mainSourceSet.getOutput().getAsFileTree().matching(p -> p.include("module-info.class"));
            task.getModuleInfoFile().setFrom(moduleInfoFile);

        });
        // add the plugin properties and metadata to test resources, so unit tests can
        // know about the plugin (used by test security code to statically initialize the plugin in unit tests)
        var testSourceSet = project.getExtensions().getByType(SourceSetContainer.class).getByName("test");
        Map<String, Object> map = Map.of("builtBy", buildProperties);
        testSourceSet.getOutput().dir(map, new File(project.getBuildDir(), "generated-resources"));
        testSourceSet.getResources().srcDir(pluginMetadata);

        var bundleSpec = createBundleSpec(project, pluginMetadata, buildProperties);
        extension.setBundleSpec(bundleSpec);
        // create the actual bundle task, which zips up all the files for the plugin
        final var bundle = project.getTasks().register("bundlePlugin", Zip.class, zip -> zip.with(bundleSpec));
        project.getTasks().named(BasePlugin.ASSEMBLE_TASK_NAME).configure(task -> task.dependsOn(bundle));

        // also make the zip available as a configuration (used when depending on this project)
        var configuration = project.getConfigurations().create("zip");
        configuration.getAttributes().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.ZIP_TYPE);
        project.getArtifacts().add("zip", bundle);

        var explodedBundle = project.getTasks().register(EXPLODED_BUNDLE_PLUGIN_TASK_NAME, Sync.class, sync -> {
            sync.with(bundleSpec);
            sync.into(new File(project.getBuildDir(), "explodedBundle/" + extension.getName()));
        });

        // also make the exploded bundle available as a configuration (used when depending on this project)
        var explodedBundleZip = project.getConfigurations().create(EXPLODED_BUNDLE_CONFIG);
        explodedBundleZip.setCanBeResolved(false);
        explodedBundleZip.setCanBeConsumed(true);
        explodedBundleZip.getAttributes().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.DIRECTORY_TYPE);
        project.getArtifacts().add(EXPLODED_BUNDLE_CONFIG, explodedBundle);
        return bundle;
    }

    private static CopySpec createBundleSpec(
        Project project,
        File pluginMetadata,
        TaskProvider<GeneratePluginPropertiesTask> buildProperties
    ) {
        var bundleSpec = project.copySpec();
        bundleSpec.from(buildProperties);
        bundleSpec.from(pluginMetadata, copySpec -> {
            // metadata (eg custom security policy)
            // the codebases properties file is only for tests and not needed in production
            copySpec.exclude("plugin-security.codebases");
        });
        bundleSpec.from(
            (Callable<TaskProvider<Task>>) () -> project.getPluginManager().hasPlugin("com.github.johnrengelman.shadow")
                ? project.getTasks().named("shadowJar")
                : project.getTasks().named("jar")
        );
        bundleSpec.from(
            project.getConfigurations()
                .getByName("runtimeClasspath")
                .minus(project.getConfigurations().getByName(CompileOnlyResolvePlugin.RESOLVEABLE_COMPILE_ONLY_CONFIGURATION_NAME))
        );

        // extra files for the plugin to go into the zip
        bundleSpec.from("src/main/packaging");// TODO: move all config/bin/_size/etc into packaging
        bundleSpec.from("src/main", copySpec -> {
            copySpec.include("config/**");
            copySpec.include("bin/**");
        });
        return bundleSpec;
    }
}
