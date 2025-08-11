/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.plugin;

import groovy.lang.Closure;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.dependencies.CompileOnlyResolvePlugin;
import org.elasticsearch.gradle.jarhell.JarHellPlugin;
import org.elasticsearch.gradle.test.GradleTestPolicySetupPlugin;
import org.elasticsearch.gradle.testclusters.ElasticsearchCluster;
import org.elasticsearch.gradle.testclusters.RunTask;
import org.elasticsearch.gradle.testclusters.TestClustersPlugin;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.Transformer;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.dsl.DependencyHandler;
import org.gradle.api.artifacts.type.ArtifactTypeDefinition;
import org.gradle.api.file.RegularFile;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.BasePluginExtension;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.publish.PublishingExtension;
import org.gradle.api.publish.maven.MavenPublication;
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin;
import org.gradle.api.publish.maven.tasks.GenerateMavenPom;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Zip;
import org.gradle.jvm.tasks.Jar;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Encapsulates build configuration for an Elasticsearch plugin.
 */
public class PluginBuildPlugin implements Plugin<Project> {

    public static final String BUNDLE_PLUGIN_TASK_NAME = "bundlePlugin";

    @Override
    public void apply(final Project project) {
        project.getPluginManager().apply(JavaPlugin.class);
        project.getPluginManager().apply(TestClustersPlugin.class);
        project.getPluginManager().apply(CompileOnlyResolvePlugin.class);
        project.getPluginManager().apply(JarHellPlugin.class);
        project.getPluginManager().apply(GradleTestPolicySetupPlugin.class);

        PluginPropertiesExtension extension = project.getExtensions()
            .create(PLUGIN_EXTENSION_NAME, PluginPropertiesExtension.class, project);
        configureDependencies(project);

        final TaskProvider<Zip> bundleTask = createBundleTasks(project, extension);
        project.afterEvaluate(project1 -> {
            final PluginPropertiesExtension pluginPropertiesExtension = project1.getExtensions().getByType(PluginPropertiesExtension.class);
            configurePublishing(project1, pluginPropertiesExtension);
            String name = pluginPropertiesExtension.getName();

            final BasePluginExtension basepluginExtension = project1.getExtensions().getByType(BasePluginExtension.class);
            basepluginExtension.getArchivesName().set(name);
            project1.setDescription(pluginPropertiesExtension.getDescription());

            if (pluginPropertiesExtension.getName() == null) {
                throw new InvalidUserDataException("name is a required setting for esplugin");
            }

            if (pluginPropertiesExtension.getDescription() == null) {
                throw new InvalidUserDataException("description is a required setting for esplugin");
            }

            if (pluginPropertiesExtension.getType().equals(PluginType.BOOTSTRAP) == false
                && pluginPropertiesExtension.getClassname() == null) {
                throw new InvalidUserDataException("classname is a required setting for esplugin");
            }

            Map<String, Object> map = new LinkedHashMap<>(12);
            map.put("name", pluginPropertiesExtension.getName());
            map.put("description", pluginPropertiesExtension.getDescription());
            map.put("version", pluginPropertiesExtension.getVersion());
            map.put("elasticsearchVersion", Version.fromString(VersionProperties.getElasticsearch()).toString());
            map.put("javaVersion", project1.getExtensions().getByType(JavaPluginExtension.class).getTargetCompatibility().toString());
            map.put(
                "classname",
                pluginPropertiesExtension.getType().equals(PluginType.BOOTSTRAP) ? "" : pluginPropertiesExtension.getClassname()
            );
            map.put("extendedPlugins", pluginPropertiesExtension.getExtendedPlugins().stream().collect(Collectors.joining(",")));
            map.put("hasNativeController", pluginPropertiesExtension.isHasNativeController());
            map.put("requiresKeystore", pluginPropertiesExtension.isRequiresKeystore());
            map.put("type", pluginPropertiesExtension.getType().toString());
            map.put("javaOpts", pluginPropertiesExtension.getJavaOpts());
            map.put("licensed", pluginPropertiesExtension.isLicensed());
            project1.getTasks().withType(Copy.class).named("pluginProperties").configure(copy -> {
                copy.expand(map);
                copy.getInputs().properties(map);
            });
        });
        project.getConfigurations().getByName("default").extendsFrom(project.getConfigurations().getByName("runtimeClasspath"));

        // allow running ES with this plugin in the foreground of a build
        var testClusters = testClusters(project, TestClustersPlugin.EXTENSION_NAME);
        var runCluster = testClusters.register("runTask", c -> {
            if (GradleUtils.isModuleProject(project.getPath())) {
                c.module(bundleTask.flatMap((Transformer<Provider<RegularFile>, Zip>) zip -> zip.getArchiveFile()));
            } else {
                c.plugin(bundleTask.flatMap((Transformer<Provider<RegularFile>, Zip>) zip -> zip.getArchiveFile()));
            }
        });

        project.getTasks().register("run", RunTask.class, r -> {
            r.useCluster(runCluster.get());
            r.dependsOn(project.getTasks().named(BUNDLE_PLUGIN_TASK_NAME));
        });
    }

    @SuppressWarnings("unchecked")
    private static NamedDomainObjectContainer<ElasticsearchCluster> testClusters(Project project, String extensionName) {
        return (NamedDomainObjectContainer<ElasticsearchCluster>) project.getExtensions().getByName(extensionName);
    }

    private static void configurePublishing(Project project, PluginPropertiesExtension extension) {
        project.getPlugins().withType(MavenPublishPlugin.class).configureEach(plugin -> {
            project.afterEvaluate(project1 -> {
                PublishingExtension publishingExtension = project.getExtensions().getByType(PublishingExtension.class);
                MavenPublication elastic = publishingExtension.getPublications().maybeCreate("elastic", MavenPublication.class);
                if (extension.isHasClientJar()) {
                    project.getTasks()
                        .withType(Jar.class)
                        .configureEach(jar -> jar.getArchiveBaseName().set(jar.getArchiveBaseName().get() + "-client"));
                    project.getTasks().withType(GenerateMavenPom.class).configureEach(pomTask -> {
                        final BasePluginExtension basepluginExtension = project.getExtensions().getByType(BasePluginExtension.class);
                        String archivesName = basepluginExtension.getArchivesName().get();
                        pomTask.setDestination(
                            new File(project.getBuildDir(), "/distributions/" + archivesName + "-client-" + project.getVersion() + ".pom")
                        );
                    });
                    publishingExtension.getPublications().withType(MavenPublication.class).configureEach(publication -> {
                        publication.setArtifactId(extension.getName() + "-client");
                    });
                    publishingExtension.repositories(repositories -> {});
                }
            });
        });
    }

    private static void configureDependencies(final Project project) {
        DependencyHandler dependencies = project.getDependencies();
        dependencies.add("compileOnly", "org.elasticsearch:elasticsearch:" + VersionProperties.getElasticsearch());
        dependencies.add("testImplementation", "org.elasticsearch.test:framework:" + VersionProperties.getElasticsearch());
        dependencies.add("testImplementation", "org.apache.logging.log4j:log4j-core:" + VersionProperties.getVersions().get("log4j"));

        // we "upgrade" these optional deps to provided for plugins, since they will run
        // with a full elasticsearch server that includes optional deps
        dependencies.add("compileOnly", "org.locationtech.spatial4j:spatial4j:" + VersionProperties.getVersions().get("spatial4j"));
        dependencies.add("compileOnly", "org.locationtech.jts:jts-core:" + VersionProperties.getVersions().get("jts"));
        dependencies.add("compileOnly", "org.apache.logging.log4j:log4j-api:" + VersionProperties.getVersions().get("log4j"));
        dependencies.add("compileOnly", "org.apache.logging.log4j:log4j-core:" + VersionProperties.getVersions().get("log4j"));
        dependencies.add("compileOnly", "net.java.dev.jna:jna:" + VersionProperties.getVersions().get("jna"));
    }

    /**
     * Adds a bundlePlugin task which builds the zip containing the plugin jars,
     * metadata, properties, and packaging files
     */
    private static TaskProvider<Zip> createBundleTasks(final Project project, PluginPropertiesExtension extension) {
        final File pluginMetadata = project.file("src/main/plugin-metadata");
        final File templateFile = new File(project.getBuildDir(), "templates/plugin-descriptor.properties");

        // create tasks to build the properties file for this plugin
        final TaskProvider<Task> copyPluginPropertiesTemplate = project.getTasks()
            .register("copyPluginPropertiesTemplate", new Action<Task>() {
                @Override
                public void execute(Task task) {
                    task.getOutputs().file(templateFile);
                    // intentionally an Action and not a lambda to avoid issues with up-to-date check
                    task.doLast(new Action<Task>() {
                        @Override
                        public void execute(Task task) {
                            InputStream resourceTemplate = PluginBuildPlugin.class.getResourceAsStream("/" + templateFile.getName());
                            try {
                                String templateText = IOUtils.toString(resourceTemplate, StandardCharsets.UTF_8.name());
                                FileUtils.write(templateFile, templateText, "UTF-8");
                            } catch (IOException e) {
                                throw new GradleException("Unable to copy plugin properties", e);
                            }
                        }
                    });
                }
            });
        final TaskProvider<Copy> buildProperties = project.getTasks().register("pluginProperties", Copy.class, copy -> {
            copy.dependsOn(copyPluginPropertiesTemplate);
            copy.from(templateFile);
            copy.into(new File(project.getBuildDir(), "generated-resources"));
        });
        // add the plugin properties and metadata to test resources, so unit tests can
        // know about the plugin (used by test security code to statically initialize the plugin in unit tests)
        SourceSet testSourceSet = project.getExtensions().getByType(SourceSetContainer.class).getByName("test");
        Map<String, Object> map = Map.of("builtBy", buildProperties);
        testSourceSet.getOutput().dir(map, new File(project.getBuildDir(), "generated-resources"));
        testSourceSet.getResources().srcDir(pluginMetadata);

        // create the actual bundle task, which zips up all the files for the plugin
        final TaskProvider<Zip> bundle = project.getTasks().register("bundlePlugin", Zip.class, zip -> {
            zip.from(buildProperties);
            zip.from(pluginMetadata, copySpec -> {
                // metadata (eg custom security policy)
                // the codebases properties file is only for tests and not needed in production
                copySpec.exclude("plugin-security.codebases");
            });

            /*
             * If the plugin is using the shadow plugin then we need to bundle
             * that shadow jar.
             */
            zip.from(new Closure<Object>(null, null) {
                public Object doCall(Object it) {
                    return project.getPluginManager().hasPlugin("com.github.johnrengelman.shadow")
                        ? project.getTasks().named("shadowJar")
                        : project.getTasks().named("jar");
                }

                public Object doCall() {
                    return doCall(null);
                }

            });
            zip.from(
                project.getConfigurations()
                    .getByName("runtimeClasspath")
                    .minus(project.getConfigurations().getByName(CompileOnlyResolvePlugin.RESOLVEABLE_COMPILE_ONLY_CONFIGURATION_NAME))
            );
            // extra files for the plugin to go into the zip
            zip.from("src/main/packaging");// TODO: move all config/bin/_size/etc into packaging
            zip.from("src/main", copySpec -> {
                copySpec.include("config/**");
                copySpec.include("bin/**");
            });
        });
        project.getTasks().named(BasePlugin.ASSEMBLE_TASK_NAME).configure(task -> task.dependsOn(bundle));

        // also make the zip available as a configuration (used when depending on this project)
        Configuration configuration = project.getConfigurations().create("zip");
        configuration.getAttributes().attribute(ArtifactTypeDefinition.ARTIFACT_TYPE_ATTRIBUTE, ArtifactTypeDefinition.ZIP_TYPE);
        project.getArtifacts().add("zip", bundle);

        return bundle;
    }

    public static final String PLUGIN_EXTENSION_NAME = "esplugin";
}
