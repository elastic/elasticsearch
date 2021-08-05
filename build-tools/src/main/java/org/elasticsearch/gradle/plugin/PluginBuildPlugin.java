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
import org.elasticsearch.gradle.test.GrantTestPermissionPlugin;
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
import org.gradle.api.file.RegularFile;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.publish.PublishingExtension;
import org.gradle.api.publish.maven.MavenPublication;
import org.gradle.api.publish.maven.plugins.MavenPublishPlugin;
import org.gradle.api.tasks.Copy;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Zip;

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
    @Override
    public void apply(final Project project) {
        project.getPluginManager().apply(JavaPlugin.class);
        project.getPluginManager().apply(TestClustersPlugin.class);
        project.getPluginManager().apply(CompileOnlyResolvePlugin.class);
        project.getPluginManager().apply(JarHellPlugin.class);
        project.getPluginManager().apply(GrantTestPermissionPlugin.class);

        var extension = project.getExtensions().create(PLUGIN_EXTENSION_NAME, PluginPropertiesExtension.class, project);
        configureDependencies(project);

        final var bundleTask = createBundleTasks(project, extension);
        project.afterEvaluate(project1 -> {
            project1.getExtensions().getByType(PluginPropertiesExtension.class).getExtendedPlugins().forEach(pluginName -> {
                // Auto add dependent modules to the test cluster
                if (project1.findProject(":modules:" + pluginName) != null) {
                    NamedDomainObjectContainer<ElasticsearchCluster> testClusters = testClusters(project, "testClusters");
                    testClusters.all(elasticsearchCluster -> elasticsearchCluster.module(":modules:" + pluginName));
                }
            });
            final var extension1 = project1.getExtensions().getByType(PluginPropertiesExtension.class);
            configurePublishing(project1, extension1);
            var name = extension1.getName();
            project1.setProperty("archivesBaseName", name);
            project1.setDescription(extension1.getDescription());

            if (extension1.getName() == null) {
                throw new InvalidUserDataException("name is a required setting for esplugin");
            }

            if (extension1.getDescription() == null) {
                throw new InvalidUserDataException("description is a required setting for esplugin");
            }

            if (extension1.getType().equals(PluginType.BOOTSTRAP) == false && extension1.getClassname() == null) {
                throw new InvalidUserDataException("classname is a required setting for esplugin");
            }

            Map<String, Object> map = new LinkedHashMap<>(12);
            map.put("name", extension1.getName());
            map.put("description", extension1.getDescription());
            map.put("version", extension1.getVersion());
            map.put("elasticsearchVersion", Version.fromString(VersionProperties.getElasticsearch()).toString());
            map.put("javaVersion", project1.getExtensions().getByType(JavaPluginExtension.class).getTargetCompatibility().toString());
            map.put("classname", extension1.getType().equals(PluginType.BOOTSTRAP) ? "" : extension1.getClassname());
            map.put("extendedPlugins", extension1.getExtendedPlugins().stream().collect(Collectors.joining(",")));
            map.put("hasNativeController", extension1.isHasNativeController());
            map.put("requiresKeystore", extension1.isRequiresKeystore());
            map.put("type", extension1.getType().toString());
            map.put("javaOpts", extension1.getJavaOpts());
            map.put("licensed", extension1.isLicensed());
            project1.getTasks().withType(Copy.class).named("pluginProperties").configure(copy -> {
                copy.expand(map);
                copy.getInputs().properties(map);
            });
        });
        project.getConfigurations().getByName("default").extendsFrom(project.getConfigurations().getByName("runtimeClasspath"));

        // allow running ES with this plugin in the foreground of a build
        var testClusters = testClusters(project, TestClustersPlugin.EXTENSION_NAME);
        final var runCluster = testClusters.create("runTask", cluster -> {
            if (GradleUtils.isModuleProject(project.getPath())) {
                cluster.module(bundleTask.flatMap((Transformer<Provider<RegularFile>, Zip>) zip -> zip.getArchiveFile()));
            } else {
                cluster.plugin(bundleTask.flatMap((Transformer<Provider<RegularFile>, Zip>) zip -> zip.getArchiveFile()));
            }
        });

        project.getTasks().register("run", RunTask.class, runTask -> {
            runTask.useCluster(runCluster);
            runTask.dependsOn(project.getTasks().named("bundlePlugin"));
        });
    }

    @SuppressWarnings("unchecked")
    private static NamedDomainObjectContainer<ElasticsearchCluster> testClusters(Project project, String extensionName) {
        return (NamedDomainObjectContainer<ElasticsearchCluster>) project.getExtensions().getByName(extensionName);
    }

    private static void configurePublishing(Project project, PluginPropertiesExtension extension) {
        if (project.getPlugins().hasPlugin(MavenPublishPlugin.class)) {
            PublishingExtension publishingExtension = project.getExtensions().getByType(PublishingExtension.class);
            MavenPublication elastic = publishingExtension.getPublications().maybeCreate("elastic", MavenPublication.class);
            elastic.setArtifactId(extension.getName());
        }
    }

    private static void configureDependencies(final Project project) {
        var dependencies = project.getDependencies();
        dependencies.add("compileOnly", "org.elasticsearch:elasticsearch:" + VersionProperties.getElasticsearch());
        dependencies.add("testImplementation", "org.elasticsearch.test:framework:" + VersionProperties.getElasticsearch());

        // we "upgrade" these optional deps to provided for plugins, since they will run
        // with a full elasticsearch server that includes optional deps
        dependencies.add("compileOnly", "org.locationtech.spatial4j:spatial4j:" + VersionProperties.getVersions().get("spatial4j"));
        dependencies.add("compileOnly", "org.locationtech.jts:jts-core:" + VersionProperties.getVersions().get("jts"));
        dependencies.add("compileOnly", "org.apache.logging.log4j:log4j-api:" + VersionProperties.getVersions().get("log4j"));
        dependencies.add("compileOnly", "org.apache.logging.log4j:log4j-core:" + VersionProperties.getVersions().get("log4j"));
        dependencies.add("compileOnly", "org.elasticsearch:jna:" + VersionProperties.getVersions().get("jna"));
    }

    /**
     * Adds a bundlePlugin task which builds the zip containing the plugin jars,
     * metadata, properties, and packaging files
     */
    private static TaskProvider<Zip> createBundleTasks(final Project project, PluginPropertiesExtension extension) {
        final var pluginMetadata = project.file("src/main/plugin-metadata");
        final var templateFile = new File(project.getBuildDir(), "templates/plugin-descriptor.properties");

        // create tasks to build the properties file for this plugin
        final var copyPluginPropertiesTemplate = project.getTasks().register("copyPluginPropertiesTemplate", new Action<Task>() {
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
        final var buildProperties = project.getTasks().register("pluginProperties", Copy.class, copy -> {
            copy.dependsOn(copyPluginPropertiesTemplate);
            copy.from(templateFile);
            copy.into(new File(project.getBuildDir(), "generated-resources"));
        });
        // add the plugin properties and metadata to test resources, so unit tests can
        // know about the plugin (used by test security code to statically initialize the plugin in unit tests)
        var testSourceSet = project.getExtensions().getByType(SourceSetContainer.class).getByName("test");
        Map<String, Object> map = Map.of("builtBy", buildProperties);
        testSourceSet.getOutput().dir(map, new File(project.getBuildDir(), "generated-resources"));
        testSourceSet.getResources().srcDir(pluginMetadata);

        // create the actual bundle task, which zips up all the files for the plugin
        final var bundle = project.getTasks().register("bundlePlugin", Zip.class, zip -> {
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
        project.getConfigurations().create("zip");
        project.getArtifacts().add("zip", bundle);

        return bundle;
    }

    public static final String PLUGIN_EXTENSION_NAME = "esplugin";
}
