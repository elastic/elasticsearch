/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import nebula.plugin.info.InfoBrokerPlugin;

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.elasticsearch.gradle.internal.info.BuildParams;
import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ConfigurationContainer;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.JavaLibraryPlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.api.tasks.javadoc.Javadoc;
import org.gradle.external.javadoc.CoreJavadocOptions;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import java.io.File;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.gradle.internal.conventions.util.Util.toStringable;

/**
 * A wrapper around Gradle's Java plugin that applies our
 * common configuration for production code.
 */
public class ElasticsearchJavaPlugin implements Plugin<Project> {

    public static final String MODULE_API_CONFIGURATION_NAME = "moduleApi";
    public static final String MODULE_IMPLEMENTATION_CONFIGURATION_NAME = "moduleImplementation";
    public static final String MODULE_RUNTIME_ONLY_CONFIGURATION_NAME = "moduleRuntimeOnly";
    public static final String MODULE_COMPILE_ONLY_CONFIGURATION_NAME = "moduleCompileOnly";
    public static final String MODULE_COMPILE_PATH_CONFIGURATION_NAME = "moduleCompilePath";

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(ElasticsearchJavaBasePlugin.class);
        project.getPluginManager().apply(JavaLibraryPlugin.class);

        // configureConfigurations(project);
        configureModuleConfigurations(project);
        configureJars(project);
        configureJarManifest(project);
        configureJavadoc(project);
        testCompileOnlyDeps(project);
    }

    private static void configureModuleConfigurations(Project project) {
        // disable Gradle's modular support
        project.getTasks()
            .withType(JavaCompile.class)
            .configureEach(compileTask -> compileTask.getModularity().getInferModulePath().set(false));

        defineModuleConfigurations(project);
    }

    private static void defineModuleConfigurations(Project project) {
        ConfigurationContainer configurations = project.getConfigurations();

        Configuration moduleApiConfiguration = configurations.create(MODULE_API_CONFIGURATION_NAME);
        moduleApiConfiguration.setDescription("Module API dependencies");
        configurations.getByName(JavaPlugin.API_CONFIGURATION_NAME).extendsFrom(moduleApiConfiguration);

        Configuration moduleImplementationConfiguration = configurations.create(MODULE_IMPLEMENTATION_CONFIGURATION_NAME);
        moduleImplementationConfiguration.setDescription("Module implementation dependencies");
        configurations.getByName(JavaPlugin.IMPLEMENTATION_CONFIGURATION_NAME).extendsFrom(moduleImplementationConfiguration);

        Configuration moduleRuntimeOnlyConfiguration = configurations.create(MODULE_RUNTIME_ONLY_CONFIGURATION_NAME);
        moduleRuntimeOnlyConfiguration.setDescription("Module runtime only dependencies");
        configurations.getByName(JavaPlugin.RUNTIME_ONLY_CONFIGURATION_NAME).extendsFrom(moduleRuntimeOnlyConfiguration);

        Configuration moduleCompileOnlyConfiguration = configurations.create(MODULE_COMPILE_ONLY_CONFIGURATION_NAME);
        moduleCompileOnlyConfiguration.setDescription("Module compile only dependencies");
        configurations.getByName(JavaPlugin.COMPILE_ONLY_CONFIGURATION_NAME).extendsFrom(moduleCompileOnlyConfiguration); // << HACKING here

        Configuration moduleCompilePathConfiguration = configurations.create(MODULE_COMPILE_PATH_CONFIGURATION_NAME);
        moduleCompilePathConfiguration.setDescription("Module compile path dependencies");
        moduleCompilePathConfiguration.extendsFrom(
            moduleApiConfiguration,
            moduleImplementationConfiguration,
            moduleCompileOnlyConfiguration
        );

        // All these configurations are for resolution only and have a preference to see 'classes'
        // folder instead of JARs for inter-project references.
        List.of(
            moduleApiConfiguration,
            moduleImplementationConfiguration,
            moduleRuntimeOnlyConfiguration,
            moduleCompileOnlyConfiguration,
            moduleCompilePathConfiguration
        ).forEach(conf -> {
            conf.setCanBeConsumed(false);
            conf.setCanBeResolved(true);
            // conf.attributes(attr ->
            // attr(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE, objects.named(LibraryElements, LibraryElements.CLASSES)))
        });
    }

    private static void testCompileOnlyDeps(Project project) {
        // we want to test compileOnly deps!
        Configuration compileOnlyConfig = project.getConfigurations().getByName(JavaPlugin.COMPILE_ONLY_CONFIGURATION_NAME);
        Configuration testImplementationConfig = project.getConfigurations().getByName(JavaPlugin.TEST_IMPLEMENTATION_CONFIGURATION_NAME);
        testImplementationConfig.extendsFrom(compileOnlyConfig);
    }

    /**
     * Adds additional manifest info to jars
     */
    static void configureJars(Project project) {
        project.getTasks().withType(Jar.class).configureEach(jarTask -> {
            // we put all our distributable files under distributions
            jarTask.getDestinationDirectory().set(new File(project.getBuildDir(), "distributions"));
            // fixup the jar manifest
            // Explicitly using an Action interface as java lambdas
            // are not supported by Gradle up-to-date checks
            jarTask.doFirst(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    // this doFirst is added before the info plugin, therefore it will run
                    // after the doFirst added by the info plugin, and we can override attributes
                    jarTask.getManifest()
                        .attributes(
                            Map.of("Build-Date", BuildParams.getBuildDate(), "Build-Java-Version", BuildParams.getGradleJavaVersion())
                        );
                }
            });
        });
        project.getPluginManager().withPlugin("com.github.johnrengelman.shadow", p -> {
            project.getTasks().withType(ShadowJar.class).configureEach(shadowJar -> {
                /*
                 * Replace the default "-all" classifier with null
                 * which will leave the classifier off of the file name.
                 */
                shadowJar.getArchiveClassifier().set((String) null);
                /*
                 * Not all cases need service files merged but it is
                 * better to be safe
                 */
                shadowJar.mergeServiceFiles();
            });
            // Add "original" classifier to the non-shadowed JAR to distinguish it from the shadow JAR
            project.getTasks().named(JavaPlugin.JAR_TASK_NAME, Jar.class).configure(jar -> jar.getArchiveClassifier().set("original"));
            // Make sure we assemble the shadow jar
            project.getTasks().named(BasePlugin.ASSEMBLE_TASK_NAME).configure(task -> task.dependsOn("shadowJar"));
        });
    }

    private static void configureJarManifest(Project project) {
        project.getPlugins().withType(InfoBrokerPlugin.class).whenPluginAdded(manifestPlugin -> {
            manifestPlugin.add("Module-Origin", toStringable(BuildParams::getGitOrigin));
            manifestPlugin.add("Change", toStringable(BuildParams::getGitRevision));
            manifestPlugin.add("X-Compile-Elasticsearch-Version", toStringable(VersionProperties::getElasticsearch));
            manifestPlugin.add("X-Compile-Lucene-Version", toStringable(VersionProperties::getLucene));
            manifestPlugin.add(
                "X-Compile-Elasticsearch-Snapshot",
                toStringable(() -> Boolean.toString(VersionProperties.isElasticsearchSnapshot()))
            );
        });

        project.getPluginManager().apply("nebula.info-broker");
        project.getPluginManager().apply("nebula.info-basic");
        project.getPluginManager().apply("nebula.info-java");
        project.getPluginManager().apply("nebula.info-jar");
    }

    private static void configureJavadoc(Project project) {
        project.getTasks().withType(Javadoc.class).configureEach(javadoc -> {
            /*
             * Generate docs using html5 to suppress a warning from `javadoc`
             * that the default will change to html5 in the future.
             */
            CoreJavadocOptions javadocOptions = (CoreJavadocOptions) javadoc.getOptions();
            javadocOptions.addBooleanOption("html5", true);
        });

        TaskProvider<Javadoc> javadoc = project.getTasks().withType(Javadoc.class).named("javadoc");

        // for now, just build without modular doc information
        javadoc.configure(doc -> doc.exclude("module-info.java"));

        // remove compiled classes from the Javadoc classpath:
        // http://mail.openjdk.java.net/pipermail/javadoc-dev/2018-January/000400.html
        javadoc.configure(doc -> doc.setClasspath(Util.getJavaMainSourceSet(project).get().getCompileClasspath()));

        // ensure javadoc task is run with 'check'
        project.getTasks().named(LifecycleBasePlugin.CHECK_TASK_NAME).configure(t -> t.dependsOn(javadoc));
    }

}
