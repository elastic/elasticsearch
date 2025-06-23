/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import nebula.plugin.info.InfoBrokerPlugin;

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.elasticsearch.gradle.internal.info.BuildParameterExtension;
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.gradle.api.Action;
import org.gradle.api.JavaVersion;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.JavaLibraryPlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.api.tasks.javadoc.Javadoc;
import org.gradle.external.javadoc.CoreJavadocOptions;
import org.gradle.jvm.toolchain.JavaLanguageVersion;
import org.gradle.jvm.toolchain.JavaToolchainService;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import java.io.File;
import java.util.Map;

import javax.inject.Inject;

import static org.elasticsearch.gradle.internal.conventions.util.Util.toStringable;
import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;

/**
 * A wrapper around Gradle's Java plugin that applies our
 * common configuration for production code.
 */
public class ElasticsearchJavaPlugin implements Plugin<Project> {

    private final JavaToolchainService javaToolchains;

    @Inject
    ElasticsearchJavaPlugin(JavaToolchainService javaToolchains) {
        this.javaToolchains = javaToolchains;
    }

    @Override
    public void apply(Project project) {
        project.getRootProject().getPlugins().apply(GlobalBuildInfoPlugin.class);
        Property<BuildParameterExtension> buildParams = loadBuildParams(project);
        project.getPluginManager().apply(ElasticsearchJavaBasePlugin.class);
        project.getPluginManager().apply(JavaLibraryPlugin.class);
        project.getPluginManager().apply(ElasticsearchJavaModulePathPlugin.class);

        // configureConfigurations(project);
        configureJars(project, buildParams.get());
        configureJarManifest(project, buildParams.get());
        configureJavadoc(project, buildParams.get());
        testCompileOnlyDeps(project);
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
    static void configureJars(Project project, BuildParameterExtension buildParams) {
        String buildDate = buildParams.getBuildDate().toString();
        JavaVersion gradleJavaVersion = buildParams.getGradleJavaVersion();
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
                    jarTask.getManifest().attributes(Map.of("Build-Date", buildDate, "Build-Java-Version", gradleJavaVersion));
                }
            });
        });
        project.getPluginManager().withPlugin("com.gradleup.shadow", p -> {
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

    private static void configureJarManifest(Project project, BuildParameterExtension buildParams) {
        Provider<String> gitOrigin = buildParams.getGitOrigin();
        Provider<String> gitRevision = buildParams.getGitRevision();

        project.getPlugins().withType(InfoBrokerPlugin.class).whenPluginAdded(manifestPlugin -> {
            manifestPlugin.add("Module-Origin", toStringable(() -> gitOrigin.get()));
            manifestPlugin.add("Change", toStringable(() -> gitRevision.get()));
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

    private void configureJavadoc(Project project, BuildParameterExtension buildParams) {
        project.getTasks().withType(Javadoc.class).configureEach(javadoc -> {
            /*
             * Generate docs using html5 to suppress a warning from `javadoc`
             * that the default will change to html5 in the future.
             */
            CoreJavadocOptions javadocOptions = (CoreJavadocOptions) javadoc.getOptions();
            javadocOptions.addBooleanOption("html5", true);

            javadoc.getJavadocTool().set(javaToolchains.javadocToolFor(spec -> {
                spec.getLanguageVersion().set(JavaLanguageVersion.of(buildParams.getMinimumRuntimeVersion().getMajorVersion()));
            }));
        });

        TaskProvider<Javadoc> javadoc = project.getTasks().withType(Javadoc.class).named("javadoc");

        // remove compiled classes from the Javadoc classpath:
        // http://mail.openjdk.java.net/pipermail/javadoc-dev/2018-January/000400.html
        javadoc.configure(doc -> doc.setClasspath(Util.getJavaMainSourceSet(project).get().getCompileClasspath()));

        // ensure javadoc task is run with 'check'
        project.getTasks().named(LifecycleBasePlugin.CHECK_TASK_NAME).configure(t -> t.dependsOn(javadoc));
    }
}
