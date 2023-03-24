/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import kotlinx.kover.KoverPlugin;
import kotlinx.kover.api.CounterType;
import kotlinx.kover.api.DefaultIntellijEngine;
import kotlinx.kover.api.KoverProjectConfig;
import kotlinx.kover.api.VerificationTarget;
import kotlinx.kover.api.VerificationValueType;
import kotlinx.kover.tasks.KoverVerificationTask;
import nebula.plugin.info.InfoBrokerPlugin;

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar;

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.elasticsearch.gradle.internal.info.BuildParams;
import org.elasticsearch.gradle.internal.precommit.transport.TransportTestExistPrecommitPlugin;
import org.elasticsearch.gradle.internal.precommit.transport.TransportTestExistTask;
import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.JavaLibraryPlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.api.tasks.javadoc.Javadoc;
import org.gradle.external.javadoc.CoreJavadocOptions;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.gradle.internal.conventions.util.Util.toStringable;

/**
 * A wrapper around Gradle's Java plugin that applies our
 * common configuration for production code.
 */
public class ElasticsearchJavaPlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(ElasticsearchJavaBasePlugin.class);
        project.getPluginManager().apply(JavaLibraryPlugin.class);
        project.getPluginManager().apply(ElasticsearchJavaModulePathPlugin.class);

        // configureConfigurations(project);
        configureJars(project);
        configureJarManifest(project);
        configureJavadoc(project);
        testCompileOnlyDeps(project);

        configureJacoco(project);
    }

    private static void configureJacoco(Project project) {
        project.getPluginManager().apply(KoverPlugin.class);

        project.getPluginManager().apply(TransportTestExistPrecommitPlugin.class);

        // project.getTasks().named("test").configure(task -> { task.finalizedBy(project.getTasks().named("transportTestExistCheck")); });

        project.getTasks().named("transportTestExistCheck").configure(task -> {
            task.mustRunAfter("compileJava");
        });
            project.getTasks().named("koverVerify", KoverVerificationTask.class).configure(t -> {
            // t.dependsOn(project.getTasks().named("transportTestExistCheck"));// *1
            TransportTestExistTask transportTestExistCheck = project.getTasks()
                .named("transportTestExistCheck", TransportTestExistTask.class)
                .get();
            t.dependsOn(transportTestExistCheck);

            t.doFirst(t2 -> {
                project.getExtensions().configure(KoverProjectConfig.class, kover -> {
                    kover.verify(verify -> {
                        verify.rule(rule -> {

                            rule.overrideClassFilter(koverClassFilter -> {
                                List<String> transportClasses = readTransportClasses(transportTestExistCheck.getOutputFile());
                                koverClassFilter.getIncludes().addAll(transportClasses);
                            });
                            rule.setTarget(VerificationTarget.CLASS);
                            rule.setEnabled(true);
                            rule.bound(bound -> {
                                bound.setMinValue(100);
                                bound.setCounter(CounterType.INSTRUCTION);
                                bound.setValueType(VerificationValueType.COVERED_PERCENTAGE);
                            });
                        });
                    });
                });

            });
        });
        // adding a fake rule so that verification can run. the real rule is added with doFirst because of transportClass scanning being
        // done after tests
        project.getExtensions().configure(KoverProjectConfig.class, kover -> {
            kover.getEngine().set(DefaultIntellijEngine.INSTANCE);
            kover.verify(verify -> {
                verify.rule(rule -> {
                    rule.bound(bound -> {
                        bound.setMinValue(0);
                        bound.setCounter(CounterType.INSTRUCTION);
                        bound.setValueType(VerificationValueType.COVERED_PERCENTAGE);
                    });
                });
            });
        });

        /*
        > Task :modules:ingest-geoip:transportTestExistCheck
        Execution optimizations have been disabled for task ':modules:ingest-geoip:transportTestExistCheck' to ensure correctness due to the following reasons:
        - Gradle detected a problem with the following location: '/Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/modules/ingest-geoip/build/generated-resources/transport-classes.txt'. Reason: Task ':modules:ingest-geoip:internalClusterTest' uses this output of task ':modules:ingest-geoip:transportTestExistCheck' without declaring an explicit or implicit dependency. This can lead to incorrect results being produced, depending on what order the tasks are executed. Please refer to https://docs.gradle.org/7.6.1/userguide/validation_problems.html#implicit_dependency for more details about this problem.
        - Gradle detected a problem with the following location: '/Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/modules/ingest-geoip/build/generated-resources/transport-classes.txt'. Reason: Task ':modules:ingest-geoip:test' uses this output of task ':modules:ingest-geoip:transportTestExistCheck' without declaring an explicit or implicit dependency. This can lead to incorrect results being produced, depending on what order the tasks are executed. Please refer to https://docs.gradle.org/7.6.1/userguide/validation_problems.html#implicit_dependency for more details about this problem.
        - Gradle detected a problem with the following location: '/Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/modules/ingest-geoip/build/generated-resources/transport-classes.txt'. Reason: Task ':modules:ingest-geoip:compileInternalClusterTestJava' uses this output of task ':modules:ingest-geoip:transportTestExistCheck' without declaring an explicit or implicit dependency. This can lead to incorrect results being produced, depending on what order the tasks are executed. Please refer to https://docs.gradle.org/7.6.1/userguide/validation_problems.html#implicit_dependency for more details about this problem.
        Gradle detected a problem with the following location: '/Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/modules/ingest-geoip/build/generated-resources/transport-classes.txt'. Reason: Task ':modules:ingest-geoip:internalClusterTest' uses this output of task ':modules:ingest-geoip:transportTestExistCheck' without declaring an explicit or implicit dependency. This can lead to incorrect results being produced, depending on what order the tasks are executed. This behaviour has been deprecated and is scheduled to be removed in Gradle 8.0. Execution optimizations are disabled to ensure correctness. See https://docs.gradle.org/7.6.1/userguide/validation_problems.html#implicit_dependency for more details.
        Gradle detected a problem with the following location: '/Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/modules/ingest-geoip/build/generated-resources/transport-classes.txt'. Reason: Task ':modules:ingest-geoip:test' uses this output of task ':modules:ingest-geoip:transportTestExistCheck' without declaring an explicit or implicit dependency. This can lead to incorrect results being produced, depending on what order the tasks are executed. This behaviour has been deprecated and is scheduled to be removed in Gradle 8.0. Execution optimizations are disabled to ensure correctness. See https://docs.gradle.org/7.6.1/userguide/validation_problems.html#implicit_dependency for more details.
        Gradle detected a problem with the following location: '/Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/modules/ingest-geoip/build/generated-resources/transport-classes.txt'. Reason: Task ':modules:ingest-geoip:compileInternalClusterTestJava' uses this output of task ':modules:ingest-geoip:transportTestExistCheck' without declaring an explicit or implicit dependency. This can lead to incorrect results being produced, depending on what order the tasks are executed. This behaviour has been deprecated and is scheduled to be removed in Gradle 8.0. Execution optimizations are disabled to ensure correctness. See https://docs.gradle.org/7.6.1/userguide/validation_problems.html#implicit_dependency for more details.
        */

    }

    private static List<String> readTransportClasses(RegularFileProperty file) {
        try {
            // - Gradle detected a problem with the following location:
            // '/Users/przemyslawgomulka/workspace/pgomulka/elasticsearch/modules/aggregations/build/generated-resources/transport-classes.txt'.
            // Reason: Task ':modules:aggregations:test' uses this output of task ':modules:aggregations:transportTestExistCheck' without
            // declaring an explicit or implicit dependency. This can lead to incorrect results being produced, depending on what order the
            // tasks are executed. Please refer to https://docs.gradle.org/7.6.1/userguide/validation_problems.html#implicit_dependency for
            // more details about this problem.
            // Provider<RegularFile> file = project.getLayout().getBuildDirectory().file(TransportTestExistTask.TRANSPORT_CLASSES);
            // RegularFileProperty file = project.getTasks()
            // .named("transportTestExistCheck", TransportTestExistTask.class)
            // .get()
            // .getOutputFile();
            Path path = file.get().getAsFile().toPath();
            return Files.readAllLines(path);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return Collections.emptyList();
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

        // remove compiled classes from the Javadoc classpath:
        // http://mail.openjdk.java.net/pipermail/javadoc-dev/2018-January/000400.html
        javadoc.configure(doc -> doc.setClasspath(Util.getJavaMainSourceSet(project).get().getCompileClasspath()));

        // ensure javadoc task is run with 'check'
        project.getTasks().named(LifecycleBasePlugin.CHECK_TASK_NAME).configure(t -> t.dependsOn(javadoc));
    }
}
