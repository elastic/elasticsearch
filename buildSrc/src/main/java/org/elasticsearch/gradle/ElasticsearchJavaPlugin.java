/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle;

import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar;
import nebula.plugin.info.InfoBrokerPlugin;
import org.elasticsearch.gradle.info.BuildParams;
import org.elasticsearch.gradle.info.GlobalBuildInfoPlugin;
import org.elasticsearch.gradle.util.Util;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.JavaVersion;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.ModuleDependency;
import org.gradle.api.artifacts.ProjectDependency;
import org.gradle.api.artifacts.ResolutionStrategy;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.JavaLibraryPlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.api.tasks.compile.CompileOptions;
import org.gradle.api.tasks.compile.GroovyCompile;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.api.tasks.javadoc.Javadoc;
import org.gradle.external.javadoc.CoreJavadocOptions;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.gradle.util.Util.toStringable;

/**
 * A wrapper around Gradle's Java plugin that applies our common configuration.
 */
public class ElasticsearchJavaPlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        // make sure the global build info plugin is applied to the root project
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);
        // common repositories setup
        project.getPluginManager().apply(RepositoriesSetupPlugin.class);
        project.getPluginManager().apply(JavaLibraryPlugin.class);
        project.getPluginManager().apply(ElasticsearchTestBasePlugin.class);

        configureConfigurations(project);
        configureCompile(project);
        configureInputNormalization(project);
        configureJars(project);
        configureJarManifest(project);
        configureJavadoc(project);
    }

    /**
     * Makes dependencies non-transitive.
     * <p>
     * Gradle allows setting all dependencies as non-transitive very easily.
     * Sadly this mechanism does not translate into maven pom generation. In order
     * to effectively make the pom act as if it has no transitive dependencies,
     * we must exclude each transitive dependency of each direct dependency.
     * <p>
     * Determining the transitive deps of a dependency which has been resolved as
     * non-transitive is difficult because the process of resolving removes the
     * transitive deps. To sidestep this issue, we create a configuration per
     * direct dependency version. This specially named and unique configuration
     * will contain all of the transitive dependencies of this particular
     * dependency. We can then use this configuration during pom generation
     * to iterate the transitive dependencies and add excludes.
     */
    public static void configureConfigurations(Project project) {
        // we want to test compileOnly deps!
        Configuration compileOnlyConfig = project.getConfigurations().getByName(JavaPlugin.COMPILE_ONLY_CONFIGURATION_NAME);
        Configuration testImplementationConfig = project.getConfigurations().getByName(JavaPlugin.TEST_IMPLEMENTATION_CONFIGURATION_NAME);
        testImplementationConfig.extendsFrom(compileOnlyConfig);

        // we are not shipping these jars, we act like dumb consumers of these things
        if (project.getPath().startsWith(":test:fixtures") || project.getPath().equals(":build-tools")) {
            return;
        }
        // fail on any conflicting dependency versions
        project.getConfigurations().all(configuration -> {
            if (configuration.getName().endsWith("Fixture")) {
                // just a self contained test-fixture configuration, likely transitive and hellacious
                return;
            }
            configuration.resolutionStrategy(ResolutionStrategy::failOnVersionConflict);
        });

        // force all dependencies added directly to compile/testImplementation to be non-transitive, except for ES itself
        Consumer<String> disableTransitiveDeps = configName -> {
            Configuration config = project.getConfigurations().getByName(configName);
            config.getDependencies().all(dep -> {
                if (dep instanceof ModuleDependency
                    && dep instanceof ProjectDependency == false
                    && dep.getGroup().startsWith("org.elasticsearch") == false) {
                    ((ModuleDependency) dep).setTransitive(false);
                }
            });
        };
        disableTransitiveDeps.accept(JavaPlugin.API_CONFIGURATION_NAME);
        disableTransitiveDeps.accept(JavaPlugin.IMPLEMENTATION_CONFIGURATION_NAME);
        disableTransitiveDeps.accept(JavaPlugin.COMPILE_ONLY_CONFIGURATION_NAME);
        disableTransitiveDeps.accept(JavaPlugin.RUNTIME_ONLY_CONFIGURATION_NAME);
        disableTransitiveDeps.accept(JavaPlugin.TEST_IMPLEMENTATION_CONFIGURATION_NAME);
    }

    /**
     * Adds compiler settings to the project
     */
    public static void configureCompile(Project project) {
        project.getExtensions().getExtraProperties().set("compactProfile", "full");

        JavaPluginExtension java = project.getExtensions().getByType(JavaPluginExtension.class);
        java.setSourceCompatibility(BuildParams.getMinimumRuntimeVersion());
        java.setTargetCompatibility(BuildParams.getMinimumRuntimeVersion());

        Function<File, String> canonicalPath = file -> {
            try {
                return file.getCanonicalPath();
            } catch (IOException e) {
                throw new GradleException("Failed to get canonical path for " + file, e);
            }
        };

        project.afterEvaluate(p -> {
            project.getTasks().withType(JavaCompile.class).configureEach(compileTask -> {
                CompileOptions compileOptions = compileTask.getOptions();
                /*
                 * -path because gradle will send in paths that don't always exist.
                 * -missing because we have tons of missing @returns and @param.
                 * -serial because we don't use java serialization.
                 */
                // don't even think about passing args with -J-xxx, oracle will ask you to submit a bug report :)
                // fail on all javac warnings
                List<String> compilerArgs = compileOptions.getCompilerArgs();
                compilerArgs.add("-Werror");
                compilerArgs.add("-Xlint:all,-path,-serial,-options,-deprecation,-try");
                compilerArgs.add("-Xdoclint:all");
                compilerArgs.add("-Xdoclint:-missing");

                // either disable annotation processor completely (default) or allow to enable them if an annotation processor is explicitly
                // defined
                if (compilerArgs.contains("-processor") == false) {
                    compilerArgs.add("-proc:none");
                }

                compileOptions.setEncoding("UTF-8");
                compileOptions.setIncremental(true);

                // TODO: use native Gradle support for --release when available (cf. https://github.com/gradle/gradle/issues/2510)
                final JavaVersion targetCompatibilityVersion = JavaVersion.toVersion(compileTask.getTargetCompatibility());
                compilerArgs.add("--release");
                compilerArgs.add(targetCompatibilityVersion.getMajorVersion());

            });
            // also apply release flag to groovy, which is used in build-tools
            project.getTasks().withType(GroovyCompile.class).configureEach(compileTask -> {

                // TODO: this probably shouldn't apply to groovy at all?
                // TODO: use native Gradle support for --release when available (cf. https://github.com/gradle/gradle/issues/2510)
                final JavaVersion targetCompatibilityVersion = JavaVersion.toVersion(compileTask.getTargetCompatibility());
                final List<String> compilerArgs = compileTask.getOptions().getCompilerArgs();
                compilerArgs.add("--release");
                compilerArgs.add(targetCompatibilityVersion.getMajorVersion());
            });
        });
    }

    /**
     * Apply runtime classpath input normalization so that changes in JAR manifests don't break build cacheability
     */
    public static void configureInputNormalization(Project project) {
        project.getNormalization().getRuntimeClasspath().ignore("META-INF/MANIFEST.MF");
    }

    /**
     * Adds additional manifest info to jars
     */
    static void configureJars(Project project) {
        project.getTasks()
            .withType(Jar.class)
            .configureEach(
                jarTask -> {
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
                                    Map.of(
                                        "Build-Date",
                                        BuildParams.getBuildDate(),
                                        "Build-Java-Version",
                                        BuildParams.getGradleJavaVersion()
                                    )
                                );
                        }
                    });
                }
            );
        project.getPluginManager().withPlugin("com.github.johnrengelman.shadow", p -> {
            project.getTasks()
                .withType(ShadowJar.class)
                .configureEach(
                    shadowJar -> {
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
                    }
                );
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
        javadoc.configure(doc ->
        // remove compiled classes from the Javadoc classpath:
        // http://mail.openjdk.java.net/pipermail/javadoc-dev/2018-January/000400.html
        doc.setClasspath(Util.getJavaMainSourceSet(project).get().getCompileClasspath()));

        // ensure javadoc task is run with 'check'
        project.getTasks().named(LifecycleBasePlugin.CHECK_TASK_NAME).configure(t -> t.dependsOn(javadoc));
    }
}
