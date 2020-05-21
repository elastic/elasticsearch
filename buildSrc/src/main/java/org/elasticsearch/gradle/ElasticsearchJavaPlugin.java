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

import com.github.jengelman.gradle.plugins.shadow.ShadowBasePlugin;
import com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar;
import nebula.plugin.info.InfoBrokerPlugin;
import org.elasticsearch.gradle.info.BuildParams;
import org.elasticsearch.gradle.info.GlobalBuildInfoPlugin;
import org.elasticsearch.gradle.test.ErrorReportingTestListener;
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
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.bundling.Jar;
import org.gradle.api.tasks.compile.CompileOptions;
import org.gradle.api.tasks.compile.GroovyCompile;
import org.gradle.api.tasks.compile.JavaCompile;
import org.gradle.api.tasks.javadoc.Javadoc;
import org.gradle.api.tasks.testing.Test;
import org.gradle.external.javadoc.CoreJavadocOptions;
import org.gradle.internal.jvm.Jvm;
import org.gradle.language.base.plugins.LifecycleBasePlugin;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.elasticsearch.gradle.util.GradleUtils.maybeConfigure;
import static org.elasticsearch.gradle.util.Util.toStringable;

/**
 * A wrapper around Gradle's Java plugin that applies our common configuration.
 */
public class ElasticsearchJavaPlugin implements Plugin<Project> {
    @Override
    public void apply(Project project) {
        // make sure the global build info plugin is applied to the root project
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);

        project.getPluginManager().apply(JavaPlugin.class);
        configureConfigurations(project);
        configureCompile(project);
        configureInputNormalization(project);
        configureTestTasks(project);
        configureJarManifest(project);
        configureJavadoc(project);
    }

    /**
     * Makes dependencies non-transitive.
     *
     * Gradle allows setting all dependencies as non-transitive very easily.
     * Sadly this mechanism does not translate into maven pom generation. In order
     * to effectively make the pom act as if it has no transitive dependencies,
     * we must exclude each transitive dependency of each direct dependency.
     *
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
        Configuration testCompileConfig = project.getConfigurations().getByName(JavaPlugin.TEST_COMPILE_CONFIGURATION_NAME);
        testCompileConfig.extendsFrom(compileOnlyConfig);

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

        // force all dependencies added directly to compile/testCompile to be non-transitive, except for ES itself
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
        disableTransitiveDeps.accept(JavaPlugin.COMPILE_CONFIGURATION_NAME);
        disableTransitiveDeps.accept(JavaPlugin.TEST_COMPILE_CONFIGURATION_NAME);
        disableTransitiveDeps.accept(JavaPlugin.COMPILE_ONLY_CONFIGURATION_NAME);
        disableTransitiveDeps.accept(JavaPlugin.RUNTIME_ONLY_CONFIGURATION_NAME);
    }

    /** Adds compiler settings to the project */
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
        // common options to both java and groovy
        Consumer<CompileOptions> configureFork = compileOptions -> {
            // we only fork if the Gradle JDK is not the same as the compiler JDK
            String compilerJavaHome = canonicalPath.apply(BuildParams.getCompilerJavaHome());
            String currentJavaHome = canonicalPath.apply(Jvm.current().getJavaHome());
            if (compilerJavaHome.equals(currentJavaHome)) {
                compileOptions.setFork(false);
            } else {
                compileOptions.setFork(true);
                compileOptions.getForkOptions().setJavaHome(BuildParams.getCompilerJavaHome());
            }
        };

        project.afterEvaluate(p -> {
            project.getTasks().withType(JavaCompile.class).configureEach(compileTask -> {
                CompileOptions compileOptions = compileTask.getOptions();

                configureFork.accept(compileOptions);
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
                configureFork.accept(compileTask.getOptions());

                // TODO: this probably shouldn't apply to groovy at all?
                // TODO: use native Gradle support for --release when available (cf. https://github.com/gradle/gradle/issues/2510)
                final JavaVersion targetCompatibilityVersion = JavaVersion.toVersion(compileTask.getTargetCompatibility());
                final List<String> compilerArgs = compileTask.getOptions().getCompilerArgs();
                compilerArgs.add("--release");
                compilerArgs.add(targetCompatibilityVersion.getMajorVersion());
            });
        });

        project.getPluginManager().withPlugin("com.github.johnrengelman.shadow", plugin -> {
            // Ensure that when we are compiling against the "original" JAR that we also include any "shadow" dependencies on the compile
            // classpath
            Configuration shadowConfig = project.getConfigurations().getByName(ShadowBasePlugin.getCONFIGURATION_NAME());
            Configuration apiConfig = project.getConfigurations().getByName(JavaPlugin.API_ELEMENTS_CONFIGURATION_NAME);
            shadowConfig.getDependencies().all(dependency -> apiConfig.getDependencies().add(dependency));
        });
    }

    /**
     * Apply runtime classpath input normalization so that changes in JAR manifests don't break build cacheability
     */
    public static void configureInputNormalization(Project project) {
        project.getNormalization().getRuntimeClasspath().ignore("META-INF/MANIFEST.MF");
    }

    public static void configureTestTasks(Project project) {
        // Default test task should run only unit tests
        maybeConfigure(project.getTasks(), "test", Test.class, task -> task.include("**/*Tests.class"));

        // none of this stuff is applicable to the `:buildSrc` project tests
        if (project.getPath().equals(":build-tools")) {
            return;
        }

        File heapdumpDir = new File(project.getBuildDir(), "heapdump");

        project.getTasks().withType(Test.class).configureEach(test -> {
            File testOutputDir = new File(test.getReports().getJunitXml().getDestination(), "output");

            ErrorReportingTestListener listener = new ErrorReportingTestListener(test.getTestLogging(), testOutputDir);
            test.getExtensions().add("errorReportingTestListener", listener);
            test.addTestOutputListener(listener);
            test.addTestListener(listener);

            /*
             * We use lazy-evaluated strings in order to configure system properties whose value will not be known until
             * execution time (e.g. cluster port numbers). Adding these via the normal DSL doesn't work as these get treated
             * as task inputs and therefore Gradle attempts to snapshot them before/after task execution. This fails due
             * to the GStrings containing references to non-serializable objects.
             *
             * We bypass this by instead passing this system properties vi a CommandLineArgumentProvider. This has the added
             * side-effect that these properties are NOT treated as inputs, therefore they don't influence things like the
             * build cache key or up to date checking.
             */
            SystemPropertyCommandLineArgumentProvider nonInputProperties = new SystemPropertyCommandLineArgumentProvider();

            // We specifically use an anonymous inner class here because lambda task actions break Gradle cacheability
            // See: https://docs.gradle.org/current/userguide/more_about_tasks.html#sec:how_does_it_work
            test.doFirst(new Action<>() {
                @Override
                public void execute(Task t) {
                    project.mkdir(testOutputDir);
                    project.mkdir(heapdumpDir);
                    project.mkdir(test.getWorkingDir());
                    project.mkdir(test.getWorkingDir().toPath().resolve("temp"));

                    // TODO remove once jvm.options are added to test system properties
                    test.systemProperty("java.locale.providers", "SPI,COMPAT");
                }
            });
            if (BuildParams.isInFipsJvm()) {
                project.getDependencies().add("testRuntimeOnly", "org.bouncycastle:bc-fips:1.0.1");
                project.getDependencies().add("testRuntimeOnly", "org.bouncycastle:bctls-fips:1.0.9");
            }
            test.getJvmArgumentProviders().add(nonInputProperties);
            test.getExtensions().add("nonInputProperties", nonInputProperties);

            test.setWorkingDir(project.file(project.getBuildDir() + "/testrun/" + test.getName()));
            test.setMaxParallelForks(Integer.parseInt(System.getProperty("tests.jvms", BuildParams.getDefaultParallel().toString())));

            test.exclude("**/*$*.class");

            test.jvmArgs(
                "-Xmx" + System.getProperty("tests.heap.size", "512m"),
                "-Xms" + System.getProperty("tests.heap.size", "512m"),
                "--illegal-access=warn",
                "-XX:+HeapDumpOnOutOfMemoryError"
            );

            test.getJvmArgumentProviders().add(new SimpleCommandLineArgumentProvider("-XX:HeapDumpPath=" + heapdumpDir));

            String argline = System.getProperty("tests.jvm.argline");
            if (argline != null) {
                test.jvmArgs((Object[]) argline.split(" "));
            }

            if (Util.getBooleanProperty("tests.asserts", true)) {
                test.jvmArgs("-ea", "-esa");
            }

            Map<String, String> sysprops = Map.of(
                "java.awt.headless",
                "true",
                "tests.gradle",
                "true",
                "tests.artifact",
                project.getName(),
                "tests.task",
                test.getPath(),
                "tests.security.manager",
                "true",
                "jna.nosys",
                "true"
            );
            test.systemProperties(sysprops);

            // ignore changing test seed when build is passed -Dignore.tests.seed for cacheability experimentation
            if (System.getProperty("ignore.tests.seed") != null) {
                nonInputProperties.systemProperty("tests.seed", BuildParams.getTestSeed());
            } else {
                test.systemProperty("tests.seed", BuildParams.getTestSeed());
            }

            // don't track these as inputs since they contain absolute paths and break cache relocatability
            File gradleHome = project.getGradle().getGradleUserHomeDir();
            String gradleVersion = project.getGradle().getGradleVersion();
            nonInputProperties.systemProperty("gradle.dist.lib", new File(project.getGradle().getGradleHomeDir(), "lib"));
            nonInputProperties.systemProperty(
                "gradle.worker.jar",
                gradleHome + "/caches/" + gradleVersion + "/workerMain/gradle-worker.jar"
            );
            nonInputProperties.systemProperty("gradle.user.home", gradleHome);
            // we use 'temp' relative to CWD since this is per JVM and tests are forbidden from writing to CWD
            nonInputProperties.systemProperty("java.io.tmpdir", test.getWorkingDir().toPath().resolve("temp"));

            nonInputProperties.systemProperty("compiler.java", BuildParams.getCompilerJavaVersion().getMajorVersion());
            nonInputProperties.systemProperty("runtime.java", BuildParams.getRuntimeJavaVersion().getMajorVersion());

            // TODO: remove setting logging level via system property
            test.systemProperty("tests.logger.level", "WARN");
            System.getProperties().entrySet().forEach(entry -> {
                if ((entry.getKey().toString().startsWith("tests.") || entry.getKey().toString().startsWith("es."))) {
                    test.systemProperty(entry.getKey().toString(), entry.getValue());
                }
            });

            // TODO: remove this once ctx isn't added to update script params in 7.0
            test.systemProperty("es.scripting.update.ctx_in_params", "false");

            // TODO: remove this property in 8.0
            test.systemProperty("es.search.rewrite_sort", "true");

            // TODO: remove this once cname is prepended to transport.publish_address by default in 8.0
            test.systemProperty("es.transport.cname_in_publish_address", "true");

            // Set netty system properties to the properties we configure in jvm.options
            test.systemProperty("io.netty.noUnsafe", "true");
            test.systemProperty("io.netty.noKeySetOptimization", "true");
            test.systemProperty("io.netty.recycler.maxCapacityPerThread", "0");

            test.testLogging(logging -> {
                logging.setShowExceptions(true);
                logging.setShowCauses(true);
                logging.setExceptionFormat("full");
            });

            if (OS.current().equals(OS.WINDOWS) && System.getProperty("tests.timeoutSuite") == null) {
                // override the suite timeout to 30 mins for windows, because it has the most inefficient filesystem known to man
                test.systemProperty("tests.timeoutSuite", "1800000!");
            }

            /*
             *  If this project builds a shadow JAR than any unit tests should test against that artifact instead of
             *  compiled class output and dependency jars. This better emulates the runtime environment of consumers.
             */
            project.getPluginManager().withPlugin("com.github.johnrengelman.shadow", p -> {
                // Remove output class files and any other dependencies from the test classpath, since the shadow JAR includes these
                FileCollection mainRuntime = project.getExtensions()
                    .getByType(SourceSetContainer.class)
                    .getByName(SourceSet.MAIN_SOURCE_SET_NAME)
                    .getRuntimeClasspath();
                // Add any "shadow" dependencies. These are dependencies that are *not* bundled into the shadow JAR
                Configuration shadowConfig = project.getConfigurations().getByName(ShadowBasePlugin.getCONFIGURATION_NAME());
                // Add the shadow JAR artifact itself
                FileCollection shadowJar = project.files(project.getTasks().named("shadowJar"));

                test.setClasspath(test.getClasspath().minus(mainRuntime).plus(shadowConfig).plus(shadowJar));
            });
        });
    }

    /** Adds additional manifest info to jars */
    static void configureJars(Project project) {
        ExtraPropertiesExtension ext = project.getExtensions().getExtraProperties();
        ext.set("licenseFile", null);
        ext.set("noticeFile", null);
        project.getTasks()
            .withType(Jar.class)
            .configureEach(
                jarTask -> {
                    // we put all our distributable files under distributions
                    jarTask.getDestinationDirectory().set(new File(project.getBuildDir(), "distributions"));
                    // fixup the jar manifest
                    jarTask.doFirst(
                        t -> {
                            // this doFirst is added before the info plugin, therefore it will run
                            // after the doFirst added by the info plugin, and we can override attributes
                            jarTask.getManifest()
                                .attributes(
                                    Map.of(
                                        "Build-Date",
                                        BuildParams.getBuildDate(),
                                        "Build-Java-Version",
                                        BuildParams.getCompilerJavaVersion()
                                    )
                                );
                        }
                    );
                }
            );
        // add license/notice files
        project.afterEvaluate(p -> project.getTasks().withType(Jar.class).configureEach(jarTask -> {
            File licenseFile = (File) ext.get("licenseFile");
            File noticeFile = (File) ext.get("noticeFile");
            if (licenseFile == null || noticeFile == null) {
                throw new GradleException("Must specify license and notice file for project");
            }

            jarTask.metaInf(spec -> {
                spec.from(licenseFile.getParent(), from -> {
                    from.include(licenseFile.getName());
                    from.rename(s -> "LICENSE.txt");
                });
                spec.from(noticeFile.getParent(), from -> {
                    from.include(noticeFile.getName());
                    from.rename(s -> "NOTICE.txt");
                });
            });
        }));
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
            // only explicitly set javadoc executable if compiler JDK is different from Gradle
            // this ensures better cacheability as setting ths input to an absolute path breaks portability
            Path compilerJvm = BuildParams.getCompilerJavaHome().toPath();
            Path gradleJvm = Jvm.current().getJavaHome().toPath();
            try {
                if (Files.isSameFile(compilerJvm, gradleJvm) == false) {
                    javadoc.setExecutable(compilerJvm.resolve("bin/javadoc").toString());
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            // remove compiled classes from the Javadoc classpath:
            // http://mail.openjdk.java.net/pipermail/javadoc-dev/2018-January/000400.html
            javadoc.setClasspath(Util.getJavaMainSourceSet(project).get().getCompileClasspath());
            /*
             * Generate docs using html5 to suppress a warning from `javadoc`
             * that the default will change to html5 in the future.
             */
            CoreJavadocOptions javadocOptions = (CoreJavadocOptions) javadoc.getOptions();
            javadocOptions.addBooleanOption("html5", true);
        });
        // ensure javadoc task is run with 'check'
        project.getTasks()
            .named(LifecycleBasePlugin.CHECK_TASK_NAME)
            .configure(t -> t.dependsOn(project.getTasks().withType(Javadoc.class)));
    }
}
