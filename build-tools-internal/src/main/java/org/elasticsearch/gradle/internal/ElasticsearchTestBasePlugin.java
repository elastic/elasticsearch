/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal;

import com.github.jengelman.gradle.plugins.shadow.ShadowBasePlugin;

import org.elasticsearch.gradle.OS;
import org.elasticsearch.gradle.dependencies.CompileOnlyResolvePlugin;
import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.elasticsearch.gradle.internal.test.ErrorReportingTestListener;
import org.elasticsearch.gradle.internal.test.SimpleCommandLineArgumentProvider;
import org.elasticsearch.gradle.test.GradleTestPolicySetupPlugin;
import org.elasticsearch.gradle.test.SystemPropertyCommandLineArgumentProvider;
import org.gradle.api.Action;
import org.gradle.api.JavaVersion;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.testing.Test;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.inject.Inject;

import static java.util.stream.Collectors.joining;
import static org.elasticsearch.gradle.dependencies.CompileOnlyResolvePlugin.RESOLVEABLE_COMPILE_ONLY_CONFIGURATION_NAME;
import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;
import static org.elasticsearch.gradle.util.FileUtils.mkdirs;
import static org.elasticsearch.gradle.util.GradleUtils.maybeConfigure;

/**
 * Applies commonly used settings to all Test tasks in the project
 */
public abstract class ElasticsearchTestBasePlugin implements Plugin<Project> {

    public static final String DUMP_OUTPUT_ON_FAILURE_PROP_NAME = "dumpOutputOnFailure";

    @Inject
    protected abstract ProviderFactory getProviderFactory();

    @Override
    public void apply(Project project) {
        project.getRootProject().getPlugins().apply(GlobalBuildInfoPlugin.class);
        var buildParams = loadBuildParams(project);
        project.getPluginManager().apply(GradleTestPolicySetupPlugin.class);
        // for fips mode check
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);
        // Default test task should run only unit tests
        maybeConfigure(project.getTasks(), "test", Test.class, task -> task.include("**/*Tests.class"));

        // none of this stuff is applicable to the `:buildSrc` project tests
        File heapdumpDir = new File(project.getBuildDir(), "heapdump");

        project.getTasks().withType(Test.class).configureEach(test -> {
            File testOutputDir = new File(test.getReports().getJunitXml().getOutputLocation().getAsFile().get(), "output");

            ErrorReportingTestListener listener = new ErrorReportingTestListener(test, testOutputDir);
            test.getExtensions().getExtraProperties().set(DUMP_OUTPUT_ON_FAILURE_PROP_NAME, true);
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
                    mkdirs(testOutputDir);
                    mkdirs(heapdumpDir);
                    mkdirs(test.getWorkingDir());
                    mkdirs(test.getWorkingDir().toPath().resolve("temp").toFile());

                    // TODO remove once jvm.options are added to test system properties
                    test.systemProperty("java.locale.providers", "CLDR");
                }
            });
            test.getJvmArgumentProviders().add(nonInputProperties);
            test.getExtensions().add("nonInputProperties", nonInputProperties);

            test.setWorkingDir(project.file(project.getBuildDir() + "/testrun/" + test.getName().replace("#", "_")));
            test.setMaxParallelForks(Integer.parseInt(System.getProperty("tests.jvms", buildParams.get().getDefaultParallel().toString())));

            test.exclude("**/*$*.class");

            test.jvmArgs(
                "-Xmx" + System.getProperty("tests.heap.size", "512m"),
                "-Xms" + System.getProperty("tests.heap.size", "512m"),
                "-Dtests.testfeatures.enabled=true",
                "--add-opens=java.base/java.util=ALL-UNNAMED",
                // TODO: only open these for mockito when it is modularized
                "--add-opens=java.base/java.security.cert=ALL-UNNAMED",
                "--add-opens=java.base/java.nio.channels=ALL-UNNAMED",
                "--add-opens=java.base/java.net=ALL-UNNAMED",
                "--add-opens=java.base/javax.net.ssl=ALL-UNNAMED",
                "--add-opens=java.base/java.nio.file=ALL-UNNAMED",
                "--add-opens=java.base/java.time=ALL-UNNAMED",
                "--add-opens=java.management/java.lang.management=ALL-UNNAMED",
                "--enable-native-access=ALL-UNNAMED",
                "-XX:+HeapDumpOnOutOfMemoryError"
            );

            test.getJvmArgumentProviders().add(new SimpleCommandLineArgumentProvider("-XX:HeapDumpPath=" + heapdumpDir));
            test.getJvmArgumentProviders().add(() -> {
                if (test.getJavaVersion().compareTo(JavaVersion.VERSION_23) <= 0) {
                    return List.of("-Djava.security.manager=allow");
                } else {
                    return List.of();
                }
            });

            String argline = System.getProperty("tests.jvm.argline");
            if (argline != null) {
                test.jvmArgs((Object[]) argline.split(" "));
            }

            // Check if "tests.asserts" is false or "tests.jvm.argline" contains the "-da" flag.
            boolean disableAssertions = Util.getBooleanProperty("tests.asserts", true) == false
                || (argline != null && (argline.contains("-da")))
                || (argline != null && (argline.contains("-disableassertions")));

            if (disableAssertions) {
                System.out.println("disable assertions");
                test.setEnableAssertions(false);
            }
            Map<String, String> sysprops = Map.of(
                "java.awt.headless",
                "true",
                "tests.artifact",
                project.getName(),
                "tests.security.manager",
                "true",
                "jna.nosys",
                "true"
            );
            test.systemProperties(sysprops);

            // ignore changing test seed when build is passed -Dignore.tests.seed for cacheability experimentation
            if (System.getProperty("ignore.tests.seed") != null) {
                nonInputProperties.systemProperty("tests.seed", buildParams.get().getTestSeed());
            } else {
                test.systemProperty("tests.seed", buildParams.get().getTestSeed());
            }

            // don't track these as inputs since they contain absolute paths and break cache relocatability
            File gradleUserHome = project.getGradle().getGradleUserHomeDir();
            nonInputProperties.systemProperty("gradle.user.home", gradleUserHome);
            nonInputProperties.systemProperty("workspace.dir", Util.locateElasticsearchWorkspace(project.getGradle()));
            // we use 'temp' relative to CWD since this is per JVM and tests are forbidden from writing to CWD
            nonInputProperties.systemProperty("java.io.tmpdir", test.getWorkingDir().toPath().resolve("temp"));

            nonInputProperties.systemProperty("es.entitlement.testOnlyPath", ()->{
                SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
                FileCollection mainRuntime = sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME).getRuntimeClasspath();
                FileCollection testRuntime = sourceSets.getByName(SourceSet.TEST_SOURCE_SET_NAME).getRuntimeClasspath();
                FileCollection result = testRuntime.minus(mainRuntime);
                Configuration compileOnly = project.getConfigurations()
                    .findByName(RESOLVEABLE_COMPILE_ONLY_CONFIGURATION_NAME);
                if (compileOnly != null) {
//                    result.minus(compileOnly);
                }
                String asPath = result.getAsPath();
                String[] pathEntries = asPath.split(File.pathSeparator);
                Arrays.sort(pathEntries);
                System.err.println("PATDOYLE - for " + project.getName() + " " + test.getName() + " using testOnlyPath:\n" + String.join("\n", pathEntries));
                return asPath;
            });

            test.systemProperties(getProviderFactory().systemPropertiesPrefixedBy("tests.").get());
            test.systemProperties(getProviderFactory().systemPropertiesPrefixedBy("es.").get());

            // TODO: remove setting logging level via system property
            test.systemProperty("tests.logger.level", "WARN");

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
                // override the suite timeout to 60 mins for windows, because it has the most inefficient filesystem known to man
                test.systemProperty("tests.timeoutSuite", "3600000!");
            }

            /*
             *  If this project builds a shadow JAR than any unit tests should test against that artifact instead of
             *  compiled class output and dependency jars. This better emulates the runtime environment of consumers.
             */
            project.getPluginManager().withPlugin("com.gradleup.shadow", p -> {
                if (test.getName().equals(JavaPlugin.TEST_TASK_NAME)) {
                    // Remove output class files and any other dependencies from the test classpath, since the shadow JAR includes these
                    SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
                    FileCollection mainRuntime = sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME).getRuntimeClasspath();
                    // Add any "shadow" dependencies. These are dependencies that are *not* bundled into the shadow JAR
                    Configuration shadowConfig = project.getConfigurations().getByName(ShadowBasePlugin.CONFIGURATION_NAME);
                    // Add the shadow JAR artifact itself
                    FileCollection shadowJar = project.files(project.getTasks().named("shadowJar"));
                    FileCollection testRuntime = sourceSets.getByName(SourceSet.TEST_SOURCE_SET_NAME).getRuntimeClasspath();
                    test.setClasspath(testRuntime.minus(mainRuntime).plus(shadowConfig).plus(shadowJar));
                }
            });
        });
        configureImmutableCollectionsPatch(project);
    }

    private void configureImmutableCollectionsPatch(Project project) {
        String patchProject = ":test:immutable-collections-patch";
        if (project.findProject(patchProject) == null) {
            return; // build tests may not have this project, just skip
        }
        String configurationName = "immutableCollectionsPatch";
        FileCollection patchedFileCollection = project.getConfigurations()
            .create(configurationName, config -> config.setCanBeConsumed(false));
        var deps = project.getDependencies();
        deps.add(configurationName, deps.project(Map.of("path", patchProject, "configuration", "patch")));
        project.getTasks().withType(Test.class).matching(task -> task.getName().equals("test")).configureEach(test -> {
            test.getInputs().files(patchedFileCollection);
            test.systemProperty("tests.hackImmutableCollections", "true");
            test.getJvmArgumentProviders()
                .add(
                    () -> List.of(
                        "--patch-module=java.base=" + patchedFileCollection.getSingleFile() + "/java.base" + File.pathSeparator + "/Users/prdoyle/IdeaProjects/elasticsearch/libs/entitlement/bridge/build/distributions/elasticsearch-entitlement-bridge-9.1.0-SNAPSHOT.jar",
                        "--add-opens=java.base/java.util=ALL-UNNAMED"
                    )
                );
        });
    }
}
