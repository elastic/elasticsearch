/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal;

import com.github.jengelman.gradle.plugins.shadow.ShadowBasePlugin;
import org.elasticsearch.gradle.OS;
import org.elasticsearch.gradle.internal.test.SimpleCommandLineArgumentProvider;
import org.elasticsearch.gradle.test.GradleTestPolicySetupPlugin;
import org.elasticsearch.gradle.test.SystemPropertyCommandLineArgumentProvider;
import org.elasticsearch.gradle.internal.info.BuildParams;
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.elasticsearch.gradle.internal.test.ErrorReportingTestListener;
import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.gradle.api.Action;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.testing.Test;

import java.io.File;
import java.util.Map;

import static org.elasticsearch.gradle.util.FileUtils.mkdirs;
import static org.elasticsearch.gradle.util.GradleUtils.maybeConfigure;

/**
 * Applies commonly used settings to all Test tasks in the project
 */
public class ElasticsearchTestBasePlugin implements Plugin<Project> {

    @Override
    public void apply(Project project) {
        project.getPluginManager().apply(GradleTestPolicySetupPlugin.class);
        // for fips mode check
        project.getRootProject().getPluginManager().apply(GlobalBuildInfoPlugin.class);
        // Default test task should run only unit tests
        maybeConfigure(project.getTasks(), "test", Test.class, task -> task.include("**/*Tests.class"));

        // none of this stuff is applicable to the `:buildSrc` project tests
        File heapdumpDir = new File(project.getBuildDir(), "heapdump");

        project.getTasks().withType(Test.class).configureEach(test -> {
            File testOutputDir = new File(test.getReports().getJunitXml().getOutputLocation().getAsFile().get(), "output");

            ErrorReportingTestListener listener = new ErrorReportingTestListener(test.getTestLogging(), test.getLogger(), testOutputDir);
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
                    test.systemProperty("java.locale.providers", "SPI,COMPAT");
                }
            });
            test.getJvmArgumentProviders().add(nonInputProperties);
            test.getExtensions().add("nonInputProperties", nonInputProperties);

            test.setWorkingDir(project.file(project.getBuildDir() + "/testrun/" + test.getName()));
            test.setMaxParallelForks(Integer.parseInt(System.getProperty("tests.jvms", BuildParams.getDefaultParallel().toString())));

            test.exclude("**/*$*.class");

            test.jvmArgs(
                "-Xmx" + System.getProperty("tests.heap.size", "512m"),
                "-Xms" + System.getProperty("tests.heap.size", "512m"),
                "--illegal-access=deny",
                // TODO: only open these for mockito when it is modularized
                "--add-opens=java.base/java.security.cert=ALL-UNNAMED",
                "--add-opens=java.base/java.nio.channels=ALL-UNNAMED",
                "--add-opens=java.base/java.net=ALL-UNNAMED",
                "--add-opens=java.base/javax.net.ssl=ALL-UNNAMED",
                "--add-opens=java.base/java.nio.file=ALL-UNNAMED",
                "--add-opens=java.base/java.time=ALL-UNNAMED",
                "--add-opens=java.management/java.lang.management=ALL-UNNAMED",
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
                nonInputProperties.systemProperty("tests.seed", BuildParams.getTestSeed());
            } else {
                test.systemProperty("tests.seed", BuildParams.getTestSeed());
            }

            // don't track these as inputs since they contain absolute paths and break cache relocatability
            File gradleUserHome = project.getGradle().getGradleUserHomeDir();
            nonInputProperties.systemProperty("gradle.user.home", gradleUserHome);
            // we use 'temp' relative to CWD since this is per JVM and tests are forbidden from writing to CWD
            nonInputProperties.systemProperty("java.io.tmpdir", test.getWorkingDir().toPath().resolve("temp"));

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
                test.systemProperty("tests.timeoutSuite", "2400000!");
            }

            /*
             *  If this project builds a shadow JAR than any unit tests should test against that artifact instead of
             *  compiled class output and dependency jars. This better emulates the runtime environment of consumers.
             */
            project.getPluginManager().withPlugin("com.github.johnrengelman.shadow", p -> {
                if (test.getName().equals(JavaPlugin.TEST_TASK_NAME)) {
                    // Remove output class files and any other dependencies from the test classpath, since the shadow JAR includes these
                    SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
                    FileCollection mainRuntime = sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME).getRuntimeClasspath();
                    // Add any "shadow" dependencies. These are dependencies that are *not* bundled into the shadow JAR
                    Configuration shadowConfig = project.getConfigurations().getByName(ShadowBasePlugin.getCONFIGURATION_NAME());
                    // Add the shadow JAR artifact itself
                    FileCollection shadowJar = project.files(project.getTasks().named("shadowJar"));
                    FileCollection testRuntime = sourceSets.getByName(SourceSet.TEST_SOURCE_SET_NAME).getRuntimeClasspath();
                    test.setClasspath(testRuntime.minus(mainRuntime).plus(shadowConfig).plus(shadowJar));
                }
            });
        });
    }
}
