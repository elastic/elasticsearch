/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.esql;

import groovy.lang.Closure;

import org.elasticsearch.gradle.Version;
import org.elasticsearch.gradle.internal.BwcVersions;
import org.elasticsearch.gradle.internal.info.BuildParameterExtension;
import org.elasticsearch.gradle.testclusters.StandaloneRestIntegTestTask;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskProvider;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;

/**
 * Registers the owner-suite BWC tasks for ES|QL data-source formats.
 *
 * <p>Each compatible version gets one task per coordinator direction. This plugin owns only
 * Gradle concerns: versions, source sets, class filters, coordinator modes, exclusions, and
 * mixed-cluster system properties. The selected test class owns its typed backend/codec matrix
 * policy; keeping that policy test-side lets future backends and codecs remain invisible to the
 * Gradle DSL.
 */
public class EsqlDataSourceBwcPlugin implements Plugin<Project> {

    private static final String EXTENSION_NAME = "esqlDataSourceBwc";

    @Override
    public void apply(Project project) {
        // The opt-outs below are read while elasticsearch.bwc-test is being applied, so a project that
        // applied it first would silently keep the defaults and disable its own current-version tasks.
        if (project.getPluginManager().hasPlugin("elasticsearch.bwc-test")) {
            throw new IllegalStateException(
                "elasticsearch.esql-datasource-bwc must be applied before elasticsearch.bwc-test in " + project.getPath()
            );
        }

        // The generic BWC plugin keeps these defaults enabled for existing projects. This
        // convention owns a separate source set and must preserve current task state. The
        // distribution opt-out is not cosmetic: the owning suites' own csvSpecTests and
        // javaRestTest tasks already call usesDefaultDistribution, and the generic plugin applies
        // it to every standalone REST task in the project, which registers the default_distro
        // inputs twice and fails the build. The versioned tasks below request it individually.
        project.getExtensions().getExtraProperties().set("bwcTestDisableBaseTestTask", false);
        project.getExtensions().getExtraProperties().set("bwcTestDisableBaseJavaRestTestTask", false);
        project.getExtensions().getExtraProperties().set("bwcTestUseJavaRestTestSourceSet", false);
        project.getExtensions().getExtraProperties().set("bwcTestApplyDefaultDistribution", false);

        EsqlDataSourceBwcExtension extension = project.getExtensions().create(EXTENSION_NAME, EsqlDataSourceBwcExtension.class);
        extension.getSourceSetName().convention(EsqlCsvSpecTestsPlugin.SOURCE_SET_NAME);
        extension.getClassFilters().convention(List.of());
        extension.getCoordinatorModes().convention(List.of("old", "current"));
        extension.getSystemProperties().convention(Map.of());

        project.getPluginManager().apply("elasticsearch.bwc-test");
        project.getPluginManager().withPlugin("elasticsearch.internal-java-rest-test", ignored -> configureTasks(project, extension));
    }

    private static void configureTasks(Project project, EsqlDataSourceBwcExtension extension) {
        BuildParameterExtension buildParams = loadBuildParams(project).get();
        BwcVersions bwcVersions = buildParams.getBwcVersions();

        TaskProvider<Task> all = registerAggregate(project, "esqlDataSourceBwc", "Runs ES|QL data-source BWC coverage.");

        for (Version version : bwcVersions.getWireCompatible()) {
            if (version.equals(bwcVersions.getCurrentVersion())) {
                continue;
            }

            List<TaskProvider<StandaloneRestIntegTestTask>> tasks = new ArrayList<>();
            for (String coordinator : List.of("current", "old")) {
                tasks.add(registerTestTask(project, extension, buildParams, bwcVersions, version, coordinator));
            }

            TaskProvider<Task> versionAggregate = registerAggregate(
                project,
                "v" + version + "#esqlDataSourceBwc",
                "Runs ES|QL data-source BWC coverage for " + version + "."
            );
            versionAggregate.configure(task -> task.dependsOn(tasks));
            all.configure(task -> task.dependsOn(versionAggregate));

            String standardBwcTaskName = "v" + version + "#bwcTest";
            TaskProvider<Task> standardBwc = project.getTasks().getNames().contains(standardBwcTaskName)
                ? project.getTasks().named(standardBwcTaskName)
                : project.getTasks().register(standardBwcTaskName);
            standardBwc.configure(task -> task.dependsOn(versionAggregate));
        }
    }

    private static TaskProvider<StandaloneRestIntegTestTask> registerTestTask(
        Project project,
        EsqlDataSourceBwcExtension extension,
        BuildParameterExtension buildParams,
        BwcVersions bwcVersions,
        Version version,
        String coordinator
    ) {
        String coordinatorName = coordinator.substring(0, 1).toUpperCase(Locale.ROOT) + coordinator.substring(1);
        String taskName = "v" + version + "#esqlDataSourceBwc" + coordinatorName + "Coordinator";
        return project.getTasks().register(taskName, StandaloneRestIntegTestTask.class, task -> {
            validate(extension);
            if (version.before(extension.getMinimumVersion().get()) || normalizedCoordinators(extension).contains(coordinator) == false) {
                task.setEnabled(false);
                return;
            }
            SourceSet sourceSet = project.getExtensions()
                .getByType(JavaPluginExtension.class)
                .getSourceSets()
                .getByName(extension.getSourceSetName().get());
            useBwcDistribution(task, version);
            // The mixed cluster runs current-version nodes alongside the old ones, and the
            // data-source plugins ship only in the default distribution.
            useDefaultDistribution(task, "mixed-version clusters start current-version nodes");
            task.setDescription("Runs ES|QL data-source tests with the " + coordinator + " coordinator against " + version + ".");
            task.setGroup("verification");
            // The owning suites gate their current-version runners on the build type because the
            // data-source surface is snapshot-only. Without the same gate here, check ->
            // bwcTestSnapshots would pull these suites into the release-build lane that the owners
            // deliberately opt out of.
            task.setEnabled(buildParams.getSnapshotBuild());
            task.setMaxParallelForks(1);
            task.setTestClassesDirs(sourceSet.getOutput().getClassesDirs());
            task.setClasspath(sourceSet.getRuntimeClasspath());
            task.dependsOn(sourceSet.getOutput());

            for (String classFilter : extension.getClassFilters().get()) {
                task.getFilter().includeTestsMatching(classFilter);
            }
            for (EsqlDataSourceBwcExtension.Exclusion exclusion : extension.getExclusions()) {
                task.getFilter().excludeTestsMatching(exclusion.pattern());
            }

            boolean oldSnapshot = bwcVersions.getReleased().contains(version) == false && effectiveBwcSnapshot();
            // tests.bwc is deliberately absent: elasticsearch.bwc-test already declares it for every
            // v*#* test task, and does so as a non-input property so it cannot affect up-to-date checks.
            task.systemProperty("tests.old_cluster_version", version.toString());
            task.systemProperty("tests.esql.datasource.bwc", "true");
            task.systemProperty("tests.esql.datasource.coordinator", coordinator);
            task.systemProperty("tests.esql.datasource.current_snapshot", buildParams.getSnapshotBuild().toString());
            task.systemProperty("tests.esql.datasource.old_snapshot", Boolean.toString(oldSnapshot));
            extension.getSystemProperties().get().forEach(task::systemProperty);
        });
    }

    private static void useBwcDistribution(StandaloneRestIntegTestTask task, Version version) {
        callExtensionMethod(task, "usesBwcDistribution", version);
    }

    private static void useDefaultDistribution(StandaloneRestIntegTestTask task, String reason) {
        callExtensionMethod(task, "usesDefaultDistribution", reason);
    }

    /**
     * Invokes one of the distribution-selection methods that the REST test plugins add to a task as
     * a Groovy closure extra property, since they have no Java-visible interface.
     */
    private static void callExtensionMethod(StandaloneRestIntegTestTask task, String name, Object argument) {
        Object method = task.getExtensions().getExtraProperties().get(name);
        if (method instanceof Closure<?> closure) {
            closure.call(argument);
            return;
        }
        throw new IllegalStateException("Standalone REST task is missing " + name);
    }

    private static boolean effectiveBwcSnapshot() {
        String value = System.getProperty("tests.bwc.snapshot", "true");
        if ("true".equals(value) == false && "false".equals(value) == false) {
            throw new IllegalArgumentException("tests.bwc.snapshot must be true or false but was [" + value + "]");
        }
        return Boolean.parseBoolean(value);
    }

    private static TaskProvider<Task> registerAggregate(Project project, String name, String description) {
        TaskProvider<Task> task = project.getTasks().getNames().contains(name)
            ? project.getTasks().named(name)
            : project.getTasks().register(name);
        task.configure(t -> {
            t.setDescription(description);
            t.setGroup("verification");
        });
        return task;
    }

    private static List<String> normalizedCoordinators(EsqlDataSourceBwcExtension extension) {
        Set<String> result = new HashSet<>();
        for (String coordinator : extension.getCoordinatorModes().get()) {
            String normalized = coordinator.toLowerCase(Locale.ROOT);
            if ("old".equals(normalized) == false && "current".equals(normalized) == false) {
                throw new IllegalArgumentException("Coordinator must be [old] or [current] but was [" + coordinator + "]");
            }
            result.add(normalized);
        }
        if (result.isEmpty()) {
            throw new IllegalArgumentException("At least one coordinator mode is required");
        }
        return result.stream().sorted().toList();
    }

    private static void validate(EsqlDataSourceBwcExtension extension) {
        if (extension.getMinimumVersion().isPresent() == false) {
            throw new IllegalArgumentException("esqlDataSourceBwc.minimumVersion is required");
        }
        for (EsqlDataSourceBwcExtension.Exclusion exclusion : extension.getExclusions()) {
            if (exclusion.pattern() == null || exclusion.pattern().isBlank()) {
                throw new IllegalArgumentException("A BWC exclusion must have a class pattern");
            }
            if (exclusion.owner() == null || exclusion.owner().isBlank()) {
                throw new IllegalArgumentException("BWC exclusion [" + exclusion.pattern() + "] must have an owner");
            }
            if (exclusion.reason() == null || exclusion.reason().isBlank()) {
                throw new IllegalArgumentException("BWC exclusion [" + exclusion.pattern() + "] must have a reason");
            }
        }
        normalizedCoordinators(extension);
    }
}
