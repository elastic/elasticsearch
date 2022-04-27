/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.util;

import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.UnknownTaskException;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.artifacts.ModuleDependency;
import org.gradle.api.artifacts.ProjectDependency;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceRegistration;
import org.gradle.api.services.BuildServiceRegistry;
import org.gradle.api.specs.Spec;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;
import org.gradle.plugins.ide.eclipse.model.EclipseModel;
import org.gradle.plugins.ide.idea.model.IdeaModel;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public abstract class GradleUtils {

    public static <T> Action<T> noop() {
        return t -> {};
    }

    public static SourceSetContainer getJavaSourceSets(Project project) {
        return project.getExtensions().getByType(JavaPluginExtension.class).getSourceSets();
    }

    public static void maybeConfigure(TaskContainer tasks, String name, Action<? super Task> config) {
        tasks.matching(t -> t.getName().equals(name)).configureEach(t -> config.execute(t));
    }

    public static <T extends Task> void maybeConfigure(
        TaskContainer tasks,
        String name,
        Class<? extends T> type,
        Action<? super T> config
    ) {
        tasks.withType(type).matching((Spec<T>) t -> t.getName().equals(name)).configureEach(task -> { config.execute(task); });
    }

    public static TaskProvider<?> findByName(TaskContainer tasks, String name) {
        TaskProvider<?> task;
        try {
            task = tasks.named(name);
        } catch (UnknownTaskException e) {
            return null;
        }

        return task;
    }

    @SuppressWarnings("unchecked")
    public static <T extends BuildService<?>> Provider<T> getBuildService(BuildServiceRegistry registry, String name) {
        BuildServiceRegistration<?, ?> registration = registry.getRegistrations().findByName(name);
        if (registration == null) {
            throw new GradleException("Unable to find build service with name '" + name + "'.");
        }

        return (Provider<T>) registration.getService();
    }

    /**
     * Add a source set and task of the same name that runs tests.
     * <p>
     * IDEs are also configured if setup, and the test task is added to check. The new test source
     * set extends from the normal test source set to allow sharing of utilities.
     *
     * @return A task provider for the newly created test task
     */
    public static TaskProvider<Test> addTestSourceSet(Project project, String sourceSetName) {
        project.getPluginManager().apply(JavaPlugin.class);

        // create our test source set and task
        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet testSourceSet = sourceSets.create(sourceSetName);
        TaskProvider<Test> testTask = project.getTasks().register(sourceSetName, Test.class);
        testTask.configure(task -> {
            task.setGroup(JavaBasePlugin.VERIFICATION_GROUP);
            task.setTestClassesDirs(testSourceSet.getOutput().getClassesDirs());
            task.setClasspath(testSourceSet.getRuntimeClasspath());
            // make the new test run after unit tests
            task.mustRunAfter(project.getTasks().named("test"));
        });

        Configuration testCompileConfig = project.getConfigurations().getByName(testSourceSet.getCompileClasspathConfigurationName());
        Configuration testRuntimeConfig = project.getConfigurations().getByName(testSourceSet.getRuntimeClasspathConfigurationName());
        testSourceSet.setCompileClasspath(testCompileConfig);
        testSourceSet.setRuntimeClasspath(project.getObjects().fileCollection().from(testSourceSet.getOutput(), testRuntimeConfig));

        extendSourceSet(project, SourceSet.MAIN_SOURCE_SET_NAME, sourceSetName);

        setupIdeForTestSourceSet(project, testSourceSet);

        // add to the check task
        project.getTasks().named(JavaBasePlugin.CHECK_TASK_NAME).configure(check -> check.dependsOn(testTask));

        return testTask;
    }

    public static void setupIdeForTestSourceSet(Project project, SourceSet testSourceSet) {
        // setup IDEs
        String runtimeClasspathName = testSourceSet.getRuntimeClasspathConfigurationName();
        Configuration runtimeClasspathConfiguration = project.getConfigurations().getByName(runtimeClasspathName);
        project.getPluginManager().withPlugin("idea", p -> {
            IdeaModel idea = project.getExtensions().getByType(IdeaModel.class);
            idea.getModule().setTestSourceDirs(testSourceSet.getJava().getSrcDirs());
            idea.getModule().getScopes().put(testSourceSet.getName(), Map.of("plus", List.of(runtimeClasspathConfiguration)));
        });
        project.getPluginManager().withPlugin("eclipse", p -> {
            EclipseModel eclipse = project.getExtensions().getByType(EclipseModel.class);
            List<SourceSet> eclipseSourceSets = new ArrayList<>();
            for (SourceSet old : eclipse.getClasspath().getSourceSets()) {
                eclipseSourceSets.add(old);
            }
            eclipseSourceSets.add(testSourceSet);
            eclipse.getClasspath().setSourceSets(project.getExtensions().getByType(SourceSetContainer.class));
            eclipse.getClasspath().getPlusConfigurations().add(runtimeClasspathConfiguration);
        });
    }

    /**
     * Extend the configurations of one source set from another.
     */
    public static void extendSourceSet(Project project, String parentSourceSetName, String childSourceSetName) {
        final List<Function<SourceSet, String>> configNameFunctions = Arrays.asList(
            SourceSet::getCompileOnlyConfigurationName,
            SourceSet::getImplementationConfigurationName,
            SourceSet::getRuntimeOnlyConfigurationName
        );
        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet parent = sourceSets.getByName(parentSourceSetName);
        SourceSet child = sourceSets.getByName(childSourceSetName);

        for (Function<SourceSet, String> configNameFunction : configNameFunctions) {
            String parentConfigName = configNameFunction.apply(parent);
            String childConfigName = configNameFunction.apply(child);
            Configuration parentConfig = project.getConfigurations().getByName(parentConfigName);
            Configuration childConfig = project.getConfigurations().getByName(childConfigName);
            childConfig.extendsFrom(parentConfig);
        }

        // tie this new test source set to the main and test source sets
        child.setCompileClasspath(project.getObjects().fileCollection().from(child.getCompileClasspath(), parent.getOutput()));
        child.setRuntimeClasspath(project.getObjects().fileCollection().from(child.getRuntimeClasspath(), parent.getOutput()));
    }

    /**
     * Extends one configuration from another and refreshes the classpath of a provided Test.
     * The Test parameter is only needed for eagerly defined test tasks.
     */
    public static void extendSourceSet(Project project, String parentSourceSetName, String childSourceSetName, TaskProvider<Test> test) {
        extendSourceSet(project, parentSourceSetName, childSourceSetName);
        if (test != null) {
            test.configure(t -> {
                SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
                SourceSet child = sourceSets.getByName(childSourceSetName);
                t.setClasspath(child.getRuntimeClasspath());
            });
        }
    }

    public static Dependency projectDependency(Project project, String projectPath, String projectConfig) {
        if (project.findProject(projectPath) == null) {
            throw new GradleException("no project [" + projectPath + "], project names: " + project.getRootProject().getAllprojects());
        }
        Map<String, Object> depConfig = new HashMap<>();
        depConfig.put("path", projectPath);
        depConfig.put("configuration", projectConfig);
        return project.getDependencies().project(depConfig);
    }

    /**
     * To calculate the project path from a task path without relying on Task#getProject() which is discouraged during
     * task execution time.
     */
    public static String getProjectPathFromTask(String taskPath) {
        int lastDelimiterIndex = taskPath.lastIndexOf(":");
        return lastDelimiterIndex == 0 ? ":" : taskPath.substring(0, lastDelimiterIndex);
    }

    public static boolean isModuleProject(String projectPath) {
        return projectPath.contains("modules:") || projectPath.startsWith(":x-pack:plugin");
    }

    public static void disableTransitiveDependencies(Configuration config) {
        config.getDependencies().all(dep -> {
            if (dep instanceof ModuleDependency
                && dep instanceof ProjectDependency == false
                && dep.getGroup().startsWith("org.elasticsearch") == false) {
                ((ModuleDependency) dep).setTransitive(false);
            }
        });
    }

    public static String projectPath(String taskPath) {
        return taskPath.lastIndexOf(':') == 0 ? ":" : taskPath.substring(0, taskPath.lastIndexOf(':'));
    }
}
