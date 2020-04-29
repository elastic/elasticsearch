/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle.util;

import org.elasticsearch.gradle.ElasticsearchJavaPlugin;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.PolymorphicDomainObjectContainer;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.UnknownTaskException;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.plugins.JavaBasePlugin;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.provider.Provider;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceRegistration;
import org.gradle.api.services.BuildServiceRegistry;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;
import org.gradle.plugins.ide.eclipse.model.EclipseModel;
import org.gradle.plugins.ide.idea.model.IdeaModel;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

public abstract class GradleUtils {

    public static <T> Action<T> noop() {
        return t -> {};
    }

    public static SourceSetContainer getJavaSourceSets(Project project) {
        return project.getConvention().getPlugin(JavaPluginConvention.class).getSourceSets();
    }

    public static <T> T maybeCreate(NamedDomainObjectContainer<T> collection, String name) {
        return Optional.ofNullable(collection.findByName(name)).orElse(collection.create(name));
    }

    public static <T> T maybeCreate(NamedDomainObjectContainer<T> collection, String name, Action<T> action) {
        return Optional.ofNullable(collection.findByName(name)).orElseGet(() -> {
            T result = collection.create(name);
            action.execute(result);
            return result;
        });

    }

    public static <T> T maybeCreate(PolymorphicDomainObjectContainer<T> collection, String name, Class<T> type, Action<T> action) {
        return Optional.ofNullable(collection.findByName(name)).orElseGet(() -> {
            T result = collection.create(name, type);
            action.execute(result);
            return result;
        });

    }

    public static <T extends Task> TaskProvider<T> maybeRegister(TaskContainer tasks, String name, Class<T> clazz, Action<T> action) {
        try {
            return tasks.named(name, clazz);
        } catch (UnknownTaskException e) {
            return tasks.register(name, clazz, action);
        }
    }

    public static void maybeConfigure(TaskContainer tasks, String name, Action<? super Task> config) {
        TaskProvider<?> task;
        try {
            task = tasks.named(name);
        } catch (UnknownTaskException e) {
            return;
        }

        task.configure(config);
    }

    public static <T extends Task> void maybeConfigure(
        TaskContainer tasks,
        String name,
        Class<? extends T> type,
        Action<? super T> config
    ) {
        tasks.withType(type).configureEach(task -> {
            if (task.getName().equals(name)) {
                config.execute(task);
            }
        });
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
     *
     * IDEs are also configured if setup, and the test task is added to check. The new test source
     * set extends from the normal test source set to allow sharing of utilities.
     */
    public static void addTestSourceSet(Project project, String sourceSetName) {
        project.getPluginManager().apply(ElasticsearchJavaPlugin.class);

        // create our test source set and task
        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet extraTestSourceSet = sourceSets.create(sourceSetName);
        TaskProvider<Test> testTask = project.getTasks().register(sourceSetName, Test.class);
        testTask.configure(task -> {
            task.setGroup(JavaBasePlugin.VERIFICATION_GROUP);
            task.setTestClassesDirs(extraTestSourceSet.getOutput().getClassesDirs());
            task.setClasspath(extraTestSourceSet.getRuntimeClasspath());
        });
        SourceSet mainSourceSet = sourceSets.getByName("main");
        SourceSet testSourceSet = sourceSets.getByName("test");

        extendConfiguration(project, testSourceSet, extraTestSourceSet, SourceSet::getCompileConfigurationName);
        extendConfiguration(project, testSourceSet, extraTestSourceSet, SourceSet::getImplementationConfigurationName);
        extendConfiguration(project, testSourceSet, extraTestSourceSet, SourceSet::getRuntimeConfigurationName);
        extendConfiguration(project, testSourceSet, extraTestSourceSet, SourceSet::getRuntimeOnlyConfigurationName);

        // tie this new test source set to the main and test source sets
        Configuration extraTestCompileConfig = project.getConfigurations()
            .getByName(extraTestSourceSet.getCompileClasspathConfigurationName());
        Configuration extraTestRuntimeConfig = project.getConfigurations()
            .getByName(extraTestSourceSet.getRuntimeClasspathConfigurationName());
        extraTestSourceSet.setCompileClasspath(
            project.getObjects().fileCollection().from(mainSourceSet.getOutput(), testSourceSet.getOutput(), extraTestCompileConfig)
        );
        extraTestSourceSet.setRuntimeClasspath(
            project.getObjects()
                .fileCollection()
                .from(extraTestSourceSet.getOutput(), mainSourceSet.getOutput(), testSourceSet.getOutput(), extraTestRuntimeConfig)
        );

        // setup IDEs
        String runtimeClasspathName = extraTestSourceSet.getRuntimeClasspathConfigurationName();
        Configuration runtimeClasspathConfiguration = project.getConfigurations().getByName(runtimeClasspathName);
        project.getPluginManager().withPlugin("idea", p -> {
            IdeaModel idea = project.getExtensions().getByType(IdeaModel.class);
            idea.getModule().setTestSourceDirs(extraTestSourceSet.getJava().getSrcDirs());
            idea.getModule().getScopes().put("TEST", Map.of("plus", List.of(runtimeClasspathConfiguration)));
        });
        project.getPluginManager().withPlugin("eclipse", p -> {
            EclipseModel eclipse = project.getExtensions().getByType(EclipseModel.class);
            eclipse.getClasspath().setSourceSets(List.of(extraTestSourceSet));
            eclipse.getClasspath().getPlusConfigurations().add(runtimeClasspathConfiguration);
        });

        // add to the check task
        project.getTasks().named(JavaBasePlugin.CHECK_TASK_NAME).configure(check -> check.dependsOn(testTask));
    }

    private static void extendConfiguration(Project project, SourceSet parent, SourceSet child, Function<SourceSet, String> configName) {
        String parentConfigName = configName.apply(parent);
        String childConfigName = configName.apply(child);
        Configuration parentConfig = project.getConfigurations().getByName(parentConfigName);
        Configuration childConfig = project.getConfigurations().getByName(childConfigName);
        childConfig.extendsFrom(parentConfig);
    }
}
