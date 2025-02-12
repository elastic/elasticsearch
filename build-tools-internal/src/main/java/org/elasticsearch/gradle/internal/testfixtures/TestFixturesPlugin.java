/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.testfixtures;

import com.avast.gradle.dockercompose.ComposeExtension;
import com.avast.gradle.dockercompose.DockerComposePlugin;
import com.avast.gradle.dockercompose.tasks.ComposeBuild;
import com.avast.gradle.dockercompose.tasks.ComposeDown;
import com.avast.gradle.dockercompose.tasks.ComposePull;
import com.avast.gradle.dockercompose.tasks.ComposeUp;

import org.elasticsearch.gradle.internal.docker.DockerSupportPlugin;
import org.elasticsearch.gradle.internal.docker.DockerSupportService;
import org.elasticsearch.gradle.internal.info.GlobalBuildInfoPlugin;
import org.elasticsearch.gradle.test.SystemPropertyCommandLineArgumentProvider;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Action;
import org.gradle.api.DefaultTask;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.file.FileSystemOperations;
import org.gradle.api.logging.LogLevel;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.EnumSet;
import java.util.function.BiConsumer;

import javax.inject.Inject;

import static org.elasticsearch.gradle.internal.util.ParamsUtils.loadBuildParams;

public class TestFixturesPlugin implements Plugin<Project> {

    private static final Logger LOGGER = Logging.getLogger(TestFixturesPlugin.class);
    private static final String DOCKER_COMPOSE_THROTTLE = "dockerComposeThrottle";
    static final String DOCKER_COMPOSE_YML = "docker-compose.yml";

    private final ProviderFactory providerFactory;

    @Inject
    public TestFixturesPlugin(ProviderFactory providerFactory) {
        this.providerFactory = providerFactory;
    }

    @Inject
    protected FileSystemOperations getFileSystemOperations() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void apply(Project project) {
        project.getRootProject().getPluginManager().apply(DockerSupportPlugin.class);
        project.getRootProject().getPlugins().apply(GlobalBuildInfoPlugin.class);
        var buildParams = loadBuildParams(project).get();

        TaskContainer tasks = project.getTasks();
        Provider<DockerComposeThrottle> dockerComposeThrottle = project.getGradle()
            .getSharedServices()
            .registerIfAbsent(DOCKER_COMPOSE_THROTTLE, DockerComposeThrottle.class, spec -> spec.getMaxParallelUsages().set(1));

        Provider<DockerSupportService> dockerSupport = GradleUtils.getBuildService(
            project.getGradle().getSharedServices(),
            DockerSupportPlugin.DOCKER_SUPPORT_SERVICE_NAME
        );

        ExtraPropertiesExtension ext = project.getExtensions().getByType(ExtraPropertiesExtension.class);
        File testFixturesDir = project.file("testfixtures_shared");
        ext.set("testFixturesDir", testFixturesDir);

        if (project.file(DOCKER_COMPOSE_YML).exists() == false) {
            // if only one fixture is used, that's this one, but without a compose file that's not a valid configuration
            throw new IllegalStateException("No " + DOCKER_COMPOSE_YML + " found for " + project.getPath() + ".");
        }
        project.getPluginManager().apply(BasePlugin.class);
        project.getPluginManager().apply(DockerComposePlugin.class);
        TaskProvider<TestFixtureTask> preProcessFixture = project.getTasks().register("preProcessFixture", TestFixtureTask.class, t -> {
            t.getFixturesDir().set(testFixturesDir);
            t.doFirst(task -> {
                try {
                    Files.createDirectories(testFixturesDir.toPath());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        });
        TaskProvider<Task> buildFixture = project.getTasks()
            .register("buildFixture", t -> t.dependsOn(preProcessFixture, tasks.named("composeUp")));

        TaskProvider<TestFixtureTask> postProcessFixture = project.getTasks()
            .register("postProcessFixture", TestFixtureTask.class, task -> {
                task.getFixturesDir().set(testFixturesDir);
                task.dependsOn(buildFixture);
            });

        maybeSkipTask(dockerSupport, preProcessFixture);
        maybeSkipTask(dockerSupport, postProcessFixture);
        maybeSkipTask(dockerSupport, buildFixture);

        ComposeExtension composeExtension = project.getExtensions().getByType(ComposeExtension.class);
        composeExtension.setProjectName(project.getName());
        composeExtension.getUseComposeFiles().addAll(Collections.singletonList(DOCKER_COMPOSE_YML));
        composeExtension.getRemoveContainers().set(true);
        composeExtension.getCaptureContainersOutput()
            .set(EnumSet.of(LogLevel.INFO, LogLevel.DEBUG).contains(project.getGradle().getStartParameter().getLogLevel()));
        composeExtension.getUseDockerComposeV2().set(false);
        composeExtension.getExecutable().set(this.providerFactory.provider(() -> {
            String composePath = dockerSupport.get().getDockerAvailability().dockerComposePath();
            LOGGER.debug("Docker Compose path: {}", composePath);
            return composePath != null ? composePath : "/usr/bin/docker-compose";
        }));

        tasks.withType(ComposeUp.class).named("composeUp").configure(t -> {
            // Avoid running docker-compose tasks in parallel in CI due to some issues on certain Linux distributions
            if (buildParams.getCi()) {
                t.usesService(dockerComposeThrottle);
                t.usesService(dockerSupport);
            }
            t.mustRunAfter(preProcessFixture);
            t.doLast(new Action<Task>() {
                @Override
                public void execute(Task task) {
                    dockerSupport.get().storeInfo(t.getServicesInfos());
                }
            });

        });
        tasks.named("composePull").configure(t -> t.mustRunAfter(preProcessFixture));
        tasks.named("composeDown").configure(t -> t.doLast(t2 -> getFileSystemOperations().delete(d -> d.delete(testFixturesDir))));

        // Skip docker compose tasks if it is unavailable
        maybeSkipTasks(tasks, dockerSupport, Test.class);
        maybeSkipTasks(tasks, dockerSupport, getTaskClass("org.elasticsearch.gradle.internal.test.RestIntegTestTask"));
        maybeSkipTasks(tasks, dockerSupport, getTaskClass("org.elasticsearch.gradle.internal.test.AntFixture"));
        maybeSkipTasks(tasks, dockerSupport, ComposeUp.class);
        maybeSkipTasks(tasks, dockerSupport, ComposeBuild.class);
        maybeSkipTasks(tasks, dockerSupport, ComposePull.class);
        maybeSkipTasks(tasks, dockerSupport, ComposeDown.class);

        tasks.withType(Test.class).configureEach(testTask -> {
            testTask.dependsOn(postProcessFixture);
            testTask.finalizedBy(tasks.named("composeDown"));
            SystemPropertyCommandLineArgumentProvider sysArgumentsProvider = testTask.getExtensions()
                .getByType(SystemPropertyCommandLineArgumentProvider.class);
            configureServiceInfoForTask(testTask, dockerSupport, (name, host) -> sysArgumentsProvider.systemProperty(name, host));
        });
    }

    private void maybeSkipTasks(TaskContainer tasks, Provider<DockerSupportService> dockerSupport, Class<? extends DefaultTask> taskClass) {
        tasks.withType(taskClass).configureEach(t -> maybeSkipTask(dockerSupport, t));
    }

    private void maybeSkipTask(Provider<DockerSupportService> dockerSupport, TaskProvider<? extends Task> task) {
        task.configure(t -> maybeSkipTask(dockerSupport, t));
    }

    private void maybeSkipTask(Provider<DockerSupportService> dockerSupport, Task task) {
        task.onlyIf("docker compose is available", spec -> {
            boolean isComposeAvailable = dockerSupport.get().getDockerAvailability().isComposeAvailable();
            if (isComposeAvailable == false) {
                LOGGER.info("Task {} requires docker-compose but it is unavailable. Task will be skipped.", task.getPath());
            }
            return isComposeAvailable;
        });
    }

    private void configureServiceInfoForTask(
        Task task,
        Provider<DockerSupportService> dockerSupportServiceProvider,
        BiConsumer<String, Integer> consumer
    ) {
        // Configure ports for the tests as system properties.
        // We only know these at execution time so we need to do it in doFirst
        task.usesService(dockerSupportServiceProvider);
        task.doFirst(new Action<Task>() {
            @Override
            public void execute(Task theTask) {
                dockerSupportServiceProvider.get().getTcpPorts().entrySet().stream().forEach(entry -> {
                    String service = entry.getKey();
                    entry.getValue().entrySet().stream().forEach(portMapping -> {
                        String name = "test.fixtures." + service + ".tcp." + portMapping.getKey();
                        theTask.getLogger().info("port mapping property: {}={}", name, portMapping.getValue());
                        consumer.accept(name, portMapping.getValue());
                    });
                });
                dockerSupportServiceProvider.get().getUdpPorts().entrySet().stream().forEach(entry -> {
                    String service = entry.getKey();
                    entry.getValue().entrySet().stream().forEach(portMapping -> {
                        String name = "test.fixtures." + service + ".udp." + portMapping.getKey();
                        theTask.getLogger().info("port mapping property: {}={}", name, portMapping.getValue());
                        consumer.accept(name, portMapping.getValue());
                    });
                });
            }
        });

    }

    @SuppressWarnings("unchecked")
    private Class<? extends DefaultTask> getTaskClass(String type) {
        Class<?> aClass;
        try {
            aClass = Class.forName(type);
            if (DefaultTask.class.isAssignableFrom(aClass) == false) {
                throw new IllegalArgumentException("Not a task type: " + type);
            }
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException("No such task: " + type);
        }
        return (Class<? extends DefaultTask>) aClass;
    }

}
