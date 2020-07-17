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
package org.elasticsearch.gradle.testfixtures;

import com.avast.gradle.dockercompose.ComposeExtension;
import com.avast.gradle.dockercompose.DockerComposePlugin;
import com.avast.gradle.dockercompose.ServiceInfo;
import com.avast.gradle.dockercompose.tasks.ComposeDown;
import com.avast.gradle.dockercompose.tasks.ComposePull;
import com.avast.gradle.dockercompose.tasks.ComposeUp;
import org.elasticsearch.gradle.SystemPropertyCommandLineArgumentProvider;
import org.elasticsearch.gradle.docker.DockerSupportPlugin;
import org.elasticsearch.gradle.docker.DockerSupportService;
import org.elasticsearch.gradle.info.BuildParams;
import org.elasticsearch.gradle.precommit.TestingConventionsTasks;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.Action;
import org.gradle.api.DefaultTask;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.function.BiConsumer;

public class TestFixturesPlugin implements Plugin<Project> {

    private static final Logger LOGGER = Logging.getLogger(TestFixturesPlugin.class);
    private static final String DOCKER_COMPOSE_THROTTLE = "dockerComposeThrottle";
    static final String DOCKER_COMPOSE_YML = "docker-compose.yml";

    @Override
    public void apply(Project project) {
        project.getRootProject().getPluginManager().apply(DockerSupportPlugin.class);

        TaskContainer tasks = project.getTasks();
        TestFixtureExtension extension = project.getExtensions().create("testFixtures", TestFixtureExtension.class, project);
        Provider<DockerComposeThrottle> dockerComposeThrottle = project.getGradle()
            .getSharedServices()
            .registerIfAbsent(DOCKER_COMPOSE_THROTTLE, DockerComposeThrottle.class, spec -> spec.getMaxParallelUsages().set(1));

        Provider<DockerSupportService> dockerSupport = GradleUtils.getBuildService(
            project.getGradle().getSharedServices(),
            DockerSupportPlugin.DOCKER_SUPPORT_SERVICE_NAME
        );

        ExtraPropertiesExtension ext = project.getExtensions().getByType(ExtraPropertiesExtension.class);
        File testfixturesDir = project.file("testfixtures_shared");
        ext.set("testFixturesDir", testfixturesDir);

        if (project.file(DOCKER_COMPOSE_YML).exists()) {
            project.getPluginManager().apply(BasePlugin.class);
            project.getPluginManager().apply(DockerComposePlugin.class);

            TaskProvider<Task> preProcessFixture = project.getTasks().register("preProcessFixture", t -> {
                t.getOutputs().dir(testfixturesDir);
                t.doFirst(t2 -> {
                    try {
                        Files.createDirectories(testfixturesDir.toPath());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                });
            });
            TaskProvider<Task> buildFixture = project.getTasks()
                .register("buildFixture", t -> t.dependsOn(preProcessFixture, tasks.named("composeUp")));

            TaskProvider<Task> postProcessFixture = project.getTasks().register("postProcessFixture", task -> {
                task.dependsOn(buildFixture);
                configureServiceInfoForTask(
                    task,
                    project,
                    false,
                    (name, port) -> task.getExtensions().getByType(ExtraPropertiesExtension.class).set(name, port)
                );
            });

            maybeSkipTask(dockerSupport, preProcessFixture);
            maybeSkipTask(dockerSupport, postProcessFixture);
            maybeSkipTask(dockerSupport, buildFixture);

            ComposeExtension composeExtension = project.getExtensions().getByType(ComposeExtension.class);
            composeExtension.setUseComposeFiles(Collections.singletonList(DOCKER_COMPOSE_YML));
            composeExtension.setRemoveContainers(true);
            composeExtension.setExecutable(
                project.file("/usr/local/bin/docker-compose").exists() ? "/usr/local/bin/docker-compose" : "/usr/bin/docker-compose"
            );

            tasks.named("composeUp").configure(t -> {
                // Avoid running docker-compose tasks in parallel in CI due to some issues on certain Linux distributions
                if (BuildParams.isCi()) {
                    t.usesService(dockerComposeThrottle);
                }
                t.mustRunAfter(preProcessFixture);
            });
            tasks.named("composePull").configure(t -> t.mustRunAfter(preProcessFixture));
            tasks.named("composeDown").configure(t -> t.doLast(t2 -> project.delete(testfixturesDir)));
        } else {
            project.afterEvaluate(spec -> {
                if (extension.fixtures.isEmpty()) {
                    // if only one fixture is used, that's this one, but without a compose file that's not a valid configuration
                    throw new IllegalStateException(
                        "No " + DOCKER_COMPOSE_YML + " found for " + project.getPath() + " nor does it use other fixtures."
                    );
                }
            });
        }

        extension.fixtures.matching(fixtureProject -> fixtureProject.equals(project) == false)
            .all(fixtureProject -> project.evaluationDependsOn(fixtureProject.getPath()));

        // Skip docker compose tasks if it is unavailable
        maybeSkipTasks(tasks, dockerSupport, Test.class);
        maybeSkipTasks(tasks, dockerSupport, getTaskClass("org.elasticsearch.gradle.test.RestIntegTestTask"));
        maybeSkipTasks(tasks, dockerSupport, TestingConventionsTasks.class);
        maybeSkipTasks(tasks, dockerSupport, getTaskClass("org.elasticsearch.gradle.test.AntFixture"));
        maybeSkipTasks(tasks, dockerSupport, ComposeUp.class);
        maybeSkipTasks(tasks, dockerSupport, ComposePull.class);
        maybeSkipTasks(tasks, dockerSupport, ComposeDown.class);

        tasks.withType(Test.class).configureEach(task -> extension.fixtures.all(fixtureProject -> {
            task.dependsOn(fixtureProject.getTasks().named("postProcessFixture"));
            task.finalizedBy(fixtureProject.getTasks().named("composeDown"));
            configureServiceInfoForTask(
                task,
                fixtureProject,
                true,
                (name, host) -> task.getExtensions().getByType(SystemPropertyCommandLineArgumentProvider.class).systemProperty(name, host)
            );
        }));

    }

    private void maybeSkipTasks(TaskContainer tasks, Provider<DockerSupportService> dockerSupport, Class<? extends DefaultTask> taskClass) {
        tasks.withType(taskClass).configureEach(t -> maybeSkipTask(dockerSupport, t));
    }

    private void maybeSkipTask(Provider<DockerSupportService> dockerSupport, TaskProvider<Task> task) {
        task.configure(t -> maybeSkipTask(dockerSupport, t));
    }

    private void maybeSkipTask(Provider<DockerSupportService> dockerSupport, Task task) {
        task.onlyIf(spec -> {
            boolean isComposeAvailable = dockerSupport.get().getDockerAvailability().isComposeAvailable;
            if (isComposeAvailable == false) {
                LOGGER.info("Task {} requires docker-compose but it is unavailable. Task will be skipped.", task.getPath());
            }
            return isComposeAvailable;
        });
    }

    private void configureServiceInfoForTask(
        Task task,
        Project fixtureProject,
        boolean enableFilter,
        BiConsumer<String, Integer> consumer
    ) {
        // Configure ports for the tests as system properties.
        // We only know these at execution time so we need to do it in doFirst
        task.doFirst(new Action<Task>() {
            @Override
            public void execute(Task theTask) {
                TestFixtureExtension extension = theTask.getProject().getExtensions().getByType(TestFixtureExtension.class);

                fixtureProject.getExtensions()
                    .getByType(ComposeExtension.class)
                    .getServicesInfos()
                    .entrySet()
                    .stream()
                    .filter(entry -> enableFilter == false || extension.isServiceRequired(entry.getKey(), fixtureProject.getPath()))
                    .forEach(entry -> {
                        String service = entry.getKey();
                        ServiceInfo infos = entry.getValue();
                        infos.getTcpPorts().forEach((container, host) -> {
                            String name = "test.fixtures." + service + ".tcp." + container;
                            theTask.getLogger().info("port mapping property: {}={}", name, host);
                            consumer.accept(name, host);
                        });
                        infos.getUdpPorts().forEach((container, host) -> {
                            String name = "test.fixtures." + service + ".udp." + container;
                            theTask.getLogger().info("port mapping property: {}={}", name, host);
                            consumer.accept(name, host);
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
