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
import com.avast.gradle.dockercompose.tasks.ComposeUp;
import org.elasticsearch.gradle.OS;
import org.elasticsearch.gradle.SystemPropertyCommandLineArgumentProvider;
import org.elasticsearch.gradle.info.BuildParams;
import org.elasticsearch.gradle.precommit.TestingConventionsTasks;
import org.gradle.api.Action;
import org.gradle.api.DefaultTask;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskContainer;
import org.gradle.api.tasks.testing.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.function.BiConsumer;

public class TestFixturesPlugin implements Plugin<Project> {

    private static final String DOCKER_COMPOSE_THROTTLE = "dockerComposeThrottle";
    static final String DOCKER_COMPOSE_YML = "docker-compose.yml";

    @Override
    public void apply(Project project) {
        TaskContainer tasks = project.getTasks();

        TestFixtureExtension extension = project.getExtensions().create("testFixtures", TestFixtureExtension.class, project);
        Provider<DockerComposeThrottle> dockerComposeThrottle = project.getGradle()
            .getSharedServices()
            .registerIfAbsent(DOCKER_COMPOSE_THROTTLE, DockerComposeThrottle.class, spec -> spec.getMaxParallelUsages().set(1));

        ExtraPropertiesExtension ext = project.getExtensions().getByType(ExtraPropertiesExtension.class);
        File testfixturesDir = project.file("testfixtures_shared");
        ext.set("testFixturesDir", testfixturesDir);

        if (project.file(DOCKER_COMPOSE_YML).exists()) {
            Task buildFixture = project.getTasks().create("buildFixture");
            Task pullFixture = project.getTasks().create("pullFixture");
            Task preProcessFixture = project.getTasks().create("preProcessFixture");
            preProcessFixture.doFirst((task) -> {
                try {
                    Files.createDirectories(testfixturesDir.toPath());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
            preProcessFixture.getOutputs().dir(testfixturesDir);
            buildFixture.dependsOn(preProcessFixture);
            pullFixture.dependsOn(preProcessFixture);
            Task postProcessFixture = project.getTasks().create("postProcessFixture");
            postProcessFixture.dependsOn(buildFixture);
            preProcessFixture.onlyIf(spec -> buildFixture.getEnabled());
            postProcessFixture.onlyIf(spec -> buildFixture.getEnabled());

            if (dockerComposeSupported() == false) {
                preProcessFixture.setEnabled(false);
                postProcessFixture.setEnabled(false);
                buildFixture.setEnabled(false);
                pullFixture.setEnabled(false);
            } else {
                project.getPluginManager().apply(BasePlugin.class);
                project.getPluginManager().apply(DockerComposePlugin.class);
                ComposeExtension composeExtension = project.getExtensions().getByType(ComposeExtension.class);
                composeExtension.setUseComposeFiles(Collections.singletonList(DOCKER_COMPOSE_YML));
                composeExtension.setRemoveContainers(true);
                composeExtension.setExecutable(
                    project.file("/usr/local/bin/docker-compose").exists() ? "/usr/local/bin/docker-compose" : "/usr/bin/docker-compose"
                );

                buildFixture.dependsOn(tasks.named("composeUp"));
                pullFixture.dependsOn(tasks.named("composePull"));
                tasks.named("composeUp").configure(t -> {
                    // Avoid running docker-compose tasks in parallel in CI due to some issues on certain Linux distributions
                    if (BuildParams.isCi()) {
                        t.usesService(dockerComposeThrottle);
                    }
                    t.mustRunAfter(preProcessFixture);
                });
                tasks.named("composePull").configure(t -> t.mustRunAfter(preProcessFixture));
                tasks.named("composeDown").configure(t -> t.doLast(t2 -> project.delete(testfixturesDir)));

                configureServiceInfoForTask(
                    postProcessFixture,
                    project,
                    false,
                    (name, port) -> postProcessFixture.getExtensions().getByType(ExtraPropertiesExtension.class).set(name, port)
                );
            }
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

        conditionTaskByType(tasks, extension, Test.class);
        conditionTaskByType(tasks, extension, getTaskClass("org.elasticsearch.gradle.test.RestIntegTestTask"));
        conditionTaskByType(tasks, extension, TestingConventionsTasks.class);
        conditionTaskByType(tasks, extension, ComposeUp.class);

        if (dockerComposeSupported() == false) {
            project.getLogger()
                .info(
                    "Tests for {} require docker-compose at /usr/local/bin/docker-compose or /usr/bin/docker-compose "
                        + "but none could be found so these will be skipped",
                    project.getPath()
                );
            return;
        }

        tasks.withType(Test.class, task -> extension.fixtures.all(fixtureProject -> {
            fixtureProject.getTasks().matching(it -> it.getName().equals("buildFixture")).all(task::dependsOn);
            fixtureProject.getTasks().matching(it -> it.getName().equals("composeDown")).all(task::finalizedBy);
            configureServiceInfoForTask(
                task,
                fixtureProject,
                true,
                (name, host) -> task.getExtensions().getByType(SystemPropertyCommandLineArgumentProvider.class).systemProperty(name, host)
            );
            task.dependsOn(fixtureProject.getTasks().getByName("postProcessFixture"));
        }));

    }

    private void conditionTaskByType(TaskContainer tasks, TestFixtureExtension extension, Class<? extends DefaultTask> taskClass) {
        tasks.withType(taskClass)
            .configureEach(
                task -> task.onlyIf(
                    spec -> extension.fixtures.stream()
                        .anyMatch(fixtureProject -> fixtureProject.getTasks().getByName("buildFixture").getEnabled() == false) == false
                )
            );
    }

    private void configureServiceInfoForTask(
        Task task,
        Project fixtureProject,
        boolean enableFilter,
        BiConsumer<String, Integer> consumer
    ) {
        // Configure ports for the tests as system properties.
        // We only know these at execution time so we need to do it in doFirst
        TestFixtureExtension extension = task.getProject().getExtensions().getByType(TestFixtureExtension.class);
        task.doFirst(new Action<Task>() {
            @Override
            public void execute(Task theTask) {
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

    public static boolean dockerComposeSupported() {
        if (OS.current().equals(OS.WINDOWS)) {
            return false;
        }
        final boolean hasDockerCompose = (new File("/usr/local/bin/docker-compose")).exists()
            || (new File("/usr/bin/docker-compose").exists());
        return hasDockerCompose && Boolean.parseBoolean(System.getProperty("tests.fixture.enabled", "true"));
    }

    private void disableTaskByType(TaskContainer tasks, Class<? extends Task> type) {
        tasks.withType(type, task -> task.setEnabled(false));
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
