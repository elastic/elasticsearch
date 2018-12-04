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
import org.elasticsearch.gradle.precommit.JarHellTask;
import org.elasticsearch.gradle.precommit.ThirdPartyAuditTask;
import org.gradle.api.DefaultTask;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.BasePlugin;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskContainer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;

public class TestFixturesPlugin implements Plugin<Project> {

    static final String DOCKER_COMPOSE_YML = "docker-compose.yml";

    @Override
    public void apply(Project project) {
        TaskContainer tasks = project.getTasks();

        TestFixtureExtension extension = project.getExtensions().create(
            "testFixtures", TestFixtureExtension.class, project
        );

        if (project.file(DOCKER_COMPOSE_YML).exists()) {
            // convenience boilerplate with build plugin
            // Can't reference tasks that are implemented in Groovy, use reflection  instead
            disableTaskByType(tasks, getTaskClass("org.elasticsearch.gradle.precommit.LicenseHeadersTask"));
            disableTaskByType(tasks, getTaskClass("com.carrotsearch.gradle.junit4.RandomizedTestingTask"));
            disableTaskByType(tasks, ThirdPartyAuditTask.class);
            disableTaskByType(tasks, JarHellTask.class);

            if (dockerComposeSupported(project) == false) {
                return;
            }

            project.apply(spec -> spec.plugin(BasePlugin.class));
            project.apply(spec -> spec.plugin(DockerComposePlugin.class));
            ComposeExtension composeExtension = project.getExtensions().getByType(ComposeExtension.class);
            composeExtension.setUseComposeFiles(Collections.singletonList(DOCKER_COMPOSE_YML));
            composeExtension.setRemoveContainers(true);
            composeExtension.setExecutable(
                project.file("/usr/local/bin/docker-compose").exists() ?
                    "/usr/local/bin/docker-compose" : "/usr/bin/docker-compose"
            );

            project.getTasks().getByName("clean").dependsOn("composeDown");
        } else {
            if (dockerComposeSupported(project) == false) {
                project.getLogger().warn(
                    "Tests for {} require docker-compose at /usr/local/bin/docker-compose or /usr/bin/docker-compose " +
                        "but none could not be found so these will be skipped", project.getPath()
                );
                tasks.withType(getTaskClass("com.carrotsearch.gradle.junit4.RandomizedTestingTask"), task ->
                    task.setEnabled(false)
                );
                return;
            }
            tasks.withType(getTaskClass("com.carrotsearch.gradle.junit4.RandomizedTestingTask"), task ->
                extension.fixtures.all(fixtureProject -> {
                    task.dependsOn(fixtureProject.getTasks().getByName("composeUp"));
                    task.finalizedBy(fixtureProject.getTasks().getByName("composeDown"));
                    // Configure ports for the tests as system properties.
                    // We only know these at execution time so we need to do it in doFirst
                    task.doFirst(it ->
                        fixtureProject.getExtensions().getByType(ComposeExtension.class).getServicesInfos()
                            .forEach((service, infos) ->
                                infos.getPorts()
                                    .forEach((container, host) -> setSystemProperty(
                                        it,
                                        "test.fixtures." + fixtureProject.getName() + "." + service + "." + container,
                                        host
                                    ))
                            ));
                }));
        }
    }

    @Input
    public boolean dockerComposeSupported(Project project) {
        // Don't look for docker-compose on the PATH yet that would pick up on Windows as well
        final boolean hasDockerCompose = project.file("/usr/local/bin/docker-compose").exists() ||
            project.file("/usr/bin/docker-compose").exists();
        return hasDockerCompose && Boolean.parseBoolean(System.getProperty("tests.fixture.enabled", "true"));
    }

    private void setSystemProperty(Task task, String name, Object value) {
        try {
            Method systemProperty = task.getClass().getMethod("systemProperty", String.class, Object.class);
            systemProperty.invoke(task, name, value);
        } catch (NoSuchMethodException e) {
            throw new IllegalArgumentException("Could not find systemProperty method on RandomizedTestingTask", e);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalArgumentException("Could not call systemProperty method on RandomizedTestingTask", e);
        }
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
