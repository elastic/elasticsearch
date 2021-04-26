/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.testfixtures;

import org.gradle.api.GradleException;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

public class TestFixtureExtension {

    private final Project project;
    final NamedDomainObjectContainer<Project> fixtures;
    final Map<String, String> serviceToProjectUseMap = new HashMap<>();

    public TestFixtureExtension(Project project) {
        this.project = project;
        this.fixtures = project.container(Project.class);
    }

    public void useFixture() {
        useFixture(this.project.getPath());
    }

    public void useFixture(String path) {
        addFixtureProject(path);
        serviceToProjectUseMap.put(path, this.project.getPath());
    }

    public void useFixture(String path, String serviceName) {
        addFixtureProject(path);
        String key = getServiceNameKey(path, serviceName);
        serviceToProjectUseMap.put(key, this.project.getPath());

        Optional<String> otherProject = this.findOtherProjectUsingService(key);
        if (otherProject.isPresent()) {
            throw new GradleException(
                String.format(
                    Locale.ROOT,
                    "Projects %s and %s both claim the %s service defined in the docker-compose.yml of "
                        + "%sThis is not supported because it breaks running in parallel. Configure dedicated "
                        + "services for each project and use those instead.",
                    otherProject.get(),
                    this.project.getPath(),
                    serviceName,
                    path
                )
            );
        }
    }

    private String getServiceNameKey(String fixtureProjectPath, String serviceName) {
        return fixtureProjectPath + "::" + serviceName;
    }

    private Optional<String> findOtherProjectUsingService(String serviceName) {
        return this.project.getRootProject()
            .getAllprojects()
            .stream()
            .filter(p -> p.equals(this.project) == false)
            .filter(p -> p.getExtensions().findByType(TestFixtureExtension.class) != null)
            .map(project -> project.getExtensions().getByType(TestFixtureExtension.class))
            .flatMap(ext -> ext.serviceToProjectUseMap.entrySet().stream())
            .filter(entry -> entry.getKey().equals(serviceName))
            .map(Map.Entry::getValue)
            .findAny();
    }

    private void addFixtureProject(String path) {
        Project fixtureProject = this.project.findProject(path);
        if (fixtureProject == null) {
            throw new IllegalArgumentException("Could not find test fixture " + fixtureProject);
        }
        if (fixtureProject.file(TestFixturesPlugin.DOCKER_COMPOSE_YML).exists() == false) {
            throw new IllegalArgumentException(
                "Project " + path + " is not a valid test fixture: missing " + TestFixturesPlugin.DOCKER_COMPOSE_YML
            );
        }
        fixtures.add(fixtureProject);
        // Check for exclusive access
        Optional<String> otherProject = this.findOtherProjectUsingService(path);
        if (otherProject.isPresent()) {
            throw new GradleException(
                String.format(
                    Locale.ROOT,
                    "Projects %s and %s both claim all services from %s. This is not supported because it"
                        + " breaks running in parallel. Configure specific services in docker-compose.yml "
                        + "for each and add the service name to `useFixture`",
                    otherProject.get(),
                    this.project.getPath(),
                    path
                )
            );
        }
    }

    boolean isServiceRequired(String serviceName, String fixtureProject) {
        if (serviceToProjectUseMap.containsKey(fixtureProject)) {
            return true;
        }
        return serviceToProjectUseMap.containsKey(getServiceNameKey(fixtureProject, serviceName));
    }
}
