/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.testfixtures;

import org.gradle.api.GradleException;
import org.gradle.api.Project;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

public class TestFixtureExtension {

    private final String projectPath;
    final List<String> fixtures = new ArrayList<>();
    final Map<String, String> serviceToProjectUseMap = new HashMap<>();
    private transient Project project;

    public final File testfixturesDir;

    public TestFixtureExtension(Project project) {
        this.project = project;
        this.testfixturesDir = project.file("testfixtures_shared");
        this.projectPath = project.getPath();
    }

    public void useFixture() {
        useFixture(this.projectPath);
    }

    public void useFixture(String path) {
        addFixtureProject(path);
        serviceToProjectUseMap.put(path, this.projectPath);
    }

    public void useFixture(String path, String serviceName) {

        addFixtureProject(path);
        String key = getServiceNameKey(path, serviceName);
        serviceToProjectUseMap.put(key, this.projectPath);

        Optional<String> otherProject = this.findOtherProjectUsingService(key);
        if (otherProject.isPresent()) {
            throw new GradleException(
                String.format(
                    Locale.ROOT,
                    "Projects %s and %s both claim the %s service defined in the docker-compose.yml of "
                        + "%sThis is not supported because it breaks running in parallel. Configure dedicated "
                        + "services for each project and use those instead.",
                    otherProject.get(),
                    this.projectPath,
                    serviceName,
                    path
                )
            );
        }
    }

    private String getServiceNameKey(String fixtureProjectPath, String serviceName) {
        return fixtureProjectPath + "::" + serviceName;
    }

    // TODO: This is a hack to get around the fact that we don't have a way to get the project
    private Optional<String> findOtherProjectUsingService(String serviceName) {
        return Optional.empty();

        /*this.project.getRootProject()
        .getAllprojects()
        .stream()
        .filter(p -> p.equals(this.project) == false)
        .filter(p -> p.getExtensions().findByType(TestFixtureExtension.class) != null)
        .map(project -> project.getExtensions().getByType(TestFixtureExtension.class))
        .flatMap(ext -> ext.serviceToProjectUseMap.entrySet().stream())
        .filter(entry -> entry.getKey().equals(serviceName))
        .map(Map.Entry::getValue)
        .findAny();*/
    }

    private void addFixtureProject(String path) {
        // try to find a fix for this
        if(projectPath.equals(path) == false) {
            project.evaluationDependsOn(path);
        }
        // Project fixtureProject = this.project.findProject(path);
        /*if (fixtureProject == null) {
            throw new IllegalArgumentException("Could not find test fixture " + fixtureProject);
        }
        if (fixtureProject.file(TestFixturesPlugin.DOCKER_COMPOSE_YML).exists() == false) {
            throw new IllegalArgumentException(
                "Project " + path + " is not a valid test fixture: missing " + TestFixturesPlugin.DOCKER_COMPOSE_YML
            );
        }*/
        fixtures.add(path);
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
                    this.projectPath,
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
