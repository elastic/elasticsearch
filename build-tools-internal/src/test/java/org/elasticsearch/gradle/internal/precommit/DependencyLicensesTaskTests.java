/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.precommit;

import org.apache.groovy.util.Maps;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Dependency;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;

public class DependencyLicensesTaskTests {

    private static final String PERMISSIVE_LICENSE_TEXT = "Eclipse Public License - v 2.0";
    private static final String STRICT_LICENSE_TEXT = "GNU LESSER GENERAL PUBLIC LICENSE Version 3";

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private TaskProvider<DependencyLicensesTask> task;

    private Project project;

    private Dependency dependency;

    @Before
    public void prepare() {
        project = createProject();
        task = createDependencyLicensesTask(project);
        dependency = project.getDependencies().localGroovy();
        task.configure(new Action<DependencyLicensesTask>() {
            @Override
            public void execute(DependencyLicensesTask dependencyLicensesTask) {
                dependencyLicensesTask.mapping(Maps.of("from", "groovy-.*", "to", "groovy"));
                dependencyLicensesTask.mapping(Maps.of("from", "javaparser-.*", "to", "groovy"));
            }
        });
    }

    @Test
    public void givenProjectWithLicensesDirButNoDependenciesThenShouldThrowException() throws Exception {
        expectedException.expect(GradleException.class);
        expectedException.expectMessage(containsString("exists, but there are no dependencies"));

        getLicensesDir(project).mkdir();
        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithoutLicensesDirButWithDependenciesThenShouldThrowException() throws Exception {
        expectedException.expect(GradleException.class);
        expectedException.expectMessage(containsString("does not exist, but there are dependencies"));

        project.getDependencies().add("implementation", dependency);
        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithoutLicensesDirNorDependenciesThenShouldReturnSilently() throws Exception {
        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithDependencyButNoLicenseFileThenShouldReturnException() throws Exception {
        expectedException.expect(GradleException.class);
        expectedException.expectMessage(containsString("Missing LICENSE for "));

        project.getDependencies().add("implementation", project.getDependencies().localGroovy());

        getLicensesDir(project).mkdir();
        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithDependencyButNoNoticeFileThenShouldReturnException() throws Exception {
        expectedException.expect(GradleException.class);
        expectedException.expectMessage(containsString("Missing NOTICE for "));

        project.getDependencies().add("implementation", dependency);

        createFileIn(getLicensesDir(project), "groovy-LICENSE.txt", PERMISSIVE_LICENSE_TEXT);

        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithStrictDependencyButNoSourcesFileThenShouldReturnException() throws Exception {
        expectedException.expect(GradleException.class);
        expectedException.expectMessage(containsString("Missing SOURCES for "));

        project.getDependencies().add("implementation", dependency);

        createFileIn(getLicensesDir(project), "groovy-LICENSE.txt", STRICT_LICENSE_TEXT);
        createFileIn(getLicensesDir(project), "groovy-NOTICE.txt", "");

        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithStrictDependencyAndEverythingInOrderThenShouldReturnSilently() throws Exception {
        project.getDependencies().add("implementation", dependency);

        createFileIn(getLicensesDir(project), "groovy-LICENSE.txt", STRICT_LICENSE_TEXT);
        createFileIn(getLicensesDir(project), "groovy-NOTICE.txt", "");
        createFileIn(getLicensesDir(project), "groovy-SOURCES.txt", "");

        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithDependencyAndEverythingInOrderThenShouldReturnSilently() throws Exception {
        project.getDependencies().add("implementation", dependency);

        File licensesDir = getLicensesDir(project);

        createAllDefaultDependencyFiles(licensesDir, "groovy");
        createAllDefaultDependencyFiles(licensesDir, "groovy");
        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithALicenseButWithoutTheDependencyThenShouldThrowException() throws Exception {
        expectedException.expect(GradleException.class);
        expectedException.expectMessage(containsString("Unused license "));

        project.getDependencies().add("implementation", dependency);

        File licensesDir = getLicensesDir(project);
        createAllDefaultDependencyFiles(licensesDir, "groovy");
        createFileIn(licensesDir, "non-declared-LICENSE.txt", "");

        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithANoticeButWithoutTheDependencyThenShouldThrowException() throws Exception {
        expectedException.expect(GradleException.class);
        expectedException.expectMessage(containsString("Unused notice "));

        project.getDependencies().add("implementation", dependency);

        File licensesDir = getLicensesDir(project);
        createAllDefaultDependencyFiles(licensesDir, "groovy");
        createFileIn(licensesDir, "non-declared-NOTICE.txt", "");

        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithADependencyMappingThenShouldReturnSilently() throws Exception {
        project.getDependencies().add("implementation", dependency);

        File licensesDir = getLicensesDir(project);
        createAllDefaultDependencyFiles(licensesDir, "groovy");

        Map<String, String> mappings = new HashMap<>();
        mappings.put("from", "groovy");
        mappings.put("to", "groovy");

        task.get().mapping(mappings);
        task.get().checkDependencies();
    }

    @Test
    public void givenProjectWithAIgnoreShaConfigurationAndNoShaFileThenShouldReturnSilently() throws Exception {
        project.getDependencies().add("implementation", dependency);

        File licensesDir = getLicensesDir(project);
        createFileIn(licensesDir, "groovy-LICENSE.txt", PERMISSIVE_LICENSE_TEXT);
        createFileIn(licensesDir, "groovy-NOTICE.txt", "");

        task.get().ignoreSha("groovy");
        task.get().ignoreSha("groovy-ant");
        task.get().ignoreSha("groovy-astbuilder");
        task.get().ignoreSha("groovy-console");
        task.get().ignoreSha("groovy-datetime");
        task.get().ignoreSha("groovy-dateutil");
        task.get().ignoreSha("groovy-groovydoc");
        task.get().ignoreSha("groovy-json");
        task.get().ignoreSha("groovy-nio");
        task.get().ignoreSha("groovy-sql");
        task.get().ignoreSha("groovy-templates");
        task.get().ignoreSha("groovy-test");
        task.get().ignoreSha("groovy-xml");
        task.get().ignoreSha("javaparser-core");
        task.get().checkDependencies();
    }

    private Project createProject() {
        Project project = ProjectBuilder.builder().build();
        project.getPlugins().apply(JavaPlugin.class);

        return project;
    }

    private void createAllDefaultDependencyFiles(File licensesDir, String dependencyName) throws IOException {
        createFileIn(licensesDir, dependencyName + "-LICENSE.txt", PERMISSIVE_LICENSE_TEXT);
        createFileIn(licensesDir, dependencyName + "-NOTICE.txt", "");
    }

    private File getLicensesDir(Project project) {
        return getFile(project, "licenses");
    }

    private File getFile(Project project, String fileName) {
        return project.getProjectDir().toPath().resolve(fileName).toFile();
    }

    private void createFileIn(File parent, String name, String content) throws IOException {
        parent.mkdir();

        Path file = parent.toPath().resolve(name);
        file.toFile().createNewFile();

        Files.write(file, content.getBytes(StandardCharsets.UTF_8));
    }

    private TaskProvider<DependencyLicensesTask> createDependencyLicensesTask(Project project) {
        TaskProvider<DependencyLicensesTask> task = project.getTasks()
            .register("dependencyLicenses", DependencyLicensesTask.class, new Action<DependencyLicensesTask>() {
                @Override
                public void execute(DependencyLicensesTask dependencyLicensesTask) {
                    dependencyLicensesTask.setDependencies(getDependencies(project));
                }
            });

        return task;
    }

    private FileCollection getDependencies(Project project) {
        return project.getConfigurations().getByName("compileClasspath");
    }
}
