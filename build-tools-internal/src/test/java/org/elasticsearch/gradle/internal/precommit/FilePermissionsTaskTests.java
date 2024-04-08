/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.OS;
import org.elasticsearch.gradle.util.GradleUtils;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class FilePermissionsTaskTests {

    @Test
    public void testCheckPermissionsWhenAnExecutableFileExists() throws Exception {
        assumeFalse("Functionality is Unix specific", OS.current() == OS.WINDOWS);

        Project project = createProject();

        FilePermissionsTask filePermissionsTask = createTask(project);
        File file = new File(project.getProjectDir(), "src/main/java/Code.java");
        file.getParentFile().mkdirs();
        file.createNewFile();
        file.setExecutable(true);

        try {
            filePermissionsTask.checkInvalidPermissions();
            Assert.fail("the check should have failed because of the executable file permission");
        } catch (GradleException e) {
            assertTrue(e.getMessage().startsWith("Found invalid file permissions"));
        }
        file.delete();
    }

    @Test
    public void testCheckPermissionsWhenNoFileExists() throws Exception {
        assumeFalse("Functionality is Unix specific", OS.current() == OS.WINDOWS);

        Project project = createProject();

        FilePermissionsTask filePermissionsTask = createTask(project);

        filePermissionsTask.checkInvalidPermissions();

        File outputMarker = new File(project.getBuildDir(), "markers/filePermissions");
        List<String> result = Files.readAllLines(outputMarker.toPath(), Charset.forName("UTF-8"));
        assertEquals("done", result.get(0));
    }

    @Test
    public void testCheckPermissionsWhenNoExecutableFileExists() throws Exception {
        assumeFalse("Functionality is Unix specific", OS.current() == OS.WINDOWS);

        Project project = createProject();

        FilePermissionsTask filePermissionsTask = createTask(project);

        File file = new File(project.getProjectDir(), "src/main/java/Code.java");
        file.getParentFile().mkdirs();
        file.createNewFile();

        filePermissionsTask.checkInvalidPermissions();

        File outputMarker = new File(project.getBuildDir(), "markers/filePermissions");
        List<String> result = Files.readAllLines(outputMarker.toPath(), Charset.forName("UTF-8"));
        assertEquals("done", result.get(0));

        file.delete();
    }

    private Project createProject() {
        Project project = ProjectBuilder.builder().build();
        project.getPlugins().apply(JavaPlugin.class);
        return project;
    }

    private FilePermissionsTask createTask(Project project) {
        return project.getTasks()
            .create(
                "filePermissionsTask",
                FilePermissionsTask.class,
                filePermissionsTask -> filePermissionsTask.getSources()
                    .addAll(
                        project.provider(
                            () -> GradleUtils.getJavaSourceSets(project).stream().map(s -> s.getAllSource()).collect(Collectors.toList())
                        )
                    )
            );
    }

}
