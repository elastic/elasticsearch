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
package org.elasticsearch.gradle.precommit;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.apache.tools.ant.taskdefs.condition.Os;
import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Assert;

public class FilePermissionsTaskTests extends GradleUnitTestCase {

    public void testCheckPermissionsWhenAnExecutableFileExists() throws Exception {
        RandomizedTest.assumeFalse("Functionality is Unix specific", Os.isFamily(Os.FAMILY_WINDOWS));

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

    public void testCheckPermissionsWhenNoFileExists() throws Exception {
        RandomizedTest.assumeFalse("Functionality is Unix specific", Os.isFamily(Os.FAMILY_WINDOWS));

        Project project = createProject();

        FilePermissionsTask filePermissionsTask = createTask(project);

        filePermissionsTask.checkInvalidPermissions();

        File outputMarker = new File(project.getBuildDir(), "markers/filePermissions");
        List<String> result = Files.readAllLines(outputMarker.toPath(), Charset.forName("UTF-8"));
        assertEquals("done", result.get(0));
    }

    public void testCheckPermissionsWhenNoExecutableFileExists() throws Exception {
        RandomizedTest.assumeFalse("Functionality is Unix specific", Os.isFamily(Os.FAMILY_WINDOWS));

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
        return project.getTasks().create("filePermissionsTask", FilePermissionsTask.class);
    }

}
