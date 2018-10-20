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
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.List;

import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import static java.util.Collections.singletonMap;

public class FilePermissionsTaskTest extends GradleUnitTestCase {
    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    public void testCheckPermissionsWhenAnExecutableFileExists() throws Exception {
        Project project = createProject();

        FilePermissionsTask filePermissionsTask = createTask(project);
        File outputMarker = temporaryFolder.newFile();
        filePermissionsTask.setOutputMarker(outputMarker);

        File file = new File(project.getProjectDir(), "src/main/java/Code.java");
        file.getParentFile().mkdirs();
        file.createNewFile();
        file.setExecutable(true);

        try {
            filePermissionsTask.checkInvalidPermissions();
        } catch (GradleException e) {
            assertEquals(true, e.getMessage().startsWith("Found invalid file permissions"));
        }
    }


    public void testCheckPermissionsWhenNoExecutableFileExists() throws Exception {
        Project project = createProject();

        FilePermissionsTask filePermissionsTask = createTask(project);
        File outputMarker = temporaryFolder.newFile();
        filePermissionsTask.setOutputMarker(outputMarker);

        File file = new File(project.getProjectDir(), "src/main/java/Code.java");
        file.getParentFile().mkdirs();
        file.createNewFile();

        filePermissionsTask.checkInvalidPermissions();

        List<String> result = Files.readAllLines(outputMarker.toPath(), Charset.forName("UTF-8"));
        assertEquals("done", result.get(0));
    }

    private Project createProject() throws IOException {
        Project project = ProjectBuilder.builder().withProjectDir(temporaryFolder.newFolder()).build();
        project.getPlugins().apply(JavaPlugin.class);
        return project;
    }
    private FilePermissionsTask createTask(Project project) {
        FilePermissionsTask task = (FilePermissionsTask) project.task(singletonMap("type", FilePermissionsTask.class), "filePermissionsTask");
        return task;
    }
}