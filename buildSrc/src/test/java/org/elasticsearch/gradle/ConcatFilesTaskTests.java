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
package org.elasticsearch.gradle;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Arrays;

import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;

public class ConcatFilesTaskTests extends GradleUnitTestCase {

    public void testHeaderAdded() throws IOException {

        Project project = createProject();
        ConcatFilesTask concatFilesTask = createTask(project);

        concatFilesTask.setHeaderLine("Header");

        File file = new File(project.getProjectDir(), "src/main/java/Code.java");
        file.getParentFile().mkdirs();
        file.createNewFile();
        concatFilesTask.setTarget(file);
        concatFilesTask.setFiles(project.fileTree("tmp/"));

        concatFilesTask.concatFiles();

        assertEquals(Arrays.asList("Header"), Files.readAllLines(concatFilesTask.getTarget().toPath(), StandardCharsets.UTF_8));

        file.delete();
    }

    public void testConcatenationWithUnique() throws IOException {

        Project project = createProject();
        ConcatFilesTask concatFilesTask = createTask(project);

        File file = new File(project.getProjectDir(), "src/main/java/Code.java");
        file.getParentFile().mkdirs();
        file.createNewFile();
        concatFilesTask.setTarget(file);

        File file1 = new File(project.getProjectDir(), "src/main/input/java/file1.java");
        File file2 = new File(project.getProjectDir(), "src/main/input/text/file2.txt");
        file1.getParentFile().mkdirs();
        file2.getParentFile().mkdirs();
        file1.createNewFile();
        file2.createNewFile();
        Files.write(file1.toPath(), ("Hello" + System.lineSeparator() +  "Hello").getBytes(StandardCharsets.UTF_8));
        Files.write(file2.toPath(), ("Hello" + System.lineSeparator() + "नमस्ते").getBytes(StandardCharsets.UTF_8));

        concatFilesTask.setFiles(project.fileTree(file1.getParentFile().getParentFile()));

        concatFilesTask.concatFiles();

        assertEquals(
            Arrays.asList("Hello", "नमस्ते"),
            Files.readAllLines(concatFilesTask.getTarget().toPath(), StandardCharsets.UTF_8)
        );

    }

    private Project createProject() {
        Project project = ProjectBuilder.builder().build();
        return project;
    }

    private ConcatFilesTask createTask(Project project) {
        return project.getTasks().create("concatFilesTask", ConcatFilesTask.class);
    }

}
