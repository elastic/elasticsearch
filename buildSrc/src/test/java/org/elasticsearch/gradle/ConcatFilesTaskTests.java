package org.elasticsearch.gradle;

import java.io.File;
import java.io.IOException;
import java.lang.NullPointerException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import org.apache.tools.ant.taskdefs.condition.Os;
import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.file.FileTree;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Assert;

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
public class ConcatFilesTaskTests extends GradleUnitTestCase {
    public void testSomething() {
        assertEquals(true, true);
//        assertEquals(true, false);
    }

    public void testHeaderAdded() throws IOException {

        Project project = createProject();

        ConcatFilesTask concatFilesTask = createTask(project);

        concatFilesTask.setHeaderLine("Header");

        File file = new File(project.getProjectDir(), "src/main/java/Code.java");
        file.getParentFile().mkdirs();
        file.createNewFile();
        concatFilesTask.setTarget(file);
        concatFilesTask.setFiles(new FileTree());
//            assertEquals(1, 0);


        concatFilesTask.concatFiles();
        assertEquals(true, false);

        assertEquals(Arrays.asList("Header"), Files.readAllLines(concatFilesTask.getTarget().toPath(), Charset.forName("UTF-8")));

        file.delete();
    }

    private Project createProject() {
        Project project = ProjectBuilder.builder().build();
        project.getPlugins().apply(JavaPlugin.class);
        return project;
    }

    private ConcatFilesTask createTask(Project project) {
        return project.getTasks().create("concatFilesTask", ConcatFilesTask.class);
    }

}
