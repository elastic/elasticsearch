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

import groovy.lang.Closure;
import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.plugins.ExtraPropertiesExtension;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.BeforeClass;

import static org.hamcrest.CoreMatchers.equalTo;

public class JdkDownloadPluginTests extends GradleUnitTestCase {
    private static Project rootProject;

    @BeforeClass
    public static void setupRoot() {
         rootProject = ProjectBuilder.builder().build();
    }

    public void testNullVersion() {
        createProject().getTasks().create("example", task -> {
            assertUsesJdkError(task, null, "linux", "version must be specified to usesJdk");
        });
    }

    public void testNullPlatform() {
        createProject().getTasks().create("example", task -> {
            assertUsesJdkError(task, "11.0.2+33", null, "platform must be specified to usesJdk");
        });
    }

    public void testUnknownPlatform() {
        createProject().getTasks().create("example", task -> {
            assertUsesJdkError(task, "11.0.2+33", "unknown", "platform must be one of [linux, windows, darwin]");
        });
    }

    public void testJdkAlreadySet() {
        createProject().getTasks().create("example", task -> {
            usesJdk(task, "11.0.2+33", "linux");
            assertUsesJdkError(task, "11.0.2+33", "linux", "jdk version already set for task");
        });
    }

    public void testBadVersionFormat() {
        createProject().getTasks().create("example", task -> {
            assertUsesJdkError(task, "badversion", "linux", "Malformed jdk version [badversion]");
        });
    }

    public void testReuseAcrossTasks() {
        Project project = createProject();
        project.getTasks().create("example1", task -> usesJdk(task, "11.0.2+33", "linux"));
        int numTasks = rootProject.getTasks().size();
        project.getTasks().create("example2", task -> usesJdk(task, "11.0.2+33", "linux"));
        assertThat(rootProject.getTasks().size(), equalTo(numTasks));
    }

    public void testReuseAcrossProjects() {
        createProject().getTasks().create("example", task -> usesJdk(task, "11.0.2+33", "linux"));
        int numTasks = rootProject.getTasks().size();
        createProject().getTasks().create("example", task -> usesJdk(task, "11.0.2+33", "linux"));
        assertThat(rootProject.getTasks().size(), equalTo(numTasks));
    }

    private void assertUsesJdkError(Task task, String version, String platform, String message) {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> usesJdk(task, version, platform));
        assertThat(e.getMessage(), equalTo(message));
    }

    private void usesJdk(Task task, String version, String platform) {
        ExtraPropertiesExtension extraProperties = task.getExtensions().findByType(ExtraPropertiesExtension.class);
        assertTrue(extraProperties.has("usesJdk"));
        @SuppressWarnings("unchecked")
        Closure<Void> usesJdkClosure = (Closure<Void>)extraProperties.get("usesJdk");
        usesJdkClosure.call(version, platform);
    }

    private Project createProject() {
        Project project = ProjectBuilder.builder().withParent(rootProject).build();
        project.getPlugins().apply("elasticsearch.jdk-download");
        return project;
    }
}
