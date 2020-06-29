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
package org.elasticsearch.gradle.plugin;

import org.elasticsearch.gradle.BwcVersions;
import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.internal.project.ProjectInternal;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Before;
import org.junit.Ignore;
import org.mockito.Mockito;

import java.util.stream.Collectors;

public class PluginBuildPluginTests extends GradleUnitTestCase {

    private Project project;

    @Before
    public void setUp() throws Exception {
        project = ProjectBuilder.builder().withName(getClass().getName()).build();
    }

    public void testApply() {
        // FIXME: distribution download plugin doesn't support running externally
        project.getExtensions().getExtraProperties().set("bwcVersions", Mockito.mock(BwcVersions.class));
        project.getPlugins().apply(PluginBuildPlugin.class);

        assertNotNull(
            "plugin extension created with the right name",
            project.getExtensions().findByName(PluginBuildPlugin.PLUGIN_EXTENSION_NAME)
        );
        assertNotNull("plugin extensions has the right type", project.getExtensions().findByType(PluginPropertiesExtension.class));

        assertNotNull("plugin created an integTest class", project.getTasks().findByName("integTest"));
    }

    @Ignore("https://github.com/elastic/elasticsearch/issues/47123")
    public void testApplyWithAfterEvaluate() {
        project.getExtensions().getExtraProperties().set("bwcVersions", Mockito.mock(BwcVersions.class));
        project.getPlugins().apply(PluginBuildPlugin.class);
        PluginPropertiesExtension extension = project.getExtensions().getByType(PluginPropertiesExtension.class);
        extension.setNoticeFile(project.file("test.notice"));
        extension.setLicenseFile(project.file("test.license"));
        extension.setDescription("just a test");
        extension.setClassname(getClass().getName());

        ((ProjectInternal) project).evaluate();

        assertNotNull(
            "Task to generate notice not created: " + project.getTasks().stream().map(Task::getPath).collect(Collectors.joining(", ")),
            project.getTasks().findByName("generateNotice")
        );
    }
}
