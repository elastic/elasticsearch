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

import org.elasticsearch.gradle.VersionProperties;
import org.elasticsearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.InvalidUserDataException;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Rule;
import org.junit.rules.ExpectedException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.stream.Collectors;

public class PluginPropertiesTaskTests extends GradleUnitTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    public void testCheckPluginPropertiesExtensionMissingName() {
        Project project = createProject();

        thrown.expect(InvalidUserDataException.class);
        thrown.expectMessage("name is a required setting for esplugin");
        createTask(project, null, "desc", "a.b.c").getExtension();
    }

    public void testCheckPluginPropertiesExtensionMissingDescription() {
        Project project = createProject();

        thrown.expect(InvalidUserDataException.class);
        thrown.expectMessage("description is a required setting for esplugin");
        createTask(project, "name", null, "a.b.c").getExtension();
    }

    public void testCheckPluginPropertiesExtensionMissingClassname() {
        Project project = createProject();

        thrown.expect(InvalidUserDataException.class);
        thrown.expectMessage("classname is a required setting for esplugin");
        createTask(project, "name", "desc", null).getExtension();
    }

    public void testCheckValidPluginPropertiesTaskPropertySubstitution() throws IOException {
        Project project = createProject();
        PluginPropertiesTask pluginPropertiesTask = createTask(project, "plugin-name", "plugin-description",
                "PluginClassname");

        pluginPropertiesTask.performAction();

        PluginPropertiesExtension pluginPropertiesTaskExtension = pluginPropertiesTask.getProject().getExtensions()
                .getByType(PluginPropertiesExtension.class);

        File generatedPluginDescriptorFile = new File(project.getBuildDir(), "generated-resources/plugin-descriptor.properties");

        assertTrue(generatedPluginDescriptorFile.exists());

        Properties generatedPluginDescriptorProps = new Properties();
        generatedPluginDescriptorProps.load(new FileInputStream(generatedPluginDescriptorFile));

        assertEquals(pluginPropertiesTaskExtension.getName(), generatedPluginDescriptorProps.getProperty("name"));
        assertEquals(pluginPropertiesTaskExtension.getDescription(), generatedPluginDescriptorProps.getProperty("description"));
        assertEquals(pluginPropertiesTaskExtension.getClassname(), generatedPluginDescriptorProps.getProperty("classname"));
        assertEquals(pluginPropertiesTaskExtension.getVersion(), generatedPluginDescriptorProps.getProperty("version"));
        assertEquals(VersionProperties.getElasticsearch(), generatedPluginDescriptorProps.getProperty("elasticsearch.version"));

        assertEquals(pluginPropertiesTaskExtension.getExtendedPlugins().stream().collect(Collectors.joining(",")),
                generatedPluginDescriptorProps.getProperty("extended.plugins"));

        assertEquals(project.getConvention().getPlugin(JavaPluginConvention.class).getTargetCompatibility().toString(),
                generatedPluginDescriptorProps.getProperty("java.version"));

        assertEquals(String.valueOf(pluginPropertiesTaskExtension.hasNativeController()),
                generatedPluginDescriptorProps.getProperty("has.native.controller"));
    }

    private Project createProject() {
        Project project = ProjectBuilder.builder().build();
        project.getPlugins().apply(JavaPlugin.class);
        return project;
    }

    private PluginPropertiesTask createTask(Project project, String name, String description, String classname) {
        PluginPropertiesTask task =  project.getTasks().create("copyPluginPropertiesTemplate", PluginPropertiesTask.class);

        PluginPropertiesExtension extension = task.getProject().getExtensions().getByType(PluginPropertiesExtension.class);

        extension.setName(name);
        extension.setDescription(description);
        extension.setClassname(classname);
        extension.setVersion("4.2.0");
        extension.setExtendedPlugins(Arrays.asList("plugin1", "plugin2"));

        return task;
    }
}
