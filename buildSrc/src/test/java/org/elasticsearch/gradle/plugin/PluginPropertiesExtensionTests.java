/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.plugin;

import org.elasticsearch.gradle.internal.test.GradleUnitTestCase;
import org.gradle.api.Project;
import org.gradle.api.plugins.JavaPlugin;
import org.gradle.testfixtures.ProjectBuilder;

public class PluginPropertiesExtensionTests extends GradleUnitTestCase {

    public void testCreatingPluginPropertiesExtensionWithNameAndVersion() {
        String projectName = "Test";
        String projectVersion = "5.0";

        PluginPropertiesExtension pluginPropertiesExtension = new PluginPropertiesExtension(
            this.createProject(projectName, projectVersion)
        );

        assertEquals(projectName, pluginPropertiesExtension.getName());
        assertEquals(projectVersion, pluginPropertiesExtension.getVersion());
    }

    public void testCreatingPluginPropertiesExtensionWithNameWithoutVersion() {
        String projectName = "Test";

        PluginPropertiesExtension pluginPropertiesExtension = new PluginPropertiesExtension(this.createProject(projectName, null));

        assertEquals(projectName, pluginPropertiesExtension.getName());
        assertEquals("unspecified", pluginPropertiesExtension.getVersion());
    }

    private Project createProject(String projectName, String version) {
        Project project = ProjectBuilder.builder().withName(projectName).build();
        project.setVersion(version);

        project.getPlugins().apply(JavaPlugin.class);

        return project;
    }
}
