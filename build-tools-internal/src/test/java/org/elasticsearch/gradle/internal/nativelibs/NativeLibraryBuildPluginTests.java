/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class NativeLibraryBuildPluginTests {

    private Project project;

    @Before
    public void setUp() {
        project = ProjectBuilder.builder().build();
        project.getPlugins().apply(NativeLibraryBuildPlugin.class);
    }

    @Test
    public void testTakesThePublishedLibraryRatherThanBuildingItByDefault() {
        assertEquals(BuildNativeLibraryTask.PUBLISHED_MODE, buildTask().getMode().get());
    }

    @Test
    public void testTakesThePublishedLibraryWhenTheModeVariableIsNotSet() {
        extension().getModeEnvironmentVariable().set("A_VARIABLE_THAT_IS_NOT_SET");

        assertEquals(BuildNativeLibraryTask.PUBLISHED_MODE, buildTask().getMode().get());
    }

    @Test
    public void testRefusesAHostBuildWhenNoCommandWasDeclared() {
        GradleException ex = assertThrows(
            GradleException.class,
            () -> extension().hostCommandFor(project.getLayout().getProjectDirectory())
        );

        assertTrue(ex.getMessage().contains("hostCommand"));
    }

    private NativeLibraryBuildExtension extension() {
        return (NativeLibraryBuildExtension) project.getExtensions().getByName(NativeLibraryBuildPlugin.EXTENSION);
    }

    private BuildNativeLibraryTask buildTask() {
        return (BuildNativeLibraryTask) project.getTasks().getByName(NativeLibraryBuildPlugin.BUILD_TASK);
    }
}
