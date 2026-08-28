/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.internal.nativelibs;

import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class VerifyNativeLibraryLinuxAbiTaskTests {

    @Test
    public void registersVerifyTaskAndWiresCheck() {
        Project project = ProjectBuilder.builder().build();
        project.getPlugins().apply(NativeLibrariesLinuxAbiPlugin.class);

        assertTrue(project.getTasks().getNames().contains(NativeLibrariesLinuxAbiPlugin.VERIFY_TASK));
        assertTrue(project.getTasks().getNames().contains("check"));
    }
}
