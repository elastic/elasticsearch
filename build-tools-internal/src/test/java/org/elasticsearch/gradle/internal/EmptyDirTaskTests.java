/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal;

import org.elasticsearch.gradle.OS;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeFalse;

public class EmptyDirTaskTests {

    @Test
    public void testCreateEmptyDir() throws Exception {
        Project project = ProjectBuilder.builder().build();
        EmptyDirTask emptyDirTask = project.getTasks().create("emptyDirTask", EmptyDirTask.class);
        assertEquals(0755, emptyDirTask.getDirMode());

        // generate a new temporary folder and make sure it does not exists
        File newEmptyFolder = getNewNonExistingTempFolderFile(project);

        emptyDirTask.setDir(newEmptyFolder);
        emptyDirTask.create();

        assertTrue(newEmptyFolder.exists());
        assertTrue(newEmptyFolder.isDirectory());
        assertTrue(newEmptyFolder.canExecute());
        assertTrue(newEmptyFolder.canRead());
        assertTrue(newEmptyFolder.canWrite());

        // cleanup
        newEmptyFolder.delete();
    }

    @Test
    public void testCreateEmptyDirNoPermissions() throws Exception {
        assumeFalse("Functionality is Unix specific", OS.current() == OS.WINDOWS);

        Project project = ProjectBuilder.builder().build();
        EmptyDirTask emptyDirTask = project.getTasks().create("emptyDirTask", EmptyDirTask.class);
        emptyDirTask.setDirMode(0000);

        // generate a new temporary folder and make sure it does not exists
        File newEmptyFolder = getNewNonExistingTempFolderFile(project);

        emptyDirTask.setDir(newEmptyFolder);
        emptyDirTask.create();

        assertTrue(newEmptyFolder.exists());
        assertTrue(newEmptyFolder.isDirectory());
        assertFalse(newEmptyFolder.canExecute());
        assertFalse(newEmptyFolder.canRead());
        assertFalse(newEmptyFolder.canWrite());

        // cleanup
        newEmptyFolder.delete();
    }

    private File getNewNonExistingTempFolderFile(Project project) throws IOException {
        File newEmptyFolder = new File(project.getBuildDir(), "empty-dir");
        assertFalse(newEmptyFolder.exists());
        return newEmptyFolder;
    }

}
