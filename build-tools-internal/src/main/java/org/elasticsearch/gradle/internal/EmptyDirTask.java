/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal;

import java.io.File;

import javax.inject.Inject;

import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.TaskAction;
import org.gradle.internal.file.Chmod;

/**
 * Creates an empty directory.
 */
public class EmptyDirTask extends DefaultTask {

    private File dir;
    private int dirMode = 0755;

    /**
     * Creates an empty directory with the configured permissions.
     */
    @TaskAction
    public void create() {
        dir.mkdirs();
        getChmod().chmod(dir, dirMode);
    }

    @Inject
    public Chmod getChmod() {
        throw new UnsupportedOperationException();
    }

    @Internal
    public File getDir() {
        return dir;
    }

    @Input
    public String getDirPath() {
        return dir.getPath();
    }

    /**
     * @param dir The directory to create
     */
    public void setDir(File dir) {
        this.dir = dir;
    }

    /**
     * @param dir The path of the directory to create. Takes a String and coerces it to a file.
     */
    public void setDir(String dir) {
        this.dir = getProject().file(dir);
    }

    @Input
    public int getDirMode() {
        return dirMode;
    }

    /**
     * @param dirMode The permissions to apply to the new directory
     */
    public void setDirMode(int dirMode) {
        this.dirMode = dirMode;
    }

}
