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

import javax.inject.Inject;

import org.gradle.api.DefaultTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.TaskAction;
import org.gradle.internal.nativeintegration.filesystem.Chmod;

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

    @Input
    public File getDir() {
        return dir;
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
