/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.gradle.util;

import org.gradle.api.UncheckedIOException;

import java.io.File;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

public final class FileUtils {

    /**
     * Like {@link java.io.File#mkdirs()}, except throws an informative error if a dir cannot be created.
     *
     * @param dir The dir to create, including any non existent parent dirs.
     */
    public static void mkdirs(File dir) {
        dir = dir.getAbsoluteFile();
        if (dir.isDirectory()) {
            return;
        }

        if (dir.exists() && dir.isDirectory() == false) {
            throw new UncheckedIOException(String.format("Cannot create directory '%s' as it already exists, but is not a directory", dir));
        }

        List<File> toCreate = new LinkedList<File>();
        File parent = dir.getParentFile();
        while (parent.exists() == false) {
            toCreate.add(parent);
            parent = parent.getParentFile();
        }
        Collections.reverse(toCreate);
        for (File parentDirToCreate : toCreate) {
            if (parentDirToCreate.isDirectory()) {
                continue;
            }
            File parentDirToCreateParent = parentDirToCreate.getParentFile();
            if (parentDirToCreateParent.isDirectory() == false) {
                throw new UncheckedIOException(
                    String.format(
                        "Cannot create parent directory '%s' when creating directory '%s' as '%s' is not a directory",
                        parentDirToCreate,
                        dir,
                        parentDirToCreateParent
                    )
                );
            }
            if (parentDirToCreate.mkdir() == false && parentDirToCreate.isDirectory() == false) {
                throw new UncheckedIOException(
                    String.format("Failed to create parent directory '%s' when creating directory '%s'", parentDirToCreate, dir)
                );
            }
        }
        if (dir.mkdir() == false && dir.isDirectory() == false) {
            throw new UncheckedIOException(String.format("Failed to create directory '%s'", dir));
        }
    }
}
