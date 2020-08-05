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

package org.elasticsearch.gradle.transform;

import org.gradle.api.artifacts.transform.InputArtifact;
import org.gradle.api.artifacts.transform.TransformAction;
import org.gradle.api.artifacts.transform.TransformOutputs;
import org.gradle.api.artifacts.transform.TransformParameters;
import org.gradle.api.file.FileSystemLocation;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.internal.UncheckedException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

public interface UnpackTransform extends TransformAction<TransformParameters.None> {

    @PathSensitive(PathSensitivity.NAME_ONLY)
    @InputArtifact
    Provider<FileSystemLocation> getArchiveFile();

    @Override
    default void transform(TransformOutputs outputs) {
        File archiveFile = getArchiveFile().get().getAsFile();
        File unzipDir = outputs.dir(archiveFile.getName());
        try {
            unpack(archiveFile, unzipDir);
        } catch (IOException e) {
            throw UncheckedException.throwAsUncheckedException(e);
        }
    }

    void unpack(File archiveFile, File targetDir) throws IOException;

    /*
     * We want to remove up to the and including the jdk-.* relative paths. That is a JDK archive is structured as:
     *   jdk-12.0.1/
     *   jdk-12.0.1/Contents
     *   ...
     *
     * and we want to remove the leading jdk-12.0.1. Note however that there could also be a leading ./ as in
     *   ./
     *   ./jdk-12.0.1/
     *   ./jdk-12.0.1/Contents
     *
     * so we account for this and search the path components until we find the jdk-12.0.1, and strip the leading components.
     */
    static Path trimArchiveExtractPath(String relativePath) {
        final Path entryName = Paths.get(relativePath);
        int index = 0;
        for (; index < entryName.getNameCount(); index++) {
            if (entryName.getName(index).toString().matches("jdk-?\\d.*")) {
                break;
            }
        }
        if (index + 1 >= entryName.getNameCount()) {
            // this happens on the top-level directories in the archive, which we are removing
            return null;
        }
        // finally remove the top-level directories from the output path
        return entryName.subpath(index + 1, entryName.getNameCount());
    }

}
