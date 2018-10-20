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
package org.elasticsearch.gradle.precommit;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFileAttributeView;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.tools.ant.taskdefs.condition.Os;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaPluginConvention;
import org.gradle.api.tasks.*;
import org.gradle.api.tasks.util.PatternFilterable;
import org.gradle.api.tasks.util.PatternSet;

/**
 * Checks source files for correct file permissions.
 */
public class FilePermissionsTask extends DefaultTask {

    /**
     * A pattern set of which files should be checked.
     */
    private final PatternFilterable filesFilter = new PatternSet()
            // we always include all source files, and exclude what should not be checked
            .include("**")
            // exclude sh files that might have the executable bit set
            .exclude("**/*.sh");

    @OutputFile
    private File outputMarker = new File(getProject().getBuildDir(), "markers/filePermissions");

    public FilePermissionsTask() {
        setDescription("Checks java source files for correct file permissions");
    }

    /**
     * Returns the files this task will check
     */
    @InputFiles
    public FileCollection files() {
        SourceSetContainer sourceSets = getProject().getConvention().getPlugin(JavaPluginConvention.class).getSourceSets();
        Object[] fileTreeStream = sourceSets.stream()
                .map(sourceSet -> sourceSet.getAllSource().matching(filesFilter))
                .toArray();
        return getProject().files(fileTreeStream);
    }

    @TaskAction
    public void checkInvalidPermissions() throws IOException {
        if (Os.isFamily(Os.FAMILY_WINDOWS)) {
            throw new StopExecutionException();
        }

        List<String> failures = new ArrayList<String>();
        for (File f : files()) {
            PosixFileAttributeView fileAttributeView = Files.getFileAttributeView(f.toPath(), PosixFileAttributeView.class);
            Set<PosixFilePermission> permissions = fileAttributeView.readAttributes().permissions();
            if (permissions.contains(PosixFilePermission.OTHERS_EXECUTE)
                    || permissions.contains(PosixFilePermission.OWNER_EXECUTE)
                    || permissions.contains(PosixFilePermission.GROUP_EXECUTE)) {
                failures.add("Source file is executable: " + f);
            }
        }

        if (!failures.isEmpty()) {
            throw new GradleException("Found invalid file permissions:\n" + String.join("\n", failures));
        }

        Files.write(outputMarker.toPath(), "done".getBytes("UTF-8"));
    }

    public File getOutputMarker() {
        return outputMarker;
    }

    public void setOutputMarker(File outputMarker) {
        this.outputMarker = outputMarker;
    }
}
