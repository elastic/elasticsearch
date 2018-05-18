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
package org.elasticsearch.gradle.precommit

import org.gradle.api.DefaultTask
import org.gradle.api.GradleException
import org.gradle.api.file.FileCollection
import org.gradle.api.tasks.InputFiles
import org.gradle.api.tasks.OutputFile
import org.gradle.api.tasks.SourceSet
import org.gradle.api.tasks.TaskAction
import org.gradle.api.tasks.util.PatternSet
import org.gradle.api.tasks.util.PatternFilterable
import org.apache.tools.ant.taskdefs.condition.Os

import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermission
import java.nio.file.attribute.PosixFileAttributeView

import static java.nio.file.attribute.PosixFilePermission.OTHERS_EXECUTE
import static java.nio.file.attribute.PosixFilePermission.GROUP_EXECUTE
import static java.nio.file.attribute.PosixFilePermission.OWNER_EXECUTE

/**
 * Checks source files for correct file permissions.
 */
public class FilePermissionsTask extends DefaultTask {

    /** A pattern set of which files should be checked. */
    private PatternFilterable filesFilter = new PatternSet()

    @OutputFile
    File outputMarker = new File(project.buildDir, 'markers/filePermissions')

    FilePermissionsTask() {
        onlyIf { !Os.isFamily(Os.FAMILY_WINDOWS) }
        description = "Checks java source files for correct file permissions"
        // we always include all source files, and exclude what should not be checked
        filesFilter.include('**')
        // exclude sh files that might have the executable bit set
        filesFilter.exclude('**/*.sh')
    }

    /** Returns the files this task will check */
    @InputFiles
    FileCollection files() {
        List<FileCollection> collections = new ArrayList<>()
        for (SourceSet sourceSet : project.sourceSets) {
            collections.add(sourceSet.allSource.matching(filesFilter))
        }
        return project.files(collections.toArray())
    }

    @TaskAction
    void checkInvalidPermissions() {
        List<String> failures = new ArrayList<>()
        for (File f : files()) {
            PosixFileAttributeView fileAttributeView = Files.getFileAttributeView(f.toPath(), PosixFileAttributeView.class)
            Set<PosixFilePermission> permissions = fileAttributeView.readAttributes().permissions()
            if (permissions.contains(OTHERS_EXECUTE) || permissions.contains(OWNER_EXECUTE) ||
                permissions.contains(GROUP_EXECUTE)) {
                failures.add("Source file is executable: " + f)
            }
        }
        if (failures.isEmpty() == false) {
            throw new GradleException('Found invalid file permissions:\n' + failures.join('\n'))
        }
        outputMarker.setText('done', 'UTF-8')
    }

}
