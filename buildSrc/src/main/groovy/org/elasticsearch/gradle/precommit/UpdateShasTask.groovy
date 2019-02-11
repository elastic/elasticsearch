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
import org.gradle.api.tasks.TaskAction

import java.nio.file.Files
import java.security.MessageDigest

/**
 * A task to update shas used by {@code DependencyLicensesCheck}
 */
public class UpdateShasTask extends DefaultTask {

    /** The parent dependency licenses task to use configuration from */
    public DependencyLicensesTask parentTask

    public UpdateShasTask() {
        description = 'Updates the sha files for the dependencyLicenses check'
        onlyIf { parentTask.licensesDir.exists() }
    }

    @TaskAction
    public void updateShas() {
        Set<File> shaFiles = new HashSet<File>()
        parentTask.licensesDir.eachFile {
            String name = it.getName()
            if (name.endsWith(DependencyLicensesTask.SHA_EXTENSION)) {
                shaFiles.add(it)
            }
        }
        for (File dependency : parentTask.dependencies) {
            String jarName = dependency.getName()
            File shaFile = new File(parentTask.licensesDir, jarName + DependencyLicensesTask.SHA_EXTENSION)
            if (shaFile.exists() == false) {
                logger.lifecycle("Adding sha for ${jarName}")
                String sha = MessageDigest.getInstance("SHA-1").digest(dependency.getBytes()).encodeHex().toString()
                shaFile.setText(sha, 'UTF-8')
            } else {
                shaFiles.remove(shaFile)
            }
        }
        shaFiles.each { shaFile ->
            logger.lifecycle("Removing unused sha ${shaFile.getName()}")
            Files.delete(shaFile.toPath())
        }
    }
}
