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

import org.gradle.api.DefaultTask;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.TaskProvider;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.security.NoSuchAlgorithmException;
import java.util.Set;

/**
 * A task to update shas used by {@code DependencyLicensesCheck}
 */
public class UpdateShasTask extends DefaultTask {

    private final Logger logger = Logging.getLogger(getClass());

    /** The parent dependency licenses task to use configuration from */
    private TaskProvider<DependencyLicensesTask> parentTask;

    public UpdateShasTask() {
        setDescription("Updates the sha files for the dependencyLicenses check");
        setOnlyIf(element -> parentTask.get().getLicensesDir() != null);
    }

    @TaskAction
    public void updateShas() throws NoSuchAlgorithmException, IOException {
        Set<File> shaFiles = parentTask.get().getShaFiles();

        for (File dependency : parentTask.get().getDependencies()) {
            String jarName = dependency.getName();
            File shaFile = parentTask.get().getShaFile(jarName);

            if (shaFile.exists() == false) {
                createSha(dependency, jarName, shaFile);
            } else {
                shaFiles.remove(shaFile);
            }
        }

        for (File shaFile : shaFiles) {
            logger.lifecycle("Removing unused sha " + shaFile.getName());
            shaFile.delete();
        }
    }

    private void createSha(File dependency, String jarName, File shaFile) throws IOException, NoSuchAlgorithmException {
        logger.lifecycle("Adding sha for " + jarName);

        String sha = parentTask.get().getSha1(dependency);

        Files.write(shaFile.toPath(), sha.getBytes(StandardCharsets.UTF_8), StandardOpenOption.CREATE);
    }

    @Internal
    public DependencyLicensesTask getParentTask() {
        return parentTask.get();
    }

    public void setParentTask(TaskProvider<DependencyLicensesTask> parentTask) {
        this.parentTask = parentTask;
    }
}
