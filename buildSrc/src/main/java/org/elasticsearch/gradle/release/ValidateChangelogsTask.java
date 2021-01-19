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

package org.elasticsearch.gradle.release;

import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.util.PatternSet;

import java.io.File;

public class ValidateChangelogsTask extends DefaultTask {
    private static final Logger LOGGER = Logging.getLogger(ValidateChangelogsTask.class);

    private final ConfigurableFileCollection changelogs = getProject().getObjects().fileCollection();

    public ValidateChangelogsTask() {
        this.changelogs.setFrom(
            getProject().getLayout()
                .getProjectDirectory()
                .dir("docs/changelog")
                .getAsFileTree()
                .matching(new PatternSet().include("**/*.yml", "**/*.yaml"))
                .getFiles()
        );
    }

    @InputFiles
    public FileCollection getChangelogs() {
        return changelogs;
    }

    @TaskAction
    public void executeTask() {
        LOGGER.info("Finding and validating changelog files...");

        for (File file : this.changelogs.getFiles()) {
            ChangelogEntry.parseChangelog(file);
        }
    }
}
