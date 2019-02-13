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
package org.elasticsearch.gradle.testclusters;

import org.gradle.api.DefaultTask;
import org.gradle.api.Project;
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.util.Set;
import java.util.stream.Collectors;

public class SyncTestClustersConfiguration extends DefaultTask {

    @InputFiles
    public FileCollection getDependencies() {
        Set<File> nonZip = getProject().getConfigurations()
            .getByName(TestClustersPlugin.HELPER_CONFIGURATION_NAME)
            .getFiles()
            .stream()
            .filter(file -> file.getName().endsWith(".zip") == false)
            .collect(Collectors.toSet());
        if(nonZip.isEmpty() == false) {
            throw new IllegalStateException("Expected only zip files in configuration : " +
                TestClustersPlugin.HELPER_CONFIGURATION_NAME + " but found " +
                nonZip
            );
        }
        return getProject().files(
            getProject().getConfigurations()
                .getByName(TestClustersPlugin.HELPER_CONFIGURATION_NAME)
                .getFiles()
        );
    }

    @OutputDirectory
    public File getOutputDir() {
        return getTestClustersConfigurationExtractDir(getProject());
    }

    @TaskAction
    public void doExtract() {
        File outputDir = getOutputDir();
        getProject().delete(outputDir);
        outputDir.mkdirs();
        getDependencies().forEach(dep ->
            getProject().copy(spec -> {
                spec.from(getProject().zipTree(dep));
                spec.into(new File(outputDir, "zip"));
            })
        );
    }

    static File getTestClustersConfigurationExtractDir(Project project) {
        return new File(TestClustersPlugin.getTestClustersBuildDir(project), "extract");
    }

}
