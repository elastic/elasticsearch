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

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.StopExecutionException;
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Export Elasticsearch build resources to configurable paths
 * <p>
 * Wil overwrite existing files and create missing directories.
 * Useful for resources that that need to be passed to other processes trough the filesystem or otherwise can't be
 * consumed from the classpath.
 */
public class ExportElasticsearchBuildResourcesTask extends DefaultTask {

    private final Logger logger = Logging.getLogger(ExportElasticsearchBuildResourcesTask.class);

    private final Set<String> resources = new HashSet<>();

    private DirectoryProperty outputDir;

    public ExportElasticsearchBuildResourcesTask() {
        outputDir = getProject().getObjects().directoryProperty();
        outputDir.set(new File(getProject().getBuildDir(), "build-tools-exported"));
    }

    @OutputDirectory
    public DirectoryProperty getOutputDir() {
        return outputDir;
    }

    @Input
    public Set<String> getResources() {
        return Collections.unmodifiableSet(resources);
    }

    @Classpath
    public String getResourcesClasspath() {
        // This will make sure the task is not considered up to date if the resources are changed.
        logger.info("Classpath: {}", System.getProperty("java.class.path"));
        return System.getProperty("java.class.path");
    }

    public void setOutputDir(File outputDir) {
        this.outputDir.set(outputDir);
    }

    public File copy(String resource) {
        if (getState().getExecuted() || getState().getExecuting()) {
            throw new GradleException(
                "buildResources can't be configured after the task ran. " + "Make sure task is not used after configuration time"
            );
        }
        resources.add(resource);
        return outputDir.file(resource).get().getAsFile();
    }

    @TaskAction
    public void doExport() {
        if (resources.isEmpty()) {
            setDidWork(false);
            throw new StopExecutionException();
        }
        resources.stream().parallel().forEach(resourcePath -> {
            Path destination = outputDir.get().file(resourcePath).getAsFile().toPath();
            try (InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath)) {
                Files.createDirectories(destination.getParent());
                if (is == null) {
                    throw new GradleException("Can't export `" + resourcePath + "` from build-tools: not found");
                }
                Files.copy(is, destination, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                throw new GradleException("Can't write resource `" + resourcePath + "` to " + destination, e);
            }
        });
    }

}
