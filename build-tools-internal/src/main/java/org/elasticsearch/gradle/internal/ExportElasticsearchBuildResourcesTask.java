/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal;

import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.model.ObjectFactory;
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

import javax.inject.Inject;

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

    @Inject
    public ExportElasticsearchBuildResourcesTask(ObjectFactory objects) {
        outputDir = objects.directoryProperty();
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

    public void copy(String resource) {
        if (getState().getExecuted() || getState().getExecuting()) {
            throw new GradleException(
                "buildResources can't be configured after the task ran. " + "Make sure task is not used after configuration time"
            );
        }
        resources.add(resource);
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
