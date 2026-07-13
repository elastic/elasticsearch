/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.test;

import org.elasticsearch.gradle.LoggedExec;
import org.gradle.api.DefaultTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkerExecutor;

import javax.inject.Inject;

@CacheableTask
public abstract class ClusterFeaturesMetadataTask extends DefaultTask {
    private FileCollection classpath;

    @OutputFile
    public abstract RegularFileProperty getOutputFile();

    @Classpath
    public FileCollection getClasspath() {
        return classpath;
    }

    public void setClasspath(FileCollection classpath) {
        this.classpath = classpath;
    }

    @Inject
    public abstract WorkerExecutor getWorkerExecutor();

    @TaskAction
    public void execute() {
        getWorkerExecutor().noIsolation().submit(ClusterFeaturesMetadataWorkAction.class, params -> {
            params.getClasspath().setFrom(getClasspath());
            params.getOutputFile().set(getOutputFile());
        });
    }

    public interface ClusterFeaturesWorkParameters extends WorkParameters {
        ConfigurableFileCollection getClasspath();

        RegularFileProperty getOutputFile();
    }

    public abstract static class ClusterFeaturesMetadataWorkAction implements WorkAction<ClusterFeaturesWorkParameters> {
        private final ExecOperations execOperations;

        @Inject
        public ClusterFeaturesMetadataWorkAction(ExecOperations execOperations) {
            this.execOperations = execOperations;
        }

        @Override
        public void execute() {
            LoggedExec.javaexec(execOperations, spec -> {
                spec.getMainClass().set("org.elasticsearch.extractor.features.ClusterFeaturesMetadataExtractor");
                spec.classpath(getParameters().getClasspath());
                spec.args(getParameters().getOutputFile().get().getAsFile().getAbsolutePath());
            });
        }
    }
}
