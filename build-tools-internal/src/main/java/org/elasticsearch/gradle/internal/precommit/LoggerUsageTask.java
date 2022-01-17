/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.LoggedExec;
import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.plugins.JavaPluginExtension;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkQueue;
import org.gradle.workers.WorkerExecutor;

import java.io.File;

import javax.inject.Inject;

/**
 * Runs LoggerUsageCheck on a set of directories.
 */
@CacheableTask
public abstract class LoggerUsageTask extends PrecommitTask {

    private FileCollection classpath;

    public LoggerUsageTask() {
        setDescription("Runs LoggerUsageCheck on output directories of all source sets");
    }

    @Inject
    abstract public WorkerExecutor getWorkerExecutor();

    @TaskAction
    public void runLoggerUsageTask() {
        WorkQueue workQueue = getWorkerExecutor().noIsolation();
        workQueue.submit(LoggerUsageWorkAction.class, parameters -> {
            parameters.getClasspath().setFrom(getClasspath());
            parameters.getClassDirectories().setFrom(getClassDirectories());
        });
    }

    @Classpath
    public FileCollection getClasspath() {
        return classpath;
    }

    public void setClasspath(FileCollection classpath) {
        this.classpath = classpath;
    }

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    @SkipWhenEmpty
    public FileCollection getClassDirectories() {
        return getProject().getExtensions()
            .getByType(JavaPluginExtension.class)
            .getSourceSets()
            .stream()
            // Don't pick up all source sets like the java9 ones as logger-check doesn't support the class format
            .filter(
                sourceSet -> sourceSet.getName().equals(SourceSet.MAIN_SOURCE_SET_NAME)
                    || sourceSet.getName().equals(SourceSet.TEST_SOURCE_SET_NAME)
            )
            .map(sourceSet -> sourceSet.getOutput().getClassesDirs())
            .reduce(FileCollection::plus)
            .orElse(getProject().files())
            .filter(File::exists);
    }

    abstract static class LoggerUsageWorkAction implements WorkAction<Parameters> {

        private final ExecOperations execOperations;

        @Inject
        public LoggerUsageWorkAction(ExecOperations execOperations) {
            this.execOperations = execOperations;
        }

        @Override
        public void execute() {
            LoggedExec.javaexec(execOperations, spec -> {
                spec.getMainClass().set("org.elasticsearch.test.loggerusage.ESLoggerUsageChecker");
                spec.classpath(getParameters().getClasspath());
                getParameters().getClassDirectories().forEach(spec::args);
            });
        }
    }

    interface Parameters extends WorkParameters {
        ConfigurableFileCollection getClassDirectories();

        ConfigurableFileCollection getClasspath();
    }

}
