/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit.transport;

import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitTask;
import org.gradle.api.GradleException;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.SetProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkQueue;
import org.gradle.workers.WorkerExecutor;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

/**
 * Runs LoggerUsageCheck on a set of directories.
 */
@CacheableTask
public abstract class TransportTestExistTask extends PrecommitTask {

    private FileCollection mainSources;
    private FileCollection testSources;
    private Configuration compileClasspath;
    private Configuration testClasspath;
    Set<String> skipClasses = new HashSet<>();

    public TransportTestExistTask() {
        setDescription("Runs TransportTestExistTask on output directories of all source sets");
    }

    @Inject
    abstract public WorkerExecutor getWorkerExecutor();

    @TaskAction
    public void runLoggerUsageTask() {
        WorkQueue workQueue = getWorkerExecutor().noIsolation();
        workQueue.submit(TransportTestExistWorkAction.class, parameters -> {
            parameters.getMainSources().setFrom(mainSources);
            parameters.getTestSources().setFrom(testSources);
            parameters.getCompileClasspath().setFrom(compileClasspath);
            parameters.getTestClasspath().setFrom(testClasspath);
            parameters.getSkipClasses().set(skipClasses);
        });
    }

    @InputFiles
    @SkipWhenEmpty
    @PathSensitive(PathSensitivity.RELATIVE)
    public FileCollection getMainSources() {
        return mainSources;
    }

    public void setMainSources(FileCollection mainSources) {
        this.mainSources = mainSources;
    }

    @InputFiles
    @SkipWhenEmpty
    @PathSensitive(PathSensitivity.RELATIVE)
    public FileCollection getTestSources() {
        return testSources;
    }

    public void setTestSources(FileCollection testSources) {
        this.testSources = testSources;
    }

    @Classpath
    public Configuration getCompileClasspath() {
        return compileClasspath;
    }

    public void setCompileClasspath(Configuration compileClasspath) {
        this.compileClasspath = compileClasspath;
    }

    @Classpath
    public Configuration getTestClasspath() {
        return testClasspath;
    }

    public void setTestClasspath(Configuration testClasspath) {
        this.testClasspath = testClasspath;
    }

    public void skipMissingTransportTest(String className, String reason) {
        skipClasses.add(classNameToPath(className));
    }

    public void skipTest(String className, String reason) {
        skipClasses.add(classNameToPath(className));
    }

    private String classNameToPath(String className) {
        return className.replace('.', File.separatorChar);
    }

    abstract static class TransportTestExistWorkAction implements WorkAction<Parameters> {

        private final Logger logger = Logging.getLogger(TransportTestExistTask.class);

        private final ExecOperations execOperations;

        @Inject
        public TransportTestExistWorkAction(ExecOperations execOperations) {
            this.execOperations = execOperations;
        }

        @Override
        public void execute() {
            TransportTestsScanner transportTestsScanner = new TransportTestsScanner();
            Set<String> missingTestClasses = transportTestsScanner.findTransportClassesMissingTests(
                getParameters().getMainSources().getFiles(),
                getParameters().getTestSources().getFiles(),
                getParameters().getCompileClasspath().getFiles(),
                getParameters().getTestClasspath().getFiles()
            );

            if (missingTestClasses.size() > 0) {
                throw new GradleException(
                    "There are "
                        + missingTestClasses.size()
                        + " missing tests for classes\n"
                        + "tasks.named(\"transportTestExistCheck\").configure { task ->\n"
                        + missingTestClasses.stream()
                            .map(s -> "task.skipMissingTransportTest(\"" + s + "\",\"missing test\")")
                            .collect(Collectors.joining("\n"))
                        + "\n}"
                );
            }
        }

    }

    interface Parameters extends WorkParameters {
        ConfigurableFileCollection getMainSources();

        ConfigurableFileCollection getTestSources();

        ConfigurableFileCollection getCompileClasspath();

        ConfigurableFileCollection getTestClasspath();

        SetProperty<String> getSkipClasses();
    }

}
