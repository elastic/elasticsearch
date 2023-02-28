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
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.provider.SetProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkQueue;
import org.gradle.workers.WorkerExecutor;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

// Verifies if every transport class has a corresponding transport test class
@CacheableTask
public abstract class TransportTestExistTask extends PrecommitTask {
    private static final String MISSING_TRANSPORT_TESTS_FILE = "transport-tests/missing-transport-tests.txt";

    private FileCollection mainSources;
    private FileCollection testSources;
    private FileCollection compileClasspath;
    private FileCollection testClasspath;
    Set<String> skipClasses = new HashSet<>();

    public TransportTestExistTask() {
        setDescription("Runs TransportTestExistTask on output directories of all source sets");
    }

    @Inject
    abstract public WorkerExecutor getWorkerExecutor();

    @TaskAction
    public void runTask() {
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
    public FileCollection getCompileClasspath() {
        return compileClasspath;
    }

    public void setCompileClasspath(FileCollection compileClasspath) {
        this.compileClasspath = compileClasspath;
    }

    @Classpath
    public FileCollection getTestClasspath() {
        return testClasspath;
    }

    public void setTestClasspath(FileCollection testClasspath) {
        this.testClasspath = testClasspath;
    }

    abstract static class TransportTestExistWorkAction implements WorkAction<Parameters> {

        @Inject
        public TransportTestExistWorkAction() {
        }

        @Override
        public void execute() {
            Set<String> classesToSkip = loadClassesToSkip();

            TransportTestsScanner transportTestsScanner = new TransportTestsScanner(classesToSkip);
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
                        + missingTestClasses.stream().collect(Collectors.joining("\n"))
                );
            }
        }

    }

    private static Set<String> loadClassesToSkip() {
        var inputStream = TransportTestExistTask.class.getResourceAsStream("/" + MISSING_TRANSPORT_TESTS_FILE);
        var reader = new BufferedReader(new InputStreamReader(inputStream));
        return reader.lines()
            .filter(l -> l.startsWith("//") == false)
            .filter(l -> l.trim().equals("") == false)
            .collect(Collectors.toSet());
    }

    interface Parameters extends WorkParameters {
        ConfigurableFileCollection getMainSources();

        ConfigurableFileCollection getTestSources();

        ConfigurableFileCollection getCompileClasspath();

        ConfigurableFileCollection getTestClasspath();

        SetProperty<String> getSkipClasses();
    }

}
