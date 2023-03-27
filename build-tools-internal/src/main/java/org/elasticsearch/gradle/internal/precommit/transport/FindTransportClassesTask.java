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
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.SetProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkQueue;
import org.gradle.workers.WorkerExecutor;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

// Verifies if every transport class has a corresponding transport test class
@CacheableTask
public abstract class FindTransportClassesTask extends PrecommitTask {
    private static final String MISSING_TRANSPORT_TESTS_FILE = "transport-tests/missing-transport-tests.txt";
    public static final String TRANSPORT_CLASSES = "generated-resources/transport-classes.txt";
    private FileCollection mainSources;
    private FileCollection compileClasspath;
    Set<String> skipClasses = new HashSet<>();

    @Inject
    public FindTransportClassesTask(ProjectLayout projectLayout) {
        setDescription("Runs TransportTestExistTask on output directories of all source sets");
        getOutputFile().convention(projectLayout.getBuildDirectory().file(TRANSPORT_CLASSES));
    }

    @Inject
    abstract public WorkerExecutor getWorkerExecutor();

    @OutputFile
    public abstract RegularFileProperty getOutputFile();

    @TaskAction
    public void runTask() {
        WorkQueue workQueue = getWorkerExecutor().noIsolation();
        workQueue.submit(TransportTestExistWorkAction.class, parameters -> {
            parameters.getMainSources().setFrom(mainSources);
            parameters.getCompileClasspath().setFrom(compileClasspath);
            parameters.getSkipClasses().set(skipClasses);
            parameters.getOutputFile().set(getOutputFile());
        });
    }

    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public FileCollection getMainSources() {
        return mainSources;
    }

    public void setMainSources(FileCollection mainSources) {
        this.mainSources = mainSources;
    }

    @Classpath
    public FileCollection getCompileClasspath() {
        return compileClasspath;
    }

    public void setCompileClasspath(FileCollection compileClasspath) {
        this.compileClasspath = compileClasspath;
    }

    abstract static class TransportTestExistWorkAction implements WorkAction<Parameters> {

        @Inject
        public TransportTestExistWorkAction() {}

        @Override
        public void execute() {
            Set<String> classesToSkip = loadClassesToSkip();

            TransportClassesScanner transportClassesScanner = new TransportClassesScanner(classesToSkip);
            Set<String> transportClasses = transportClassesScanner.findTransportClasses(
                getParameters().getMainSources().getFiles(),
                getParameters().getCompileClasspath().getFiles()
            );

            Path path = getParameters().getOutputFile().getAsFile().get().toPath();
            try {
                Files.createDirectories(path.getParent());

                try (PrintWriter out = new PrintWriter(Files.newOutputStream(path))) {
                    for (String transportClass : transportClasses) {
                        out.println(escapeDollar(transportClass));
                    }
                }
            } catch (IOException e) {
                throw new GradleException("Cannot create transport classes file", e);
            }

        }

        private String escapeDollar(String transportClass) {
            // coverage tools like jacoco use a simplistic regex model
            return transportClass.replaceAll("\\$.*", "*");
        }
    }

    private static Set<String> loadClassesToSkip() {
        var inputStream = FindTransportClassesTask.class.getResourceAsStream("/" + MISSING_TRANSPORT_TESTS_FILE);
        var reader = new BufferedReader(new InputStreamReader(inputStream));
        return reader.lines()
            .filter(l -> l.startsWith("//") == false)
            .filter(l -> l.trim().equals("") == false)
            .collect(Collectors.toSet());
    }

    interface Parameters extends WorkParameters {
        ConfigurableFileCollection getMainSources();

        ConfigurableFileCollection getCompileClasspath();

        SetProperty<String> getSkipClasses();

        RegularFileProperty getOutputFile();
    }

}
