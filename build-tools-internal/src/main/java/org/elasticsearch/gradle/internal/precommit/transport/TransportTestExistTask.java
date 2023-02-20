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
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.SetProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.TaskAction;
import org.gradle.process.ExecOperations;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkQueue;
import org.gradle.workers.WorkerExecutor;
import org.objectweb.asm.ClassReader;

import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

/**
 * Runs LoggerUsageCheck on a set of directories.
 */
@CacheableTask
public abstract class TransportTestExistTask extends PrecommitTask {

    private FileCollection classpath;

    private final ListProperty<FileCollection> classesDirs;

    private ObjectFactory objectFactory;
    private FileCollection mainSources;
    private FileCollection testSources;
    private Configuration compileClasspath;
    private Configuration testClasspath;
    Set<String> skipClasses = new HashSet<>();

    @Inject
    public TransportTestExistTask(ObjectFactory objectFactory) {
        this.classesDirs = objectFactory.listProperty(FileCollection.class);
        this.objectFactory = objectFactory;
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

    public void setMainSources(FileCollection mainSources) {
        this.mainSources = mainSources;
    }

    public void setTestSources(FileCollection testSources) {
        this.testSources = testSources;
    }

    public void setCompileClasspath(Configuration compileClasspath) {
        this.compileClasspath = compileClasspath;
    }

    public void setTestClasspath(Configuration testClasspath) {
        this.testClasspath = testClasspath;
    }

    public void setSkipClass(String className, String reason) {
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
            Set<File> mainClasses = getParameters().getMainSources().getFiles();
            Set<File> testClasses = getParameters().getTestSources().getFiles();
            Set<File> compileClassPath = getParameters().getCompileClasspath().getFiles();
            Set<File> testClassPath = getParameters().getTestClasspath().getFiles();
            Set<String> skipClasses = getParameters().getSkipClasses().get();

            // this is always run.. should be cached?
            ClassHierarchyScanner compileClassPathScanner = new ClassHierarchyScanner();
            ClassReaders.forEach(compileClassPath, cr -> cr.accept(compileClassPathScanner, ClassReader.SKIP_CODE));

            ClassHierarchyScanner transportClassesScanner = new ClassHierarchyScanner();
            ClassReaders.forEach(mainClasses, cr -> { cr.accept(transportClassesScanner, ClassReader.SKIP_CODE); });

            String writeableClassName = "org/elasticsearch/common/io/stream/Writeable";
            Map<String, String> subclassesOfWriteable = compileClassPathScanner.allFoundSubclasses(
                Map.of(writeableClassName, writeableClassName)
            );
            Set<String> transportClasses = transportClassesScanner.getConcreteSubclasses(subclassesOfWriteable);

            // this is always run.. should be cached?
            ClassHierarchyScanner testClassPathScanner = new ClassHierarchyScanner();
            ClassReaders.forEach(testClassPath, cr -> cr.accept(testClassPathScanner, ClassReader.SKIP_CODE));

            ClassHierarchyScanner transportTestsScanner = new ClassHierarchyScanner();
            ClassReaders.forEach(testClasses, cr -> cr.accept(transportTestsScanner, ClassReader.SKIP_CODE));
            String transportTestCase = "org/elasticsearch/test/AbstractWireTestCase";
            Map<String, String> subclassesOfTransportTestCase = testClassPathScanner.allFoundSubclasses(
                Map.of(transportTestCase, transportTestCase)
            );
            Set<String> transportTestClasses = transportTestsScanner.getConcreteSubclasses(subclassesOfTransportTestCase);
            System.out.println(transportTestClasses);
            System.out.println(skipClasses);
            transportClasses.removeAll(skipClasses);
            System.out.println(transportTestClasses);

            List<String> classesWithoutTests = new ArrayList<>();
            for (String transportClass : transportClasses) {
                findTest(transportClass, transportTestClasses, classesWithoutTests);
            }
            if (classesWithoutTests.size() > 0) {
                throw new GradleException(
                    "Missing tests for classes\n"
                        + "tasks.named(\"transportTestExistCheck\").configure { task ->\n"
                        + classesWithoutTests.stream()
                            .map(s -> "task.skipTest(\"" + s + "\",\"missing test\")")
                            .collect(Collectors.joining("\n"))
                        + "\n}"
                );
            }
        }

        private void findTest(String transportClass, Set<String> transportTestClasses, List<String> classesWithoutTests) {
            Optional<String> any = transportTestClasses.stream().filter(p -> p.contains(transportClass)).findAny();
            if (any.isPresent() == false) {
                classesWithoutTests.add(transportClass);
                System.out.println("Test missing for class " + transportClass);
            } else {
                System.out.println("Test " + any.get() + " for class " + transportClass);
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
