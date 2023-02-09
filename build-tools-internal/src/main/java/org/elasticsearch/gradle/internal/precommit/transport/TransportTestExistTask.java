/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit.transport;

import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitTask;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileCollection;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.ListProperty;
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
import org.objectweb.asm.ClassReader;

import java.io.File;
import java.util.Optional;
import java.util.Set;
import javax.inject.Inject;

/**
 * Runs LoggerUsageCheck on a set of directories.
 */
@CacheableTask
public abstract class TransportTestExistTask extends PrecommitTask {

    private FileCollection classpath;

    private final ListProperty<FileCollection> classesDirs;

    private ObjectFactory objectFactory;

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
        return classesDirs.get().stream().reduce(FileCollection::plus).orElse(objectFactory.fileCollection()).filter(File::exists);
    }

    public void addSourceSet(SourceSet sourceSet) {
        classesDirs.add(sourceSet.getOutput().getClassesDirs());
    }

    abstract static class TransportTestExistWorkAction implements WorkAction<Parameters> {

        private  final Logger logger = Logging.getLogger(TransportTestExistTask.class);


        private final ExecOperations execOperations;

        @Inject
        public TransportTestExistWorkAction(ExecOperations execOperations) {
            this.execOperations = execOperations;
        }

        @Override
        public void execute() {
            logger.info("xxxx" +getParameters().getClasspath());

            var pathSeparator = System.getProperty("path.separator");
            var paths = getParameters().getClassDirectories().getAsPath().split(pathSeparator);
            String mainClasses = mainClasses(paths);
            String testClasses = testClasses(paths);
            ClassHierarchyScanner transportClassesScanner =
                new ClassHierarchyScanner("org/elasticsearch/common/io/stream/Writeable");
            ClassReaders.forEachClassesInPath(mainClasses, cr -> cr.accept(transportClassesScanner, ClassReader.SKIP_CODE));

            ClassHierarchyScanner transportTestsScanner =
                new ClassHierarchyScanner("org/elasticsearch/common/io/stream/AbstractWireTestCase");
            ClassReaders.forEachClassesInPath(testClasses, cr -> cr.accept(transportTestsScanner, ClassReader.SKIP_CODE));

//            Set<String> transportClasses = transportClassesScanner.subClassesOf();
//            Set<String> transportTestClasses = transportTestsScanner.subClassesOf();
//            System.out.println("xxx"+transportClasses);
//            for (String transportClass : transportClasses) {
//                findTest(transportClass, transportTestClasses);
//            }
        }

        private void findTest(String transportClass, Set<String> transportTestClasses) {
            Optional<String> any = transportTestClasses.stream().filter(p -> p.contains(transportClass)).findAny();
            if(any.isPresent() == false){
                System.out.println("Missing test for class "+transportClass);
            } else {
                System.out.println("Test "+ any.get()+ " for class "+transportClass);
            }

        }

        private String testClasses(String[] paths) {
            return find(paths, "test");
        }

        private String mainClasses(String[] paths) {
            return find(paths, "main");
        }
    }

    private static String find(String[] paths, String suffix) {
        var separator = File.separatorChar;
        for (String path : paths) {
            if (path.contains("java" + separator + suffix)) {
                return path;
            }
        }
        return null;
    }

    interface Parameters extends WorkParameters {
        ConfigurableFileCollection getClassDirectories();

        ConfigurableFileCollection getClasspath();
    }

}
