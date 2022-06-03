/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitTask;
import org.gradle.api.Action;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;

import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.TaskAction;
import org.gradle.internal.classloader.ClassLoaderSpec;
import org.gradle.workers.ClassLoaderWorkerSpec;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkQueue;
import org.gradle.workers.WorkerExecutor;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Modifier;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class TestingConventionsCheckTask extends PrecommitTask {

    @Input
    abstract Property<String> getSuffix();

    @InputFiles
    abstract ConfigurableFileCollection getTestClassesDirs();

    @Classpath
    abstract ConfigurableFileCollection getClasspath();

    @Input
    public abstract ListProperty<String> getBaseClasses();

    @Inject
    abstract public WorkerExecutor getWorkerExecutor();

    @TaskAction
    void validate() {
        WorkQueue workQueue = getWorkerExecutor().classLoaderIsolation(spec -> spec.getClasspath().from(getClasspath()));
        workQueue.submit(TestingConventionsCheckWorkAction.class, parameters -> {
            parameters.getClasspath().setFrom(getClasspath());
            parameters.getClassDirectories().setFrom(getTestClassesDirs());
            parameters.getBaseClassesNames().set(getBaseClasses().get());
            parameters.getSuffix().set(getSuffix().get());
        });
    }

    abstract static class TestingConventionsCheckWorkAction implements WorkAction<Parameters> {

        @Inject
        public TestingConventionsCheckWorkAction() {}

        @Override
        public void execute() {
            List<String> testClassesCandidates = getParameters().getClassDirectories()
                    .getFiles()
                    .stream()
                    .filter(File::exists)
                    .flatMap(testRoot -> walkPathAndLoadClasses(testRoot).stream())
                    .collect(Collectors.toList());
            checkBaseClassMatching(testClassesCandidates, getParameters().getBaseClassesNames().get());
        }

        private void checkBaseClassMatching(List<String> testClassesCandidates, List<String> baseClassNames) {
             List<? extends Class<?>> testClassesCandidate = testClassesCandidates.stream().map(className ->
             loadClassWithoutInitializing(className)).toList();
             List<? extends Class<?>> baseClasses = baseClassNames.stream().map(className ->
             loadClassWithoutInitializing(className)).toList();

            Predicate<Class<?>> isStaticClass = clazz -> Modifier.isStatic(clazz.getModifiers());
            Predicate<Class<?>> isPublicClass = clazz -> Modifier.isPublic(clazz.getModifiers());
            Predicate<Class<?>> isAbstractClass = clazz -> Modifier.isAbstract(clazz.getModifiers());
            Predicate<Class<?>> extendsBaseClass = clazz -> baseClasses.stream().
                    anyMatch(baseClass -> baseClass.isAssignableFrom(clazz));

            String mismatchingClassNames = testClassesCandidate.stream()
                    .filter(isPublicClass)
                    .filter(isAbstractClass.negate())
                    .filter(extendsBaseClass.negate())
                    .map(c -> c.getName()).collect(Collectors.joining("\n\t"));

            if(mismatchingClassNames.isEmpty() == false) {
                throw new GradleException("Following test classes do not extend any supported base class:\n\t" + mismatchingClassNames);
            }
        }

        private List<String> walkPathAndLoadClasses(File testRoot) {
            var classes = new ArrayList<String>();
            try {
                Files.walkFileTree(testRoot.toPath(), new FileVisitor<Path>() {
                    private String packageName;

                    @Override
                    public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                        // First we visit the root directory
                        if (packageName == null) {
                            // And it package is empty string regardless of the directory name
                            packageName = "";
                        } else {
                            packageName += dir.getFileName() + ".";
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                        // Go up one package by jumping back to the second to last '.'
                        packageName = packageName.substring(0, 1 + packageName.lastIndexOf('.', packageName.length() - 2));
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                        String filename = file.getFileName().toString();
                        if (filename.endsWith(".class")) {
                            String className = filename.substring(0, filename.length() - ".class".length());
                            classes.add(packageName + className);
                        }
                        return FileVisitResult.CONTINUE;
                    }

                    @Override
                    public FileVisitResult visitFileFailed(Path file, IOException exc) throws IOException {
                        throw new IOException("Failed to visit " + file, exc);
                    }
                });
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
            return classes;
        }

        private Class<?> loadClassWithoutInitializing(String name) {
            try {
                return Class.forName(
                    name,
                    // Don't initialize the class to save time. Not needed for this test and this doesn't share a VM with any other tests.
                    false,
                    getClass().getClassLoader()
                );
            } catch (ClassNotFoundException e) {
                throw new RuntimeException("Failed to load class " + name + ". Incorrect classpath?", e);
            }
        }
    }

    interface Parameters extends WorkParameters {
        ConfigurableFileCollection getClassDirectories();

        ConfigurableFileCollection getClasspath();

        Property<String> getSuffix();

        ListProperty<String> getBaseClassesNames();
    }
}
