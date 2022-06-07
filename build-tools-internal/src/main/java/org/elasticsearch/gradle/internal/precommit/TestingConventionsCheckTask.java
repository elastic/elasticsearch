/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.logging.Logging;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.*;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkQueue;
import org.gradle.workers.WorkerExecutor;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import javax.inject.Inject;

@CacheableTask
public abstract class TestingConventionsCheckTask extends PrecommitTask {

    @Input
    abstract Property<String> getSuffix();

    @Internal
    abstract ConfigurableFileCollection getTestClassesDirs();

    @InputFiles
    @SkipWhenEmpty
    @IgnoreEmptyDirectories
    @PathSensitive(PathSensitivity.RELATIVE)
    public FileTree getTestClasses() {
        return getTestClassesDirs().getAsFileTree().matching(pattern -> pattern.include("**/*.class"));
    }

    @Classpath
    abstract ConfigurableFileCollection getClasspath();

    @Input
    public abstract ListProperty<String> getBaseClasses();

    @Inject
    abstract public WorkerExecutor getWorkerExecutor();

    public void baseClass(String qualifiedClassname) {
        getBaseClasses().add(qualifiedClassname);
    }

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

        private static final String JUNIT3_TEST_METHOD_PREFIX = "test";
        private static final Predicate<Class<?>> isAbstractClass = clazz -> Modifier.isAbstract(clazz.getModifiers());
        private static final Predicate<Class<?>> isPublicClass = clazz -> Modifier.isPublic(clazz.getModifiers());
        private static final Predicate<Class<?>> isStaticClass = clazz -> Modifier.isStatic(clazz.getModifiers());

        private static final Predicate<Class<?>> testClassDefaultPredicate = isAbstractClass.negate()
            .and(isPublicClass)
            .and(isStaticClass.negate());

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
            checkTestClasses(testClassesCandidates, getParameters().getBaseClassesNames().get(), getParameters().getSuffix().get());
        }

        private void checkTestClasses(List<String> testClassesCandidates, List<String> baseClassNames, String suffix) {
            List<? extends Class<?>> testClassesCandidate = testClassesCandidates.stream()
                .map(className -> loadClassWithoutInitializing(className, getClass().getClassLoader()))
                .collect(Collectors.toCollection(ArrayList::new));
            List<Class> matchingBaseClass = getBaseClassMatching(testClassesCandidate, baseClassNames);
            assertMatchesSuffix(suffix, matchingBaseClass);
            testClassesCandidate.removeAll(matchingBaseClass);
            assertNoMissmatchingTest(testClassesCandidate);
        }

        private void assertNoMissmatchingTest(List<? extends Class<?>> testClassesCandidate) {
            var mismatchingBaseClasses = testClassesCandidate.stream()
                .filter(testClassDefaultPredicate)
                .filter(TestingConventionsCheckWorkAction::seemsLikeATest)
                .collect(Collectors.toList());
            if (mismatchingBaseClasses.isEmpty() == false) {
                throw new GradleException(
                    "Following test classes do not extend any supported base class:\n\t"
                        + mismatchingBaseClasses.stream().map(c -> c.getName()).collect(Collectors.joining("\n\t"))
                );
            }
        }

        private void assertMatchesSuffix(String suffix, List<Class> matchingBaseClass) {
            // ensure base class matching do match suffix
            var matchingBaseClassNotMatchingSuffix = matchingBaseClass.stream()
                .filter(c -> c.getName().endsWith(suffix) == false)
                .collect(Collectors.toList());
            if (matchingBaseClassNotMatchingSuffix.isEmpty() == false) {
                throw new GradleException(
                    "Following test classes do not match naming convention to use suffix '"
                        + suffix
                        + "':\n\t"
                        + matchingBaseClassNotMatchingSuffix.stream().map(c -> c.getName()).collect(Collectors.joining("\n\t"))
                );
            }
        }

        private List<Class> getBaseClassMatching(List<? extends Class<?>> testClassesCandidate, List<String> baseClassNames) {
            List<? extends Class<?>> baseClasses = baseClassNames.stream()
                .map(className -> loadClassWithoutInitializing(className, getClass().getClassLoader()))
                .toList();

            Predicate<Class<?>> extendsBaseClass = clazz -> baseClasses.stream().anyMatch(baseClass -> baseClass.isAssignableFrom(clazz));
            return testClassesCandidate.stream()
                .filter(testClassDefaultPredicate)
                .filter(extendsBaseClass)
                .filter(TestingConventionsCheckWorkAction::seemsLikeATest)
                .collect(Collectors.toList());
        }

        private static boolean seemsLikeATest(Class<?> clazz) {
            try {
                Class<?> junitTest = loadClassWithoutInitializing("org.junit.Assert", clazz.getClassLoader());
                if (junitTest.isAssignableFrom(clazz)) {
                    Logging.getLogger(TestingConventionsCheckWorkAction.class)
                        .debug("{} is a test because it extends {}", clazz.getName(), junitTest.getName());
                    return true;
                }

                Class<?> junitAnnotation = loadClassWithoutInitializing("org.junit.Test", clazz.getClassLoader());
                for (Method method : clazz.getMethods()) {
                    if (matchesTestMethodNamingConvention(method)) {
                        Logging.getLogger(TestingConventionsCheckWorkAction.class)
                            .debug("{} is a test because it has method named '{}'", clazz.getName(), method.getName());
                        return true;
                    }
                    if (isAnnotated(method, junitAnnotation)) {
                        Logging.getLogger(TestingConventionsCheckWorkAction.class)
                            .debug(
                                "{} is a test because it has method '{}' annotated with '{}'",
                                clazz.getName(),
                                method.getName(),
                                junitAnnotation.getName()
                            );
                        return true;
                    }
                }

                return false;
            } catch (NoClassDefFoundError e) {
                // Include the message to get more info to get more a more useful message when running Gradle without -s
                throw new IllegalStateException("Failed to inspect class " + clazz.getName() + ". Missing class? " + e.getMessage(), e);
            }
        }

        private static boolean matchesTestMethodNamingConvention(Method method) {
            return method.getName().startsWith(JUNIT3_TEST_METHOD_PREFIX) && Modifier.isStatic(method.getModifiers()) == false;
        }

        private static boolean isAnnotated(Method method, Class<?> annotation) {
            for (Annotation presentAnnotation : method.getAnnotations()) {
                if (annotation.isAssignableFrom(presentAnnotation.getClass())) {
                    return true;
                }
            }
            return false;
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

        private static Class<?> loadClassWithoutInitializing(String name, ClassLoader classLoader) {
            try {
                return Class.forName(
                    name,
                    // Don't initialize the class to save time. Not needed for this test and this doesn't share a VM with any other tests.
                    false,
                    classLoader
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
