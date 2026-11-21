/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.precommit;

import org.elasticsearch.gradle.internal.conventions.precommit.PrecommitTask;
import org.elasticsearch.gradle.internal.conventions.problems.ElasticsearchBuildProblems;
import org.gradle.api.GradleException;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.EmptyFileVisitor;
import org.gradle.api.file.FileTree;
import org.gradle.api.file.FileVisitDetails;
import org.gradle.api.logging.Logging;
import org.gradle.api.problems.Problem;
import org.gradle.api.problems.ProblemReporter;
import org.gradle.api.problems.Problems;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.IgnoreEmptyDirectories;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;
import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;
import org.gradle.workers.WorkQueue;
import org.gradle.workers.WorkerExecutor;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.Nullable;
import javax.inject.Inject;

@CacheableTask
public abstract class TestingConventionsCheckTask extends PrecommitTask {

    @Input
    abstract ListProperty<String> getSuffixes();

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

    /**
     * Whether test classes may run with the plain {@code RandomizedRunner}. For the projects that cannot use {@code ESRandomizedRunner},
     * because they do not depend on the Elasticsearch test infrastructure.
     */
    @Input
    public abstract Property<Boolean> getAllowPlainRandomizedRunner();

    @Inject
    public abstract WorkerExecutor getWorkerExecutor();

    public void baseClass(String qualifiedClassname) {
        getBaseClasses().add(qualifiedClassname);
    }

    public void suffix(String suffix) {
        getSuffixes().add(suffix);
    }

    @TaskAction
    void validate() {
        WorkQueue workQueue = getWorkerExecutor().classLoaderIsolation(spec -> spec.getClasspath().from(getClasspath()));
        workQueue.submit(TestingConventionsCheckWorkAction.class, parameters -> {
            parameters.getClasspath().setFrom(getClasspath());
            parameters.getClassDirectories().setFrom(getTestClassesDirs());
            parameters.getBaseClassesNames().set(getBaseClasses().get());
            parameters.getSuffixes().set(getSuffixes().get());
            parameters.getAllowPlainRandomizedRunner().set(getAllowPlainRandomizedRunner());
        });
    }

    abstract static class TestingConventionsCheckWorkAction implements WorkAction<Parameters> {

        private static final String JUNIT3_TEST_METHOD_PREFIX = "test";
        private static final String PLAIN_RANDOMIZED_RUNNER = "com.carrotsearch.randomizedtesting.RandomizedRunner";
        private static final Predicate<Class<?>> isAbstractClass = clazz -> Modifier.isAbstract(clazz.getModifiers());
        private static final Predicate<Class<?>> isPublicClass = clazz -> Modifier.isPublic(clazz.getModifiers());
        private static final Predicate<Class<?>> isStaticClass = clazz -> Modifier.isStatic(clazz.getModifiers());
        private static final Predicate<Class<?>> testClassDefaultPredicate = isAbstractClass.negate()
            .and(isPublicClass)
            .and(isStaticClass.negate());

        @Inject
        public TestingConventionsCheckWorkAction() {}

        @Inject
        public abstract Problems getProblems();

        @Override
        public void execute() {
            ClassLoadingFileVisitor fileVisitor = new ClassLoadingFileVisitor();
            getParameters().getClassDirectories().getAsFileTree().visit(fileVisitor);
            checkTestClasses(
                fileVisitor.getTestClassCandidates(),
                getParameters().getBaseClassesNames().get(),
                getParameters().getSuffixes().get()
            );
        }

        private void checkTestClasses(List<String> testClassesCandidates, List<String> baseClassNames, List<String> suffixes) {
            var testClassCandidates = testClassesCandidates.stream()
                .map(className -> loadClassWithoutInitializing(className, getClass().getClassLoader()))
                .collect(Collectors.toCollection(ArrayList::new));
            var baseClasses = baseClassNames.stream()
                .map(className -> loadClassWithoutInitializing(className, getClass().getClassLoader()))
                .toList();
            testClassCandidates.removeAll(baseClasses);
            var allTestClassCandidates = List.copyOf(testClassCandidates);
            var matchingBaseClass = getBaseClassMatching(testClassCandidates, baseClasses);
            assertMatchesSuffix(suffixes, matchingBaseClass);
            testClassCandidates.removeAll(matchingBaseClass);
            assertNoMissmatchingTest(testClassCandidates);
            if (getParameters().getAllowPlainRandomizedRunner().get() == false) {
                assertNoPlainRandomizedRunner(allTestClassCandidates);
            }
        }

        private void assertNoMissmatchingTest(List<? extends Class<?>> testClassesCandidate) {
            var mismatchingBaseClasses = testClassesCandidate.stream()
                .filter(testClassDefaultPredicate)
                .filter(TestingConventionsCheckWorkAction::seemsLikeATest)
                .toList();
            if (mismatchingBaseClasses.isEmpty() == false) {
                ProblemReporter reporter = getProblems().getReporter();
                List<Problem> problems = new ArrayList<>();
                for (Class<?> clazz : mismatchingBaseClasses) {
                    problems.add(
                        reporter.create(
                            ElasticsearchBuildProblems.TESTING_CONVENTIONS.problemId(getProblems(), "Missing test base class"),
                            spec -> spec.contextualLabel(clazz.getName()).solution("Make the test class extend a supported base class")
                        )
                    );
                }
                throw reporter.throwing(
                    new GradleException(
                        "Following test classes do not extend any supported base class:\n\t"
                            + mismatchingBaseClasses.stream().map(c -> c.getName()).collect(Collectors.joining("\n\t"))
                    ),
                    problems
                );
            }
        }

        private void assertMatchesSuffix(List<String> suffixes, List<Class> matchingBaseClass) {
            var matchingBaseClassNotMatchingSuffix = matchingBaseClass.stream()
                .filter(c -> suffixes.stream().allMatch(s -> c.getName().endsWith(s) == false))
                .toList();
            if (matchingBaseClassNotMatchingSuffix.isEmpty() == false) {
                ProblemReporter reporter = getProblems().getReporter();
                List<Problem> problems = new ArrayList<>();
                for (Class<?> clazz : matchingBaseClassNotMatchingSuffix) {
                    problems.add(
                        reporter.create(
                            ElasticsearchBuildProblems.TESTING_CONVENTIONS.problemId(getProblems(), "Invalid test class suffix"),
                            spec -> spec.contextualLabel(clazz.getName()).solution("Rename the test class to use the correct suffix")
                        )
                    );
                }
                throw reporter.throwing(
                    new GradleException(
                        "Following test classes do not match naming convention to use suffix "
                            + suffixes.stream().map(s -> "'" + s + "'").collect(Collectors.joining(" or "))
                            + ":\n\t"
                            + matchingBaseClassNotMatchingSuffix.stream().map(c -> c.getName()).collect(Collectors.joining("\n\t"))
                    ),
                    problems
                );
            }
        }

        private List<Class> getBaseClassMatching(List<? extends Class<?>> testClassCandidates, List<? extends Class<?>> baseClasses) {
            Predicate<Class<?>> extendsBaseClass = clazz -> baseClasses.stream().anyMatch(baseClass -> baseClass.isAssignableFrom(clazz));
            return testClassCandidates.stream()
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
            return method.getName().startsWith(JUNIT3_TEST_METHOD_PREFIX)
                && Modifier.isStatic(method.getModifiers()) == false
                && method.getReturnType().equals(Void.TYPE);
        }

        private static boolean isAnnotated(Method method, Class<?> annotation) {
            return Stream.of(method.getAnnotations())
                .anyMatch(presentAnnotation -> annotation.isAssignableFrom(presentAnnotation.getClass()));
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

        /**
         * Tests must not run with the plain {@code RandomizedRunner}: on the JUnit Platform, the Vintage engine filters its tests in a way
         * that does not work for the parameterized ones, which {@code ESRandomizedRunner} compensates for. {@code ESTestCase} and the other
         * Elasticsearch base classes are annotated with it, and {@code @RunWith} is inherited, so this only catches the classes that
         * extend {@code LuceneTestCase} or {@code RandomizedTest} directly, or declare their own plain {@code @RunWith}.
         */
        private void assertNoPlainRandomizedRunner(List<? extends Class<?>> testClassCandidates) {
            var offendingClasses = testClassCandidates.stream()
                .filter(testClassDefaultPredicate)
                .filter(clazz -> PLAIN_RANDOMIZED_RUNNER.equals(getRunnerName(clazz)))
                .filter(TestingConventionsCheckWorkAction::seemsLikeATest)
                .toList();
            if (offendingClasses.isEmpty() == false) {
                ProblemReporter reporter = getProblems().getReporter();
                List<Problem> problems = new ArrayList<>();
                for (Class<?> clazz : offendingClasses) {
                    problems.add(
                        reporter.create(
                            ElasticsearchBuildProblems.TESTING_CONVENTIONS.problemId(getProblems(), "Plain randomized runner"),
                            spec -> spec.contextualLabel(clazz.getName())
                                .solution("Extend ESTestCase or annotate the class with @RunWith(ESRandomizedRunner.class)")
                        )
                    );
                }
                throw reporter.throwing(
                    new GradleException(
                        "Following test classes run with the plain RandomizedRunner, use ESRandomizedRunner instead:\n\t"
                            + offendingClasses.stream().map(c -> c.getName()).collect(Collectors.joining("\n\t"))
                    ),
                    problems
                );
            }
        }

        /**
         * The name of the runner of the test class, taking inherited {@code @RunWith} annotations into account, or {@code null} if there
         * is none. It is looked up by name, as the JUnit classes are not necessarily on the classpath of this check.
         */
        @Nullable
        private static String getRunnerName(Class<?> clazz) {
            for (var annotation : clazz.getAnnotations()) {
                if (annotation.annotationType().getName().equals("org.junit.runner.RunWith")) {
                    try {
                        return ((Class<?>) annotation.annotationType().getMethod("value").invoke(annotation)).getName();
                    } catch (ReflectiveOperationException e) {
                        throw new IllegalStateException("Failed to inspect the runner of " + clazz.getName(), e);
                    }
                }
            }
            return null;
        }
    }

    private static final class ClassLoadingFileVisitor extends EmptyFileVisitor {
        private static final String CLASS_POSTFIX = ".class";
        private List<String> fullQualifiedClassNames = new ArrayList<>();

        @Override
        public void visitFile(FileVisitDetails fileVisitDetails) {
            String fileName = fileVisitDetails.getName();
            if (fileName.endsWith(CLASS_POSTFIX)) {
                String packageName = Arrays.stream(fileVisitDetails.getRelativePath().getSegments())
                    .takeWhile(s -> s.equals(fileName) == false)
                    .collect(Collectors.joining("."));
                String simpleClassName = fileName.replace(CLASS_POSTFIX, "");
                String fullQualifiedClassName = packageName + (packageName.isEmpty() ? "" : ".") + simpleClassName;
                fullQualifiedClassNames.add(fullQualifiedClassName);
            }
        }

        public List<String> getTestClassCandidates() {
            return fullQualifiedClassNames;
        }
    }

    interface Parameters extends WorkParameters {
        ConfigurableFileCollection getClassDirectories();

        ConfigurableFileCollection getClasspath();

        ListProperty<String> getSuffixes();

        ListProperty<String> getBaseClassesNames();

        Property<Boolean> getAllowPlainRandomizedRunner();
    }
}
