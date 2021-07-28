/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.internal.precommit;

import groovy.lang.Closure;
import org.elasticsearch.gradle.util.GradleUtils;
import org.elasticsearch.gradle.internal.conventions.util.Util;
import org.gradle.api.DefaultTask;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Task;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.file.ProjectLayout;
import org.gradle.api.tasks.Classpath;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.testing.Test;

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestingConventionsTasks extends DefaultTask {

    private static final String TEST_METHOD_PREFIX = "test";

    private Map<String, File> testClassNames;

    private final NamedDomainObjectContainer<TestingConventionRule> naming;
    private ProjectLayout projectLayout;

    @Inject
    public TestingConventionsTasks(ProjectLayout projectLayout) {
        this.projectLayout = projectLayout;
        setDescription("Tests various testing conventions");
        // Run only after everything is compiled
        GradleUtils.getJavaSourceSets(getProject()).all(sourceSet -> dependsOn(sourceSet.getOutput().getClassesDirs()));
        naming = getProject().container(TestingConventionRule.class);
    }

    @Input
    public Map<String, Set<File>> getClassFilesPerEnabledTask() {
        return getProject().getTasks()
            .withType(Test.class)
            .stream()
            .filter(Task::getEnabled)
            .collect(Collectors.toMap(Task::getPath, task -> task.getCandidateClassFiles().getFiles()));
    }

    @Input
    public Map<String, File> getTestClassNames() {
        if (testClassNames == null) {
            testClassNames = Util.getJavaTestSourceSet(getProject())
                .get()
                .getOutput()
                .getClassesDirs()
                .getFiles()
                .stream()
                .filter(File::exists)
                .flatMap(testRoot -> walkPathAndLoadClasses(testRoot).entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        return testClassNames;
    }

    @Input
    public NamedDomainObjectContainer<TestingConventionRule> getNaming() {
        return naming;
    }

    @OutputFile
    public File getSuccessMarker() {
        return new File(getProject().getBuildDir(), "markers/" + getName());
    }

    public void naming(Closure<?> action) {
        naming.configure(action);
    }

    @Input
    public Set<String> getMainClassNamedLikeTests() {
        SourceSetContainer javaSourceSets = GradleUtils.getJavaSourceSets(getProject());
        if (javaSourceSets.findByName(SourceSet.MAIN_SOURCE_SET_NAME) == null) {
            // some test projects don't have a main source set
            return Collections.emptySet();
        }
        return javaSourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME)
            .getOutput()
            .getClassesDirs()
            .getAsFileTree()
            .getFiles()
            .stream()
            .filter(file -> file.getName().endsWith(".class"))
            .map(File::getName)
            .map(name -> name.substring(0, name.length() - 6))
            .filter(this::implementsNamingConvention)
            .collect(Collectors.toSet());
    }

    @TaskAction
    public void doCheck() throws IOException {
        final String problems;

        try (
            URLClassLoader isolatedClassLoader = new URLClassLoader(
                getTestsClassPath().getFiles().stream().map(this::fileToUrl).toArray(URL[]::new)
            )
        ) {
            Predicate<Class<?>> isStaticClass = clazz -> Modifier.isStatic(clazz.getModifiers());
            Predicate<Class<?>> isPublicClass = clazz -> Modifier.isPublic(clazz.getModifiers());
            Predicate<Class<?>> isAbstractClass = clazz -> Modifier.isAbstract(clazz.getModifiers());

            final Map<File, ? extends Class<?>> classes = getTestClassNames().entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getValue, entry -> loadClassWithoutInitializing(entry.getKey(), isolatedClassLoader)));

            final FileTree allTestClassFiles = projectLayout.files(
                classes.values()
                    .stream()
                    .filter(isStaticClass.negate())
                    .filter(isPublicClass)
                    .filter((Predicate<Class<?>>) this::implementsNamingConvention)
                    .map(clazz -> testClassNames.get(clazz.getName()))
                    .collect(Collectors.toList())
            ).getAsFileTree();

            final Map<String, Set<File>> classFilesPerTask = getClassFilesPerEnabledTask();

            final Set<File> testSourceSetFiles = Util.getJavaTestSourceSet(getProject()).get().getRuntimeClasspath().getFiles();
            final Map<String, Set<Class<?>>> testClassesPerTask = classFilesPerTask.entrySet()
                .stream()
                .filter(entry -> testSourceSetFiles.containsAll(entry.getValue()))
                .collect(
                    Collectors.toMap(
                        Map.Entry::getKey,
                        entry -> entry.getValue()
                            .stream()
                            .map(classes::get)
                            .filter(this::implementsNamingConvention)
                            .collect(Collectors.toSet())
                    )
                );

            final Map<String, Set<Class<?>>> suffixToBaseClass;
            if (classes.isEmpty()) {
                // Don't load base classes if we don't have any tests.
                // This allows defaults to be configured for projects that don't have any tests
                //
                suffixToBaseClass = Collections.emptyMap();
            } else {
                suffixToBaseClass = naming.stream()
                    .collect(
                        Collectors.toMap(
                            TestingConventionRule::getSuffix,
                            rule -> rule.getBaseClasses()
                                .stream()
                                .map(each -> loadClassWithoutInitializing(each, isolatedClassLoader))
                                .collect(Collectors.toSet())
                        )
                    );
            }

            problems = collectProblems(
                checkNoneExists(
                    "Test classes implemented by inner classes will not run",
                    classes.values()
                        .stream()
                        .filter(isStaticClass)
                        .filter(isPublicClass)
                        .filter(((Predicate<Class<?>>) this::implementsNamingConvention).or(this::seemsLikeATest))
                ),
                checkNoneExists(
                    "Seem like test classes but don't match naming convention",
                    classes.values()
                        .stream()
                        .filter(isStaticClass.negate())
                        .filter(isPublicClass)
                        .filter(isAbstractClass.negate())
                        .filter(this::seemsLikeATest) // TODO when base classes are set, check for classes that extend them
                        .filter(((Predicate<Class<?>>) this::implementsNamingConvention).negate())
                ),
                // TODO: check for non public classes that seem like tests
                // TODO: check for abstract classes that implement the naming conventions
                // No empty enabled tasks
                collectProblems(
                    testClassesPerTask.entrySet()
                        .stream()
                        .map(entry -> checkAtLeastOneExists("test class included in task " + entry.getKey(), entry.getValue().stream()))
                        .sorted()
                        .collect(Collectors.joining("\n"))
                ),
                checkNoneExists(
                    "Test classes are not included in any enabled task ("
                        + classFilesPerTask.keySet().stream().collect(Collectors.joining(","))
                        + ")",
                    allTestClassFiles.getFiles()
                        .stream()
                        .filter(testFile -> classFilesPerTask.values().stream().anyMatch(fileSet -> fileSet.contains(testFile)) == false)
                        .map(classes::get)
                ),
                collectProblems(suffixToBaseClass.entrySet().stream().filter(entry -> entry.getValue().isEmpty() == false).map(entry -> {
                    return checkNoneExists(
                        "Tests classes with suffix `"
                            + entry.getKey()
                            + "` should extend "
                            + entry.getValue().stream().map(Class::getName).collect(Collectors.joining(" or "))
                            + " but the following classes do not",
                        classes.values()
                            .stream()
                            .filter(clazz -> clazz.getName().endsWith(entry.getKey()))
                            .filter(clazz -> entry.getValue().stream().anyMatch(test -> test.isAssignableFrom(clazz)) == false)
                    );
                }).sorted().collect(Collectors.joining("\n"))),
                // TODO: check that the testing tasks are included in the right task based on the name ( from the rule )
                checkNoneExists("Classes matching the test naming convention should be in test not main", getMainClassNamedLikeTests())
            );
        }

        if (problems.isEmpty()) {
            getSuccessMarker().getParentFile().mkdirs();
            Files.write(getSuccessMarker().toPath(), new byte[] {}, StandardOpenOption.CREATE);
        } else {
            getLogger().error(problems);
            throw new IllegalStateException(String.format("Testing conventions [%s] are not honored", problems));
        }
    }

    private String collectProblems(String... problems) {
        return Stream.of(problems).map(String::trim).filter(s -> s.isEmpty() == false).collect(Collectors.joining("\n"));
    }

    private String checkNoneExists(String message, Stream<? extends Class<?>> stream) {
        String problem = stream.map(each -> "  * " + each.getName()).sorted().collect(Collectors.joining("\n"));
        if (problem.isEmpty() == false) {
            return message + ":\n" + problem;
        } else {
            return "";
        }
    }

    private String checkNoneExists(String message, Set<? extends String> candidates) {
        String problem = candidates.stream().map(each -> "  * " + each).sorted().collect(Collectors.joining("\n"));
        if (problem.isEmpty() == false) {
            return message + ":\n" + problem;
        } else {
            return "";
        }
    }

    private String checkAtLeastOneExists(String message, Stream<? extends Class<?>> stream) {
        if (stream.findAny().isPresent()) {
            return "";
        } else {
            return "Expected at least one " + message + ", but found none.";
        }
    }

    private boolean seemsLikeATest(Class<?> clazz) {
        try {
            ClassLoader classLoader = clazz.getClassLoader();

            Class<?> junitTest = loadClassWithoutInitializing("org.junit.Assert", classLoader);
            if (junitTest.isAssignableFrom(clazz)) {
                getLogger().debug("{} is a test because it extends {}", clazz.getName(), junitTest.getName());
                return true;
            }

            Class<?> junitAnnotation = loadClassWithoutInitializing("org.junit.Test", classLoader);
            for (Method method : clazz.getMethods()) {
                if (matchesTestMethodNamingConvention(method)) {
                    getLogger().debug("{} is a test because it has method named '{}'", clazz.getName(), method.getName());
                    return true;
                }
                if (isAnnotated(method, junitAnnotation)) {
                    getLogger().debug(
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

    private boolean implementsNamingConvention(Class<?> clazz) {
        Objects.requireNonNull(clazz);
        return implementsNamingConvention(clazz.getName());
    }

    private boolean implementsNamingConvention(String className) {
        if (naming.stream().map(TestingConventionRule::getSuffix).anyMatch(suffix -> className.endsWith(suffix))) {
            getLogger().debug("{} is a test because it matches the naming convention", className);
            return true;
        }
        return false;
    }

    private boolean matchesTestMethodNamingConvention(Method method) {
        return method.getName().startsWith(TEST_METHOD_PREFIX) && Modifier.isStatic(method.getModifiers()) == false;
    }

    private boolean isAnnotated(Method method, Class<?> annotation) {
        for (Annotation presentAnnotation : method.getAnnotations()) {
            if (annotation.isAssignableFrom(presentAnnotation.getClass())) {
                return true;
            }
        }
        return false;
    }

    @Classpath
    public FileCollection getTestsClassPath() {
        return Util.getJavaTestSourceSet(getProject()).get().getRuntimeClasspath();
    }

    private Map<String, File> walkPathAndLoadClasses(File testRoot) {
        Map<String, File> classes = new HashMap<>();
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
                        classes.put(packageName + className, file.toFile());
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

    private Class<?> loadClassWithoutInitializing(String name, ClassLoader isolatedClassLoader) {
        try {
            return Class.forName(
                name,
                // Don't initialize the class to save time. Not needed for this test and this doesn't share a VM with any other tests.
                false,
                isolatedClassLoader
            );
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("Failed to load class " + name + ". Incorrect test runtime classpath?", e);
        }
    }

    private URL fileToUrl(File file) {
        try {
            return file.toURI().toURL();
        } catch (MalformedURLException e) {
            throw new IllegalStateException(e);
        }
    }

}
