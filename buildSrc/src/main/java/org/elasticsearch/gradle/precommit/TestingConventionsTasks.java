/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.gradle.precommit;

import org.elasticsearch.gradle.tool.Boilerplate;
import org.gradle.api.DefaultTask;
import org.gradle.api.Task;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.FileTree;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.testing.Test;
import org.gradle.api.tasks.util.PatternFilterable;

import java.io.File;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.reflect.InvocationTargetException;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestingConventionsTasks extends DefaultTask {

    private static final String TEST_CLASS_SUFIX = "Tests";
    private static final String INTEG_TEST_CLASS_SUFIX = "IT";
    private static final String TEST_METHOD_PREFIX = "test";

    /**
     * Are there tests to execute ? Accounts for @Ignore and @AwaitsFix
     */
    private Boolean activeTestsExists;

    private Map<String, File> testClassNames;

    public TestingConventionsTasks() {
        setDescription("Tests various testing conventions");
        // Run only after everything is compiled
        Boilerplate.getJavaSourceSets(getProject()).all(sourceSet -> dependsOn(sourceSet.getClassesTaskName()));
    }

    @TaskAction
    public void doCheck() throws IOException {
        activeTestsExists = false;
        final String problems;

        try (URLClassLoader isolatedClassLoader = new URLClassLoader(
            getTestsClassPath().getFiles().stream().map(this::fileToUrl).toArray(URL[]::new)
        )) {
            Predicate<Class<?>> isStaticClass = clazz -> Modifier.isStatic(clazz.getModifiers());
            Predicate<Class<?>> isPublicClass = clazz -> Modifier.isPublic(clazz.getModifiers());
            Predicate<Class<?>> implementsNamingConvention = clazz ->
                clazz.getName().endsWith(TEST_CLASS_SUFIX) ||
                    clazz.getName().endsWith(INTEG_TEST_CLASS_SUFIX);

            Map<File, ? extends Class<?>> classes = getTestClassNames().entrySet().stream()
                .collect(Collectors.toMap(
                    Map.Entry::getValue,
                    entry -> loadClassWithoutInitializing(entry.getKey(), isolatedClassLoader))
                );

            FileTree allTestClassFiles = getProject().files(
                classes.values().stream()
                    .filter(isStaticClass.negate())
                    .filter(isPublicClass)
                    .filter(implementsNamingConvention)
                    .map(clazz -> testClassNames.get(clazz.getName()))
                    .collect(Collectors.toList())
            ).getAsFileTree();

            final Map<String, Set<File>> classFilesPerRandomizedTestingTask = classFilesPerRandomizedTestingTask(allTestClassFiles);
            final Map<String, Set<File>> classFilesPerGradleTestTask = classFilesPerGradleTestTask();

            problems = collectProblems(
                checkNoneExists(
                    "Test classes implemented by inner classes will not run",
                    classes.values().stream()
                        .filter(isStaticClass)
                        .filter(implementsNamingConvention.or(this::seemsLikeATest))
                ),
                checkNoneExists(
                    "Seem like test classes but don't match naming convention",
                    classes.values().stream()
                        .filter(isStaticClass.negate())
                        .filter(isPublicClass)
                        .filter(this::seemsLikeATest)
                        .filter(implementsNamingConvention.negate())
                ),
                checkNoneExists(
                    "Test classes are not included in any enabled task (" +
                        Stream.concat(
                            classFilesPerRandomizedTestingTask.keySet().stream(),
                            classFilesPerGradleTestTask.keySet().stream()
                        ).collect(Collectors.joining(",")) + ")",
                    allTestClassFiles.getFiles().stream()
                        .filter(testFile ->
                            classFilesPerRandomizedTestingTask.values().stream()
                                .anyMatch(fileSet -> fileSet.contains(testFile)) == false &&
                                classFilesPerGradleTestTask.values().stream()
                                    .anyMatch(fileSet -> fileSet.contains(testFile)) == false
                        )
                        .map(classes::get)
                )
            );
        }

        if (problems.isEmpty()) {
            getLogger().error(problems);
            throw new IllegalStateException("Testing conventions are not honored");
        } else {
            getSuccessMarker().getParentFile().mkdirs();
            Files.write(getSuccessMarker().toPath(), new byte[]{}, StandardOpenOption.CREATE);
        }
    }


    private String collectProblems(String... problems) {
        return Stream.of(problems)
            .map(String::trim)
            .filter(String::isEmpty)
            .map(each -> each + "\n")
            .collect(Collectors.joining());
    }


    @Input
    public Map<String, Set<File>> classFilesPerRandomizedTestingTask(FileTree testClassFiles) {
        return
            Stream.concat(
                getProject().getTasks().withType(getRandomizedTestingTask()).stream(),
                // Look at sub-projects too. As sometimes tests are implemented in parent but ran in sub-projects against
                // different configurations
                getProject().getSubprojects().stream().flatMap(subproject ->
                    subproject.getTasks().withType(getRandomizedTestingTask()).stream()
                )
            )
            .filter(Task::getEnabled)
            .collect(Collectors.toMap(
                Task::getPath,
                task -> testClassFiles.matching(getRandomizedTestingPatternSet(task)).getFiles()
            ));
    }

    @Input
    public Map<String, Set<File>> classFilesPerGradleTestTask() {
        return Stream.concat(
            getProject().getTasks().withType(Test.class).stream(),
            getProject().getSubprojects().stream().flatMap(subproject ->
                subproject.getTasks().withType(Test.class).stream()
            )
        )
            .filter(Task::getEnabled)
            .collect(Collectors.toMap(
                Task::getPath,
                task -> task.getCandidateClassFiles().getFiles()
            ));
    }

    @SuppressWarnings("unchecked")
    private PatternFilterable getRandomizedTestingPatternSet(Task task) {
        try {
            if (
                getRandomizedTestingTask().isAssignableFrom(task.getClass()) == false
            ) {
                throw new IllegalStateException("Expected " + task + " to be RandomizedTestingTask or Test but it was " + task.getClass());
            }
            Method getPatternSet = task.getClass().getMethod("getPatternSet");
            return (PatternFilterable) getPatternSet.invoke(task);
        } catch (NoSuchMethodException e) {
            throw new IllegalStateException("Expecte task to have a `patternSet` " + task, e);
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new IllegalStateException("Failed to get pattern set from task" + task, e);
        }
    }

    @SuppressWarnings("unchecked")
    private Class<? extends Task> getRandomizedTestingTask() {
        try {
            return (Class<? extends Task>) Class.forName("com.carrotsearch.gradle.junit4.RandomizedTestingTask");
        } catch (ClassNotFoundException | ClassCastException e) {
            throw new IllegalStateException("Failed to load randomized testing class", e);
        }
    }

    @Input
    @SkipWhenEmpty
    public Map<String, File> getTestClassNames() {
        if (testClassNames == null) {
            testClassNames = Boilerplate.getJavaSourceSets(getProject()).getByName("test").getOutput().getClassesDirs()
                .getFiles().stream()
                .filter(File::exists)
                .flatMap(testRoot -> walkPathAndLoadClasses(testRoot).entrySet().stream())
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        }
        return testClassNames;
    }

    @OutputFile
    public File getSuccessMarker() {
        return new File(getProject().getBuildDir(), "markers/" + getName());
    }

    private String checkNoneExists(String message, Stream<? extends Class<?>> stream) {
        String problem = stream
            .map(each -> "  * " + each.getName())
            .collect(Collectors.joining("\n"));
        if (problem.isEmpty() == false) {
            return message + ":\n" + problem;
        } else{
            return "";
        }
    }

    private boolean seemsLikeATest(Class<?> clazz) {
        try {
            ClassLoader classLoader = clazz.getClassLoader();
            Class<?> junitTest;
            try {
                junitTest = classLoader.loadClass("junit.framework.Test");
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("Could not load junit.framework.Test. It's expected that this class is " +
                    "available on the tests classpath");
            }
            if (junitTest.isAssignableFrom(clazz)) {
                getLogger().info("{} is a test because it extends junit.framework.Test", clazz.getName());
                return true;
            }
            for (Method method : clazz.getMethods()) {
                if (matchesTestMethodNamingConvention(clazz, method)) return true;
                if (isAnnotated(clazz, method, junitTest)) return true;
            }
            return false;
        } catch (NoClassDefFoundError e) {
            throw new IllegalStateException("Failed to inspect class " + clazz.getName(), e);
        }
    }

    private boolean matchesTestMethodNamingConvention(Class<?> clazz, Method method) {
        if (method.getName().startsWith(TEST_METHOD_PREFIX) &&
            Modifier.isStatic(method.getModifiers()) == false &&
            method.getReturnType().equals(Void.class)
        ) {
            getLogger().info("{} is a test because it has method: {}", clazz.getName(), method.getName());
            return true;
        }
        return false;
    }

    private boolean isAnnotated(Class<?> clazz, Method method, Class<?> annotation) {
        for (Annotation presentAnnotation : method.getAnnotations()) {
            if (annotation.isAssignableFrom(presentAnnotation.getClass())) {
                getLogger().info("{} is a test because {} is annotated with junit.framework.Test",
                    clazz.getName(), method.getName()
                );
                return true;
            }
        }
        return false;
    }

    private FileCollection getTestsClassPath() {
        // This is doesn't need to be annotated with @Classpath because we only really care about the test source set
        return getProject().files(
            getProject().getConfigurations().getByName("testCompile").resolve(),
            Boilerplate.getJavaSourceSets(getProject())
                .stream()
                .flatMap(sourceSet -> sourceSet.getOutput().getClassesDirs().getFiles().stream())
                .collect(Collectors.toList())
        );
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
            return Class.forName(name,
                // Don't initialize the class to save time. Not needed for this test and this doesn't share a VM with any other tests.
                false,
                isolatedClassLoader
            );
        } catch (ClassNotFoundException e) {
            // Will not get here as the exception will be loaded by isolatedClassLoader
            throw new RuntimeException("Failed to load class " + name, e);
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
