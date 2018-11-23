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
import org.gradle.api.file.FileCollection;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.SkipWhenEmpty;
import org.gradle.api.tasks.TaskAction;

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
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TestingConventionsTasks extends DefaultTask {

    private static final String TEST_CLASS_SUFIX = "Tests";
    private static final String INTEG_TEST_CLASS_SUFIX = "IT";
    private static final String TEST_METHOD_PREFIX = "test";
    private final List<String> problems;
    /**
     * Are there tests to execute ? Accounts for @Ignore and @AwaitsFix
     */
    private Boolean activeTestsExists;

    private List<String> testClassNames;

    public TestingConventionsTasks() {
        setDescription("Tests various testing conventions");
        // Run only after everything is compiled
        Boilerplate.getJavaSourceSets(getProject()).all(sourceSet -> dependsOn(sourceSet.getClassesTaskName()));
        problems = new ArrayList<>();
    }

    @TaskAction
    public void test() throws IOException {
        activeTestsExists = false;

        try (URLClassLoader isolatedClassLoader = new URLClassLoader(
            getTestsClassPath().getFiles().stream().map(this::fileToUrl).toArray(URL[]::new)
        )) {
            List<? extends Class<?>> classes = getTestClassNames().stream()
                .map(name -> loadClassWithoutInitializing(name, isolatedClassLoader))
                .collect(Collectors.toList());

            Predicate<Class<?>> isStaticClass = clazz -> Modifier.isStatic(clazz.getModifiers());
            Predicate<Class<?>> isPublicClass = clazz -> Modifier.isPublic(clazz.getModifiers());
            Predicate<Class<?>> implementsNamingConvention = clazz -> clazz.getName().endsWith(TEST_CLASS_SUFIX) ||
                clazz.getName().endsWith(INTEG_TEST_CLASS_SUFIX);

            checkNoneExists(
                "Test classes implemented by inner classes will not run",
                classes.stream()
                    .filter(isStaticClass)
                    .filter(implementsNamingConvention.or(this::seemsLikeATest))
            );

            checkNoneExists(
                "Seem like test classes but don't match naming convention",
                classes.stream()
                    .filter(isStaticClass.negate())
                    .filter(isPublicClass)
                    .filter(this::seemsLikeATest)
                    .filter(implementsNamingConvention.negate())
            );
        }

        if (problems.isEmpty()) {
            getSuccessMarker().getParentFile().mkdirs();
            Files.write(getSuccessMarker().toPath(), new byte[]{}, StandardOpenOption.CREATE);
        } else {
            problems.forEach(getProject().getLogger()::error);
            throw new IllegalStateException("Testing conventions are not honored");
        }
    }

    @Input
    @SkipWhenEmpty
    public List<String> getTestClassNames() {
        if (testClassNames == null) {
            testClassNames = Boilerplate.getJavaSourceSets(getProject()).getByName("test").getOutput().getClassesDirs()
                .getFiles().stream()
                .filter(File::exists)
                .flatMap(testRoot -> walkPathAndLoadClasses(testRoot).stream())
                .collect(Collectors.toList());
        }
        return testClassNames;
    }

    @OutputFile
    public File getSuccessMarker() {
        return new File(getProject().getBuildDir(), "markers/" + getName());
    }

    private void checkNoneExists(String message, Stream<? extends Class<?>> stream) {
        List<Class<?>> entries = stream.collect(Collectors.toList());
        if (entries.isEmpty() == false) {
            problems.add(message + ":");
            entries.stream()
                .map(each -> "  * " + each.getName())
            .forEach(problems::add);
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

    private List<String> walkPathAndLoadClasses(File testRoot) {
        List<String> classes = new ArrayList<>();
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
