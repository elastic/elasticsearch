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
import org.gradle.api.tasks.TaskAction;

import java.io.File;
import java.io.IOException;
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
import java.util.stream.Collectors;

public class TestingConventionsTasks extends DefaultTask {

    private Boolean activeTestsExists;

    private List<String> testClassNames;

    public TestingConventionsTasks() {
        setDescription("Tests various testing conventions");
        // Run only after everything is compiled
        Boilerplate.getJavaSourceSets(getProject()).all(sourceSet -> dependsOn(sourceSet.getClassesTaskName()));
    }

    @TaskAction
    public void test() throws IOException {

        try (URLClassLoader isolatedClassLoader = new URLClassLoader(
            getTestsClassPath().getFiles().stream().map(this::fileToUrl).toArray(URL[]::new)
        )) {
            getTestClassNames().stream()
                .map(name -> loadClassWithoutInitializing(name, isolatedClassLoader))
                .collect(Collectors.toList());
        }

        activeTestsExists = true;

        getSuccessMarker().getParentFile().mkdirs();
        Files.write(getSuccessMarker().toPath(), new byte[]{}, StandardOpenOption.CREATE);
    }

    @Input
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
                public final FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
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
                public final FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    // Go up one package by jumping back to the second to last '.'
                    packageName = packageName.substring(0, 1 + packageName.lastIndexOf('.', packageName.length() - 2));
                    return FileVisitResult.CONTINUE;
                }

                @Override
                public final FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    String filename = file.getFileName().toString();
                    if (filename.endsWith(".class") && filename.contains("$") == false) {
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
