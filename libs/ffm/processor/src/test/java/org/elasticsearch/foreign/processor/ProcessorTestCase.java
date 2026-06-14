/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.foreign.processor;

import junit.framework.TestCase;

import java.io.File;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

abstract class ProcessorTestCase extends TestCase {

    record CompilationResult(boolean success, List<String> notes, List<String> errors, Path outputDir) {
        /** Reads a resource file (relative to the compilation output dir) as a String, or returns null if missing. */
        String readResource(String relativePath) throws Exception {
            if (outputDir == null) {
                return null;
            }
            Path resourcePath = outputDir.resolve(relativePath);
            if (Files.exists(resourcePath) == false) {
                return null;
            }
            return Files.readString(resourcePath);
        }

        /** Loads a class from the compilation output directory (with initialization). Returns null if not found. */
        Class<?> loadClass(String className) throws Exception {
            return loadClass(className, true);
        }

        /**
         * Loads a class from the compilation output directory without triggering class initialization.
         * Use this when the class has a {@code <clinit>} that requires native libraries at runtime.
         */
        Class<?> loadClassNoInit(String className) throws Exception {
            return loadClass(className, false);
        }

        private Class<?> loadClass(String className, boolean initialize) throws Exception {
            if (outputDir == null) {
                return null;
            }
            URLClassLoader cl = new URLClassLoader(new URL[] { outputDir.toUri().toURL() }, ProcessorTestCase.class.getClassLoader());
            try {
                return Class.forName(className, initialize, cl);
            } catch (ClassNotFoundException e) {
                return null;
            }
        }
    }

    /**
     * Compiles a {@code module-info.java} together with a single source file as a named module.
     * The processor runs in the same way as {@link #compile}, but the source is placed in a named module
     * so that the processor can inspect the module's directives. Dependencies (e.g. {@code org.elasticsearch.foreign})
     * are picked up from the test runtime classpath via {@code --module-path}.
     */
    protected CompilationResult compileModule(String moduleName, String moduleInfo, String className, String source) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull("System Java compiler not available", compiler);

        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();

        try {
            Path tmp = Files.createTempDirectory("native-lib-gen-mod-test");
            Path moduleSrcRoot = tmp.resolve("src");
            Path moduleDir = moduleSrcRoot.resolve(moduleName);
            int lastDot = className.lastIndexOf('.');
            String pkg = lastDot < 0 ? "" : className.substring(0, lastDot);
            String simple = lastDot < 0 ? className : className.substring(lastDot + 1);
            Path pkgDir = pkg.isEmpty() ? moduleDir : moduleDir.resolve(pkg.replace('.', '/'));
            Files.createDirectories(pkgDir);
            Files.writeString(moduleDir.resolve("module-info.java"), moduleInfo);
            Files.writeString(pkgDir.resolve(simple + ".java"), source);

            Path outputDir = Files.createTempDirectory("native-lib-gen-mod-out");
            String testClasspath = System.getProperty("java.class.path");

            try (var fileManager = compiler.getStandardFileManager(diagnostics, null, null)) {
                fileManager.setLocation(StandardLocation.CLASS_OUTPUT, List.of(outputDir.toFile()));

                List<String> options = new ArrayList<>();
                options.add("--module-source-path");
                options.add(moduleSrcRoot.toString());
                options.add("--module-path");
                options.add(modulePathForDeps(testClasspath));
                // The processor itself lives on the classpath; -processor finds it via the system classloader.
                options.add("-classpath");
                options.add(testClasspath);
                options.add("-processor");
                options.add(LibraryProcessor.class.getName());

                Iterable<? extends JavaFileObject> sources = fileManager.getJavaFileObjectsFromPaths(
                    List.of(moduleDir.resolve("module-info.java"), pkgDir.resolve(simple + ".java"))
                );

                JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, diagnostics, options, null, sources);
                boolean success = task.call();

                List<String> notes = new ArrayList<>();
                List<String> errors = new ArrayList<>();
                for (Diagnostic<? extends JavaFileObject> d : diagnostics.getDiagnostics()) {
                    String msg = d.getMessage(null);
                    if (d.getKind() == Diagnostic.Kind.NOTE) {
                        notes.add(msg);
                    } else if (d.getKind() == Diagnostic.Kind.ERROR) {
                        errors.add(msg);
                    }
                }

                return new CompilationResult(success, notes, errors, outputDir);
            }
        } catch (Exception e) {
            throw new RuntimeException("Compilation setup failed", e);
        }
    }

    /**
     * Filters the test classpath down to entries that look like modules (contain a {@code module-info.class}).
     * Those entries are placed on {@code --module-path} so that the compiler can resolve {@code requires}
     * directives in the test module's {@code module-info.java}.
     */
    private static String modulePathForDeps(String testClasspath) {
        StringBuilder modulePath = new StringBuilder();
        for (String entry : testClasspath.split(File.pathSeparator)) {
            if (entry.isEmpty()) {
                continue;
            }
            Path p = Paths.get(entry);
            if (containsModuleInfo(p) == false) {
                continue;
            }
            if (modulePath.length() > 0) {
                modulePath.append(File.pathSeparator);
            }
            modulePath.append(entry);
        }
        return modulePath.toString();
    }

    private static boolean containsModuleInfo(Path entry) {
        if (Files.isDirectory(entry)) {
            return Files.exists(entry.resolve("module-info.class"));
        }
        if (Files.isRegularFile(entry) && entry.toString().endsWith(".jar")) {
            try (var jar = new java.util.jar.JarFile(entry.toFile())) {
                return jar.getEntry("module-info.class") != null;
            } catch (Exception e) {
                return false;
            }
        }
        return false;
    }

    protected CompilationResult compile(String className, String source) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull("System Java compiler not available", compiler);

        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        String processorClasspath = System.getProperty("java.class.path");

        JavaFileObject sourceFile = new SimpleJavaFileObject(
            URI.create("string:///" + className.replace('.', '/') + ".java"),
            JavaFileObject.Kind.SOURCE
        ) {
            @Override
            public CharSequence getCharContent(boolean ignoreEncodingErrors) {
                return source;
            }
        };

        try {
            Path outputDir = Files.createTempDirectory("native-lib-gen-test");
            try (var fileManager = compiler.getStandardFileManager(diagnostics, null, null)) {
                fileManager.setLocation(StandardLocation.CLASS_OUTPUT, List.of(outputDir.toFile()));

                List<String> options = new ArrayList<>();
                options.add("-classpath");
                options.add(processorClasspath);
                options.add("-processor");
                options.add(LibraryProcessor.class.getName());

                JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, diagnostics, options, null, List.of(sourceFile));
                boolean success = task.call();

                List<String> notes = new ArrayList<>();
                List<String> errors = new ArrayList<>();
                for (Diagnostic<? extends JavaFileObject> d : diagnostics.getDiagnostics()) {
                    String msg = d.getMessage(null);
                    if (d.getKind() == Diagnostic.Kind.NOTE) {
                        notes.add(msg);
                    } else if (d.getKind() == Diagnostic.Kind.ERROR) {
                        errors.add(msg);
                    }
                }

                return new CompilationResult(success, notes, errors, outputDir);
            }
        } catch (Exception e) {
            throw new RuntimeException("Compilation setup failed", e);
        }
    }
}
