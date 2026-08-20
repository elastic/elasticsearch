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

import org.elasticsearch.core.SuppressForbidden;

import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

abstract class ProcessorTestCase extends TestCase {

    record CompilationResult(boolean success, List<String> notes, List<String> warnings, List<String> errors, Path outputDir) {
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

    /** Compiles a single source file. Convenience wrapper over {@link #compile(Map)}. */
    protected CompilationResult compile(String className, String source) {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put(className, source);
        return compile(sources);
    }

    /**
     * Compiles multiple source files in a single compilation task. Useful for tests that pair an
     * annotated library declaration with a companion "usage" class that exercises the generated
     * code — the usage class can reference the generated {@code $Impl} directly by name because it
     * lives in the same package and is compiled in the same task.
     */
    @SuppressForbidden(
        reason = "StandardJavaFileManager.setLocation() requires java.io.File; no NIO alternative exists in the javax.tools API"
    )
    protected CompilationResult compile(Map<String, String> sources) {
        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull("System Java compiler not available", compiler);

        DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        String processorClasspath = System.getProperty("java.class.path");

        List<JavaFileObject> sourceFiles = new ArrayList<>();
        for (Map.Entry<String, String> entry : sources.entrySet()) {
            String className = entry.getKey();
            String source = entry.getValue();
            sourceFiles.add(
                new SimpleJavaFileObject(URI.create("string:///" + className.replace('.', '/') + ".java"), JavaFileObject.Kind.SOURCE) {
                    @Override
                    public CharSequence getCharContent(boolean ignoreEncodingErrors) {
                        return source;
                    }
                }
            );
        }

        try {
            Path outputDir = Files.createTempDirectory(Path.of(System.getProperty("java.io.tmpdir")), "native-lib-gen-test");
            try (var fileManager = compiler.getStandardFileManager(diagnostics, null, null)) {
                fileManager.setLocation(StandardLocation.CLASS_OUTPUT, List.of(outputDir.toFile())); // required by javax.tools API

                List<String> options = new ArrayList<>();
                options.add("-classpath");
                options.add(processorClasspath);
                options.add("-processor");
                options.add(LibraryProcessor.class.getName());

                JavaCompiler.CompilationTask task = compiler.getTask(null, fileManager, diagnostics, options, null, sourceFiles);
                boolean success = task.call();

                List<String> notes = new ArrayList<>();
                List<String> warnings = new ArrayList<>();
                List<String> errors = new ArrayList<>();
                for (Diagnostic<? extends JavaFileObject> d : diagnostics.getDiagnostics()) {
                    String msg = d.getMessage(null);
                    if (d.getKind() == Diagnostic.Kind.NOTE) {
                        notes.add(msg);
                    } else if (d.getKind() == Diagnostic.Kind.WARNING || d.getKind() == Diagnostic.Kind.MANDATORY_WARNING) {
                        warnings.add(msg);
                    } else if (d.getKind() == Diagnostic.Kind.ERROR) {
                        errors.add(msg);
                    }
                }

                return new CompilationResult(success, notes, warnings, errors, outputDir);
            }
        } catch (Exception e) {
            throw new RuntimeException("Compilation setup failed", e);
        }
    }
}
