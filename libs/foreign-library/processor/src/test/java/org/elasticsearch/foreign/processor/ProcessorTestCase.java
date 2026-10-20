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
import org.elasticsearch.foreign.LibraryProvider;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardLocation;
import javax.tools.ToolProvider;

abstract class ProcessorTestCase extends TestCase {

    /**
     * The outcome of a single in-process {@code javac} run over test-supplied sources with
     * {@link LibraryProcessor} attached: the collected diagnostics plus a class output directory the
     * generated classes and SPI service files were written to.
     */
    static final class CompilationResult {
        private final boolean success;
        private final List<String> notes;
        private final List<String> warnings;
        private final List<String> errors;
        private final Path outputDir;
        private ClassLoader classLoader;

        CompilationResult(boolean success, List<String> notes, List<String> warnings, List<String> errors, Path outputDir) {
            this.success = success;
            this.notes = notes;
            this.warnings = warnings;
            this.errors = errors;
            this.outputDir = outputDir;
        }

        boolean success() {
            return success;
        }

        List<String> notes() {
            return notes;
        }

        List<String> warnings() {
            return warnings;
        }

        List<String> errors() {
            return errors;
        }

        Path outputDir() {
            return outputDir;
        }

        /**
         * A single class loader over the compilation output, created lazily and cached. Loading every
         * generated class and SPI provider through one loader means their {@link Class} objects share a
         * namespace (so reflective identity comparisons hold, and one class is never initialized twice
         * in two isolated loaders).
         */
        ClassLoader classLoader() {
            if (classLoader == null) {
                if (outputDir == null) {
                    classLoader = ProcessorTestCase.class.getClassLoader();
                } else {
                    try {
                        classLoader = new URLClassLoader(new URL[] { outputDir.toUri().toURL() }, ProcessorTestCase.class.getClassLoader());
                    } catch (MalformedURLException e) {
                        throw new RuntimeException(e);
                    }
                }
            }
            return classLoader;
        }

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
            try {
                return Class.forName(className, initialize, classLoader());
            } catch (ClassNotFoundException e) {
                return null;
            }
        }

        /**
         * Loads the generated implementation of the given {@code @LibrarySpecification} interface exactly
         * as production does — through the SPI-registered {@link LibraryProvider} discovered by
         * {@link ServiceLoader} — and returns a {@link LoadedLibrary} handle for driving its
         * {@code @Function} methods. The package-private {@code $Impl} is never touched directly, so
         * behavioral tests need no {@code setAccessible} / cross-package reflection.
         */
        LoadedLibrary loadLibrary(String interfaceName) throws Exception {
            ClassLoader cl = classLoader();
            Class<?> iface = Class.forName(interfaceName, false, cl);
            for (LibraryProvider<?> provider : ServiceLoader.load(LibraryProvider.class, cl)) {
                if (provider.libraryClass() == iface) {
                    Object instance = provider.load();
                    assertNotNull("LibraryProvider for " + interfaceName + " returned no instance on this platform", instance);
                    return new LoadedLibrary(iface, instance);
                }
            }
            throw new AssertionError("No generated LibraryProvider found for " + interfaceName);
        }
    }

    /**
     * A handle to an SPI-loaded native library instance for invoking its {@code @Function} interface
     * methods reflectively in behavioral tests. Methods are resolved on the public library interface
     * rather than the hidden {@code $Impl}, so no {@code setAccessible} is required — the runtime object
     * merely being an instance of the package-private impl is irrelevant to reflective dispatch through
     * a public interface method.
     */
    static final class LoadedLibrary {
        private final Class<?> iface;
        private final Object instance;

        LoadedLibrary(Class<?> iface, Object instance) {
            this.iface = iface;
            this.instance = instance;
        }

        /**
         * Invokes the uniquely-named {@code @Function} method with the given arguments, unwrapping any
         * exception thrown by the native binding so callers observe the real cause rather than an
         * {@link InvocationTargetException} wrapper.
         */
        Object call(String methodName, Object... args) throws Throwable {
            try {
                return method(methodName).invoke(instance, args);
            } catch (InvocationTargetException e) {
                throw e.getCause();
            }
        }

        /**
         * Invokes {@code methodName} and asserts it throws {@code expected} (as the unwrapped cause),
         * returning the caught throwable for further assertions.
         */
        <T extends Throwable> T expectThrows(Class<T> expected, String methodName, Object... args) {
            try {
                call(methodName, args);
            } catch (Throwable actual) {
                if (expected.isInstance(actual)) {
                    return expected.cast(actual);
                }
                throw new AssertionError("Expected " + expected.getName() + " but got: " + actual, actual);
            }
            throw new AssertionError("Expected " + expected.getName() + " but nothing was thrown");
        }

        /**
         * Instantiates a public no-arg class from the same compilation output (e.g. an {@code @Upcall}
         * implementation the test passes into a native callback). The class is public with a public
         * constructor, so this needs no {@code setAccessible}.
         */
        Object newInstance(String className) throws Exception {
            return Class.forName(className, true, iface.getClassLoader()).getConstructor().newInstance();
        }

        private Method method(String methodName) {
            Method found = null;
            for (Method m : iface.getMethods()) {
                if (m.getName().equals(methodName)) {
                    if (found != null) {
                        throw new IllegalArgumentException("Multiple methods named '" + methodName + "' on " + iface.getName());
                    }
                    found = m;
                }
            }
            if (found == null) {
                throw new IllegalArgumentException("No method named '" + methodName + "' on " + iface.getName());
            }
            return found;
        }
    }

    /** Compiles a single source file. Convenience wrapper over {@link #compile(Map)}. */
    protected CompilationResult compile(String className, String source) {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put(className, source);
        return compile(sources);
    }

    /**
     * Compiles a single-source {@code @LibrarySpecification} library, asserts the compilation succeeded,
     * and returns the SPI-loaded {@link LoadedLibrary} handle for driving its {@code @Function} methods.
     */
    protected LoadedLibrary loadLibrary(String interfaceName, String source) throws Exception {
        Map<String, String> sources = new LinkedHashMap<>();
        sources.put(interfaceName, source);
        return loadLibrary(sources, interfaceName);
    }

    /**
     * Compiles multiple sources (a library plus any companion classes it references, such as a
     * comparator or fallback adapter), asserts success, and returns the SPI-loaded {@link LoadedLibrary}
     * handle for the named {@code @LibrarySpecification} interface.
     */
    protected LoadedLibrary loadLibrary(Map<String, String> sources, String interfaceName) throws Exception {
        CompilationResult result = compile(sources);
        assertTrue("Expected compilation to succeed but got errors: " + result.errors(), result.success());
        return result.loadLibrary(interfaceName);
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
