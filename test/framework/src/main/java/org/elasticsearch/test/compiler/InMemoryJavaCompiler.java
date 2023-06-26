/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.compiler;

import org.elasticsearch.test.PrivilegedOperations;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.tools.FileObject;
import javax.tools.ForwardingJavaFileManager;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileManager;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.ToolProvider;

/**
 * An in-memory java source code compiler. InMemoryJavaCompiler can be used for compiling source
 * code, represented as a CharSequence, to byte code, represented as a byte[].
 *
 * <p> The compiler will not use the file system at all, instead using a ByteArrayOutputStream for
 * storing the byte code.
 *
 * <p> Example:
 * <pre>{@code
 *     Map<String, CharSequence> sources = Map.of(
 *       "module-info",
 *       """
 *       module foo {
 *         exports p;
 *       }
 *       """,
 *       "p.Foo",
 *       """
 *       package p;
 *       public class Foo implements java.util.function.Supplier<String> {
 *        @Override public String get() {
 *          return "Hello World!";
 *         }
 *       }
 *       """
 *     );
 *     Map<String, byte[]> result = compile(sources);
 * }</pre>
 */
public class InMemoryJavaCompiler {
    private static class InMemoryJavaFileObject extends SimpleJavaFileObject {
        private final String className;
        private final CharSequence sourceCode;
        private final ByteArrayOutputStream byteCode;

        InMemoryJavaFileObject(String className, CharSequence sourceCode) {
            super(URI.create("string:///" + className.replace('.', '/') + Kind.SOURCE.extension), Kind.SOURCE);
            this.className = className;
            this.sourceCode = sourceCode;
            this.byteCode = new ByteArrayOutputStream();
        }

        @Override
        public CharSequence getCharContent(boolean ignoreEncodingErrors) {
            return sourceCode;
        }

        @Override
        public OutputStream openOutputStream() {
            return byteCode;
        }

        public byte[] getByteCode() {
            return byteCode.toByteArray();
        }

        public String getClassName() {
            return className;
        }
    }

    private static class FileManagerWrapper extends ForwardingJavaFileManager<JavaFileManager> {

        private final List<InMemoryJavaFileObject> files;

        FileManagerWrapper(List<InMemoryJavaFileObject> files) {
            super(getCompiler().getStandardFileManager(null, null, null));
            this.files = List.copyOf(files);
        }

        FileManagerWrapper(InMemoryJavaFileObject file) {
            super(getCompiler().getStandardFileManager(null, null, null));
            this.files = List.of(file);
        }

        public List<InMemoryJavaFileObject> getFiles() {
            return this.files;
        }

        @Override
        public JavaFileObject getJavaFileForOutput(Location location, String className, Kind kind, FileObject sibling) throws IOException {
            return files.stream()
                .filter(f -> f.getClassName().endsWith(className))
                .findFirst()
                .orElseThrow(newIOException(className, files));
        }

        static Supplier<IOException> newIOException(String className, List<InMemoryJavaFileObject> files) {
            return () -> new IOException(
                "Expected class with name " + className + ", in " + files.stream().map(InMemoryJavaFileObject::getClassName).toList()
            );
        }
    }

    /**
     * Compiles the classes with the given names and source code.
     *
     * @param sources A map of class names to source code
     * @param options Additional command line options (optional)
     * @throws RuntimeException If the compilation did not succeed
     * @return A Map containing the resulting byte code from the compilation, one entry per class name
     */
    public static Map<String, byte[]> compile(Map<String, CharSequence> sources, String... options) {
        var files = sources.entrySet().stream().map(e -> new InMemoryJavaFileObject(e.getKey(), e.getValue())).toList();
        try (FileManagerWrapper wrapper = new FileManagerWrapper(files)) {
            CompilationTask task = getCompilationTask(wrapper, options);

            boolean result = PrivilegedOperations.compilationTaskCall(task);
            if (result == false) {
                throw new RuntimeException("Could not compile " + sources.entrySet().stream().toList());
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not compile " + sources.entrySet().stream().toList());
        }

        return files.stream().collect(Collectors.toMap(InMemoryJavaFileObject::getClassName, InMemoryJavaFileObject::getByteCode));
    }

    /**
     * Compiles the class with the given name and source code.
     *
     * @param className The name of the class
     * @param sourceCode The source code for the class with name {@code className}
     * @param options Additional command line options (optional)
     * @throws RuntimeException If the compilation did not succeed
     * @return The resulting byte code from the compilation
     */
    public static byte[] compile(String className, CharSequence sourceCode, String... options) {
        InMemoryJavaFileObject file = new InMemoryJavaFileObject(className, sourceCode);
        try (FileManagerWrapper wrapper = new FileManagerWrapper(file)) {
            CompilationTask task = getCompilationTask(wrapper, options);

            boolean result = PrivilegedOperations.compilationTaskCall(task);
            if (result == false) {
                throw new RuntimeException("Could not compile " + className + " with source code " + sourceCode);
            }
        } catch (IOException e) {
            throw new RuntimeException("Could not compile " + className + " with source code " + sourceCode);
        }
        return file.getByteCode();
    }

    private static JavaCompiler getCompiler() {
        return ToolProvider.getSystemJavaCompiler();
    }

    private static CompilationTask getCompilationTask(FileManagerWrapper wrapper, String... options) {
        return getCompiler().getTask(null, wrapper, null, List.of(options), null, wrapper.getFiles());
    }
}
