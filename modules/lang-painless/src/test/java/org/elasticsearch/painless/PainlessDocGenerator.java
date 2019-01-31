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

package org.elasticsearch.painless;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.painless.lookup.PainlessClass;
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessField;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessLookupBuilder;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.spi.Whitelist;

import java.io.IOException;
import java.io.PrintStream;
import java.lang.reflect.Modifier;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;

/**
 * Generates an API reference from the method and type whitelists in {@link PainlessLookup}.
 */
public class PainlessDocGenerator {

    private static final PainlessLookup PAINLESS_LOOKUP = PainlessLookupBuilder.buildFromWhitelists(Whitelist.BASE_WHITELISTS);
    private static final Logger logger = LogManager.getLogger(PainlessDocGenerator.class);
    private static final Comparator<PainlessField> FIELD_NAME = comparing(f -> f.javaField.getName());
    private static final Comparator<PainlessMethod> METHOD_NAME = comparing(m -> m.javaMethod.getName());
    private static final Comparator<PainlessMethod> METHOD_NUMBER_OF_PARAMS = comparing(m -> m.typeParameters.size());
    private static final Comparator<PainlessConstructor> CONSTRUCTOR_NUMBER_OF_PARAMS = comparing(m -> m.typeParameters.size());

    public static void main(String[] args) throws IOException {
        Path apiRootPath = PathUtils.get(args[0]);

        // Blow away the last execution and recreate it from scratch
        IOUtils.rm(apiRootPath);
        Files.createDirectories(apiRootPath);

        Path indexPath = apiRootPath.resolve("index.asciidoc");
        logger.info("Starting to write [index.asciidoc]");
        try (PrintStream indexStream = new PrintStream(
            Files.newOutputStream(indexPath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
            false, StandardCharsets.UTF_8.name())) {
            emitGeneratedWarning(indexStream);
            List<Class<?>> classes = PAINLESS_LOOKUP.getClasses().stream().sorted(
                    Comparator.comparing(Class::getCanonicalName)).collect(Collectors.toList());
            for (Class<?> clazz : classes) {
                PainlessClass struct = PAINLESS_LOOKUP.lookupPainlessClass(clazz);
                String canonicalClassName = PainlessLookupUtility.typeToCanonicalTypeName(clazz);

                if (clazz.isPrimitive()) {
                    // Primitives don't have methods to reference
                    continue;
                }
                if (clazz == def.class) {
                    // def is special but doesn't have any methods all of its own.
                    continue;
                }
                indexStream.print("include::");
                indexStream.print(canonicalClassName);
                indexStream.println(".asciidoc[]");

                Path typePath = apiRootPath.resolve(canonicalClassName + ".asciidoc");
                logger.info("Writing [{}.asciidoc]", canonicalClassName);
                try (PrintStream typeStream = new PrintStream(
                        Files.newOutputStream(typePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                        false, StandardCharsets.UTF_8.name())) {
                    emitGeneratedWarning(typeStream);
                    typeStream.print("[[");
                    emitAnchor(typeStream, clazz);
                    typeStream.print("]]++");
                    typeStream.print(canonicalClassName);
                    typeStream.println("++::");

                    Consumer<PainlessField> documentField = field -> PainlessDocGenerator.documentField(typeStream, field);
                    Consumer<PainlessMethod> documentMethod = method -> PainlessDocGenerator.documentMethod(typeStream, method);
                    Consumer<PainlessConstructor> documentConstructor =
                            constructor -> PainlessDocGenerator.documentConstructor(typeStream, constructor);
                    struct.staticFields.values().stream().sorted(FIELD_NAME).forEach(documentField);
                    struct.fields.values().stream().sorted(FIELD_NAME).forEach(documentField);
                    struct.staticMethods.values().stream().sorted(
                            METHOD_NAME.thenComparing(METHOD_NUMBER_OF_PARAMS)).forEach(documentMethod);
                    struct.constructors.values().stream().sorted(CONSTRUCTOR_NUMBER_OF_PARAMS).forEach(documentConstructor);
                    Map<String, Class<?>> inherited = new TreeMap<>();
                    struct.methods.values().stream().sorted(METHOD_NAME.thenComparing(METHOD_NUMBER_OF_PARAMS)).forEach(method -> {
                        if (method.targetClass == clazz) {
                            documentMethod(typeStream, method);
                        } else {
                            inherited.put(canonicalClassName, method.targetClass);
                        }
                    });

                    if (false == inherited.isEmpty()) {
                        typeStream.print("* Inherits methods from ");
                        boolean first = true;
                        for (Class<?> inheritsFrom : inherited.values()) {
                            if (first) {
                                first = false;
                            } else {
                                typeStream.print(", ");
                            }
                            typeStream.print("++");
                            emitStruct(typeStream, inheritsFrom);
                            typeStream.print("++");
                        }
                        typeStream.println();
                    }
                }
            }
        }
        logger.info("Done writing [index.asciidoc]");
    }

    private static void documentField(PrintStream stream, PainlessField field) {
        stream.print("** [[");
        emitAnchor(stream, field);
        stream.print("]]");

        if (Modifier.isStatic(field.javaField.getModifiers())) {
            stream.print("static ");
        }

        emitType(stream, field.typeParameter);
        stream.print(' ');

        String javadocRoot = javadocRoot(field);
        emitJavadocLink(stream, javadocRoot, field);
        stream.print('[');
        stream.print(field.javaField.getName());
        stream.print(']');

        if (javadocRoot.equals("java8")) {
            stream.print(" (");
            emitJavadocLink(stream, "java9", field);
            stream.print("[java 9])");
        }

        stream.println();
    }

    /**
     * Document a constructor.
     */
    private static void documentConstructor(PrintStream stream, PainlessConstructor constructor) {
        stream.print("* ++[[");
        emitAnchor(stream, constructor);
        stream.print("]]");

        String javadocRoot = javadocRoot(constructor.javaConstructor.getDeclaringClass());
        emitJavadocLink(stream, javadocRoot, constructor);
        stream.print('[');

        stream.print(constructorName(constructor));

        stream.print("](");
        boolean first = true;
        for (Class<?> arg : constructor.typeParameters) {
            if (first) {
                first = false;
            } else {
                stream.print(", ");
            }
            emitType(stream, arg);
        }
        stream.print(")++");

        if (javadocRoot.equals("java8")) {
            stream.print(" (");
            emitJavadocLink(stream, "java9", constructor);
            stream.print("[java 9])");
        }

        stream.println();
    }

    /**
     * Document a method.
     */
    private static void documentMethod(PrintStream stream, PainlessMethod method) {
        stream.print("* ++[[");
        emitAnchor(stream, method);
        stream.print("]]");

        if (method.targetClass == method.javaMethod.getDeclaringClass() && Modifier.isStatic(method.javaMethod.getModifiers())) {
            stream.print("static ");
        }

        emitType(stream, method.returnType);
        stream.print(' ');

        String javadocRoot = javadocRoot(method);
        emitJavadocLink(stream, javadocRoot, method);
        stream.print('[');

        stream.print(methodName(method));

        stream.print("](");
        boolean first = true;
        for (Class<?> arg : method.typeParameters) {
            if (first) {
                first = false;
            } else {
                stream.print(", ");
            }
            emitType(stream, arg);
        }
        stream.print(")++");

        if (javadocRoot.equals("java8")) {
            stream.print(" (");
            emitJavadocLink(stream, "java9", method);
            stream.print("[java 9])");
        }

        stream.println();
    }

    /**
     * Anchor text for a {@link PainlessClass}.
     */
    private static void emitAnchor(PrintStream stream, Class<?> clazz) {
        stream.print("painless-api-reference-");
        stream.print(PainlessLookupUtility.typeToCanonicalTypeName(clazz).replace('.', '-'));
    }

    /**
     * Anchor text for a {@link PainlessConstructor}.
     */
    private static void emitAnchor(PrintStream stream, PainlessConstructor constructor) {
        emitAnchor(stream, constructor.javaConstructor.getDeclaringClass());
        stream.print('-');
        stream.print(constructorName(constructor));
        stream.print('-');
        stream.print(constructor.typeParameters.size());
    }

    /**
     * Anchor text for a {@link PainlessMethod}.
     */
    private static void emitAnchor(PrintStream stream, PainlessMethod method) {
        emitAnchor(stream, method.targetClass);
        stream.print('-');
        stream.print(methodName(method));
        stream.print('-');
        stream.print(method.typeParameters.size());
    }

    /**
     * Anchor text for a {@link PainlessField}.
     */
    private static void emitAnchor(PrintStream stream, PainlessField field) {
        emitAnchor(stream, field.javaField.getDeclaringClass());
        stream.print('-');
        stream.print(field.javaField.getName());
    }

    private static String constructorName(PainlessConstructor constructor) {
        return PainlessLookupUtility.typeToCanonicalTypeName(constructor.javaConstructor.getDeclaringClass());
    }

    private static String methodName(PainlessMethod method) {
        return PainlessLookupUtility.typeToCanonicalTypeName(method.targetClass);
    }

    /**
     * Emit a {@link Class}. If the type is primitive or an array of primitives this just emits the name of the type. Otherwise this emits
     an internal link with the text.
     */
    private static void emitType(PrintStream stream, Class<?> clazz) {
        emitStruct(stream, clazz);
        while ((clazz = clazz.getComponentType()) != null) {
            stream.print("[]");
        }
    }

    /**
     * Emit a {@link PainlessClass}. If the {@linkplain PainlessClass} is primitive or def this just emits the name of the struct.
     * Otherwise this emits an internal link with the name.
     */
    private static void emitStruct(PrintStream stream, Class<?> clazz) {
        String canonicalClassName = PainlessLookupUtility.typeToCanonicalTypeName(clazz);

        if (false == clazz.isPrimitive() && clazz != def.class) {
            stream.print("<<");
            emitAnchor(stream, clazz);
            stream.print(',');
            stream.print(canonicalClassName);
            stream.print(">>");
        } else {
            stream.print(canonicalClassName);
        }
    }

    /**
     * Emit an external link to Javadoc for a {@link PainlessMethod}.
     *
     * @param root name of the root uri variable
     */
    private static void emitJavadocLink(PrintStream stream, String root, PainlessConstructor constructor) {
        stream.print("link:{");
        stream.print(root);
        stream.print("-javadoc}/");
        stream.print(classUrlPath(constructor.javaConstructor.getDeclaringClass()));
        stream.print(".html#");
        stream.print(constructorName(constructor));
        stream.print("%2D");
        boolean first = true;
        for (Class<?> clazz: constructor.typeParameters) {
            if (first) {
                first = false;
            } else {
                stream.print("%2D");
            }
            stream.print(clazz.getName());
            if (clazz.isArray()) {
                stream.print(":A");
            }
        }
        stream.print("%2D");
    }

    /**
     * Emit an external link to Javadoc for a {@link PainlessMethod}.
     *
     * @param root name of the root uri variable
     */
    private static void emitJavadocLink(PrintStream stream, String root, PainlessMethod method) {
        stream.print("link:{");
        stream.print(root);
        stream.print("-javadoc}/");
        stream.print(classUrlPath(method.javaMethod.getDeclaringClass()));
        stream.print(".html#");
        stream.print(methodName(method));
        stream.print("%2D");
        boolean first = true;
        if (method.targetClass != method.javaMethod.getDeclaringClass()) {
            first = false;
            stream.print(method.javaMethod.getDeclaringClass().getName());
        }
        for (Class<?> clazz: method.typeParameters) {
            if (first) {
                first = false;
            } else {
                stream.print("%2D");
            }
            stream.print(clazz.getName());
            if (clazz.isArray()) {
                stream.print(":A");
            }
        }
        stream.print("%2D");
    }

    /**
     * Emit an external link to Javadoc for a {@link PainlessField}.
     *
     * @param root name of the root uri variable
     */
    private static void emitJavadocLink(PrintStream stream, String root, PainlessField field) {
        stream.print("link:{");
        stream.print(root);
        stream.print("-javadoc}/");
        stream.print(classUrlPath(field.javaField.getDeclaringClass()));
        stream.print(".html#");
        stream.print(field.javaField.getName());
    }

    /**
     * Pick the javadoc root for a {@link PainlessMethod}.
     */
    private static String javadocRoot(PainlessMethod method) {
        if (method.targetClass != method.javaMethod.getDeclaringClass()) {
            return "painless";
        }
        return javadocRoot(method.targetClass);
    }

    /**
     * Pick the javadoc root for a {@link PainlessField}.
     */
    private static String javadocRoot(PainlessField field) {
        return javadocRoot(field.javaField.getDeclaringClass());
    }

    /**
     * Pick the javadoc root for a {@link Class}.
     */
    private static String javadocRoot(Class<?> clazz) {
        String classPackage = clazz.getPackage().getName();
        if (classPackage.startsWith("java")) {
            return "java8";
        }
        if (classPackage.startsWith("org.elasticsearch.painless")) {
            return "painless";
        }
        if (classPackage.startsWith("org.elasticsearch")) {
            return "elasticsearch";
        }
        if (classPackage.startsWith("org.joda.time")) {
            return "joda-time";
        }
        if (classPackage.startsWith("org.apache.lucene")) {
            return "lucene-core";
        }
        throw new IllegalArgumentException("Unrecognized package: " + classPackage);
    }

    private static void emitGeneratedWarning(PrintStream stream) {
        stream.println("////");
        stream.println("Automatically generated by PainlessDocGenerator. Do not edit.");
        stream.println("Rebuild by running `gradle generatePainlessApi`.");
        stream.println("////");
        stream.println();
    }

    private static String classUrlPath(Class<?> clazz) {
        return clazz.getName().replace('.', '/').replace('$', '.');
    }
}
