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
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.painless.Definition.Field;
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.Struct;
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

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;
import static org.elasticsearch.painless.spi.Whitelist.BASE_WHITELISTS;

/**
 * Generates an API reference from the method and type whitelists in {@link Definition}.
 */
public class PainlessDocGenerator {

    private static final Definition definition = new Definition(BASE_WHITELISTS);
    private static final Logger logger = ESLoggerFactory.getLogger(PainlessDocGenerator.class);
    private static final Comparator<Field> FIELD_NAME = comparing(f -> f.name);
    private static final Comparator<Method> METHOD_NAME = comparing(m -> m.name);
    private static final Comparator<Method> NUMBER_OF_ARGS = comparing(m -> m.arguments.size());

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
            List<Struct> structs = definition.getStructs().stream().sorted(comparing(t -> t.name)).collect(toList());
            for (Struct struct : structs) {
                if (struct.clazz.isPrimitive()) {
                    // Primitives don't have methods to reference
                    continue;
                }
                if ("def".equals(struct.name)) {
                    // def is special but doesn't have any methods all of its own.
                    continue;
                }
                indexStream.print("include::");
                indexStream.print(struct.name);
                indexStream.println(".asciidoc[]");

                Path typePath = apiRootPath.resolve(struct.name + ".asciidoc");
                logger.info("Writing [{}.asciidoc]", struct.name);
                try (PrintStream typeStream = new PrintStream(
                        Files.newOutputStream(typePath, StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                        false, StandardCharsets.UTF_8.name())) {
                    emitGeneratedWarning(typeStream);
                    typeStream.print("[[");
                    emitAnchor(typeStream, struct);
                    typeStream.print("]]++");
                    typeStream.print(struct.name);
                    typeStream.println("++::");

                    Consumer<Field> documentField = field -> PainlessDocGenerator.documentField(typeStream, field);
                    Consumer<Method> documentMethod = method -> PainlessDocGenerator.documentMethod(typeStream, method);
                    struct.staticMembers.values().stream().sorted(FIELD_NAME).forEach(documentField);
                    struct.members.values().stream().sorted(FIELD_NAME).forEach(documentField);
                    struct.staticMethods.values().stream().sorted(METHOD_NAME.thenComparing(NUMBER_OF_ARGS)).forEach(documentMethod);
                    struct.constructors.values().stream().sorted(NUMBER_OF_ARGS).forEach(documentMethod);
                    Map<String, Struct> inherited = new TreeMap<>();
                    struct.methods.values().stream().sorted(METHOD_NAME.thenComparing(NUMBER_OF_ARGS)).forEach(method -> {
                        if (method.owner == struct) {
                            documentMethod(typeStream, method);
                        } else {
                            inherited.put(method.owner.name, method.owner);
                        }
                    });

                    if (false == inherited.isEmpty()) {
                        typeStream.print("* Inherits methods from ");
                        boolean first = true;
                        for (Struct inheritsFrom : inherited.values()) {
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

    private static void documentField(PrintStream stream, Field field) {
        stream.print("** [[");
        emitAnchor(stream, field);
        stream.print("]]");

        if (Modifier.isStatic(field.modifiers)) {
            stream.print("static ");
        }

        emitType(stream, field.clazz);
        stream.print(' ');

        String javadocRoot = javadocRoot(field);
        emitJavadocLink(stream, javadocRoot, field);
        stream.print('[');
        stream.print(field.name);
        stream.print(']');

        if (javadocRoot.equals("java8")) {
            stream.print(" (");
            emitJavadocLink(stream, "java9", field);
            stream.print("[java 9])");
        }

        stream.println();
    }

    /**
     * Document a method.
     */
    private static void documentMethod(PrintStream stream, Method method) {
        stream.print("* ++[[");
        emitAnchor(stream, method);
        stream.print("]]");

        if (null == method.augmentation && Modifier.isStatic(method.modifiers)) {
            stream.print("static ");
        }

        if (false == method.name.equals("<init>")) {
            emitType(stream, method.rtn);
            stream.print(' ');
        }

        String javadocRoot = javadocRoot(method);
        emitJavadocLink(stream, javadocRoot, method);
        stream.print('[');

        stream.print(methodName(method));

        stream.print("](");
        boolean first = true;
        for (Class<?> arg : method.arguments) {
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
     * Anchor text for a {@link Struct}.
     */
    private static void emitAnchor(PrintStream stream, Struct struct) {
        stream.print("painless-api-reference-");
        stream.print(struct.name.replace('.', '-'));
    }

    /**
     * Anchor text for a {@link Method}.
     */
    private static void emitAnchor(PrintStream stream, Method method) {
        emitAnchor(stream, method.owner);
        stream.print('-');
        stream.print(methodName(method));
        stream.print('-');
        stream.print(method.arguments.size());
    }

    /**
     * Anchor text for a {@link Field}.
     */
    private static void emitAnchor(PrintStream stream, Field field) {
        emitAnchor(stream, field.owner);
        stream.print('-');
        stream.print(field.name);
    }

    private static String methodName(Method method) {
        return method.name.equals("<init>") ? method.owner.name : method.name;
    }

    /**
     * Emit a {@link Class}. If the type is primitive or an array of primitives this just emits the name of the type. Otherwise this emits
       an internal link with the text.
     */
    private static void emitType(PrintStream stream, Class<?> clazz) {
        emitStruct(stream, definition.getPainlessStructFromJavaClass(clazz));
        while ((clazz = clazz.getComponentType()) != null) {
            stream.print("[]");
        }
    }

    /**
     * Emit a {@link Struct}. If the {@linkplain Struct} is primitive or def this just emits the name of the struct. Otherwise this emits
     * an internal link with the name.
     */
    private static void emitStruct(PrintStream stream, Struct struct) {
        if (false == struct.clazz.isPrimitive() && false == struct.name.equals("def")) {
            stream.print("<<");
            emitAnchor(stream, struct);
            stream.print(',');
            stream.print(struct.name);
            stream.print(">>");
        } else {
            stream.print(struct.name);
        }
    }

    /**
     * Emit an external link to Javadoc for a {@link Method}.
     *
     * @param root name of the root uri variable
     */
    private static void emitJavadocLink(PrintStream stream, String root, Method method) {
        stream.print("link:{");
        stream.print(root);
        stream.print("-javadoc}/");
        stream.print(classUrlPath(method.augmentation != null ? method.augmentation : method.owner.clazz));
        stream.print(".html#");
        stream.print(methodName(method));
        stream.print("%2D");
        boolean first = true;
        if (method.augmentation != null) {
            first = false;
            stream.print(method.owner.clazz.getName());
        }
        for (Class<?> clazz: method.arguments) {
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
     * Emit an external link to Javadoc for a {@link Field}.
     *
     * @param root name of the root uri variable
     */
    private static void emitJavadocLink(PrintStream stream, String root, Field field) {
        stream.print("link:{");
        stream.print(root);
        stream.print("-javadoc}/");
        stream.print(classUrlPath(field.owner.clazz));
        stream.print(".html#");
        stream.print(field.javaName);
    }

    /**
     * Pick the javadoc root for a {@link Method}.
     */
    private static String javadocRoot(Method method) {
        if (method.augmentation != null) {
            return "painless";
        }
        return javadocRoot(method.owner);
    }

    /**
     * Pick the javadoc root for a {@link Field}.
     */
    private static String javadocRoot(Field field) {
        return javadocRoot(field.owner);
    }

    /**
     * Pick the javadoc root for a {@link Struct}.
     */
    private static String javadocRoot(Struct struct) {
        String classPackage = struct.clazz.getPackage().getName();
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
        throw new IllegalArgumentException("Unrecognized packge: " + classPackage);
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
