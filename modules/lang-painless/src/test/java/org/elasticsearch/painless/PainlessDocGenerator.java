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
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.painless.lookup.PainlessClass;
import org.elasticsearch.painless.lookup.PainlessField;
import org.elasticsearch.painless.lookup.PainlessLookup;
import org.elasticsearch.painless.lookup.PainlessLookupBuilder;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
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

import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

/**
 * Generates an API reference from the method and type whitelists in {@link PainlessLookup}.
 */
public class PainlessDocGenerator {

    private static final PainlessLookup PAINLESS_LOOKUP = new PainlessLookupBuilder(Whitelist.BASE_WHITELISTS).build();
    private static final Logger logger = ESLoggerFactory.getLogger(PainlessDocGenerator.class);
    private static final Comparator<PainlessField> FIELD_NAME = comparing(f -> f.name);
    private static final Comparator<PainlessMethod> METHOD_NAME = comparing(m -> m.name);
    private static final Comparator<PainlessMethod> NUMBER_OF_ARGS = comparing(m -> m.arguments.size());

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
            List<PainlessClass> structs = PAINLESS_LOOKUP.getStructs().stream().sorted(comparing(t -> t.name)).collect(toList());
            for (PainlessClass struct : structs) {
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
                    emitAnchor(typeStream, struct.clazz);
                    typeStream.print("]]++");
                    typeStream.print(struct.name);
                    typeStream.println("++::");

                    Consumer<PainlessField> documentField = field -> PainlessDocGenerator.documentField(typeStream, field);
                    Consumer<PainlessMethod> documentMethod = method -> PainlessDocGenerator.documentMethod(typeStream, method);
                    struct.staticMembers.values().stream().sorted(FIELD_NAME).forEach(documentField);
                    struct.members.values().stream().sorted(FIELD_NAME).forEach(documentField);
                    struct.staticMethods.values().stream().sorted(METHOD_NAME.thenComparing(NUMBER_OF_ARGS)).forEach(documentMethod);
                    struct.constructors.values().stream().sorted(NUMBER_OF_ARGS).forEach(documentMethod);
                    Map<String, PainlessClass> inherited = new TreeMap<>();
                    struct.methods.values().stream().sorted(METHOD_NAME.thenComparing(NUMBER_OF_ARGS)).forEach(method -> {
                        if (method.target == struct.clazz) {
                            documentMethod(typeStream, method);
                        } else {
                            PainlessClass painlessClass = PAINLESS_LOOKUP.getPainlessStructFromJavaClass(method.target);
                            inherited.put(painlessClass.name, painlessClass);
                        }
                    });

                    if (false == inherited.isEmpty()) {
                        typeStream.print("* Inherits methods from ");
                        boolean first = true;
                        for (PainlessClass inheritsFrom : inherited.values()) {
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
    private static void documentMethod(PrintStream stream, PainlessMethod method) {
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
     * Anchor text for a {@link PainlessClass}.
     */
    private static void emitAnchor(PrintStream stream, Class<?> clazz) {
        stream.print("painless-api-reference-");
        stream.print(PainlessLookupUtility.typeToCanonicalTypeName(clazz).replace('.', '-'));
    }

    /**
     * Anchor text for a {@link PainlessMethod}.
     */
    private static void emitAnchor(PrintStream stream, PainlessMethod method) {
        emitAnchor(stream, method.target);
        stream.print('-');
        stream.print(methodName(method));
        stream.print('-');
        stream.print(method.arguments.size());
    }

    /**
     * Anchor text for a {@link PainlessField}.
     */
    private static void emitAnchor(PrintStream stream, PainlessField field) {
        emitAnchor(stream, field.target);
        stream.print('-');
        stream.print(field.name);
    }

    private static String methodName(PainlessMethod method) {
        return method.name.equals("<init>") ? PainlessLookupUtility.typeToCanonicalTypeName(method.target) : method.name;
    }

    /**
     * Emit a {@link Class}. If the type is primitive or an array of primitives this just emits the name of the type. Otherwise this emits
     an internal link with the text.
     */
    private static void emitType(PrintStream stream, Class<?> clazz) {
        emitStruct(stream, PAINLESS_LOOKUP.getPainlessStructFromJavaClass(clazz));
        while ((clazz = clazz.getComponentType()) != null) {
            stream.print("[]");
        }
    }

    /**
     * Emit a {@link PainlessClass}. If the {@linkplain PainlessClass} is primitive or def this just emits the name of the struct.
     * Otherwise this emits an internal link with the name.
     */
    private static void emitStruct(PrintStream stream, PainlessClass struct) {
        if (false == struct.clazz.isPrimitive() && false == struct.name.equals("def")) {
            stream.print("<<");
            emitAnchor(stream, struct.clazz);
            stream.print(',');
            stream.print(struct.name);
            stream.print(">>");
        } else {
            stream.print(struct.name);
        }
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
        stream.print(classUrlPath(method.augmentation != null ? method.augmentation : method.target));
        stream.print(".html#");
        stream.print(methodName(method));
        stream.print("%2D");
        boolean first = true;
        if (method.augmentation != null) {
            first = false;
            stream.print(method.target.getName());
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
     * Emit an external link to Javadoc for a {@link PainlessField}.
     *
     * @param root name of the root uri variable
     */
    private static void emitJavadocLink(PrintStream stream, String root, PainlessField field) {
        stream.print("link:{");
        stream.print(root);
        stream.print("-javadoc}/");
        stream.print(classUrlPath(field.target));
        stream.print(".html#");
        stream.print(field.javaName);
    }

    /**
     * Pick the javadoc root for a {@link PainlessMethod}.
     */
    private static String javadocRoot(PainlessMethod method) {
        if (method.augmentation != null) {
            return "painless";
        }
        return javadocRoot(method.target);
    }

    /**
     * Pick the javadoc root for a {@link PainlessField}.
     */
    private static String javadocRoot(PainlessField field) {
        return javadocRoot(field.target);
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
