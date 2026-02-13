/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.release;

import com.github.javaparser.GeneratedJavaParserConstants;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.expr.IntegerLiteralExpr;
import com.github.javaparser.ast.observer.ObservableProperty;
import com.github.javaparser.printer.ConcreteSyntaxModel;
import com.github.javaparser.printer.concretesyntaxmodel.CsmElement;
import com.github.javaparser.printer.lexicalpreservation.LexicalPreservingPrinter;

import org.gradle.api.DefaultTask;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.initialization.layout.BuildLayout;

import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.stream.Collectors;

import static com.github.javaparser.ast.observer.ObservableProperty.TYPE_PARAMETERS;
import static com.github.javaparser.printer.concretesyntaxmodel.CsmConditional.Condition.FLAG;
import static com.github.javaparser.printer.concretesyntaxmodel.CsmElement.block;
import static com.github.javaparser.printer.concretesyntaxmodel.CsmElement.child;
import static com.github.javaparser.printer.concretesyntaxmodel.CsmElement.comma;
import static com.github.javaparser.printer.concretesyntaxmodel.CsmElement.comment;
import static com.github.javaparser.printer.concretesyntaxmodel.CsmElement.conditional;
import static com.github.javaparser.printer.concretesyntaxmodel.CsmElement.list;
import static com.github.javaparser.printer.concretesyntaxmodel.CsmElement.newline;
import static com.github.javaparser.printer.concretesyntaxmodel.CsmElement.none;
import static com.github.javaparser.printer.concretesyntaxmodel.CsmElement.sequence;
import static com.github.javaparser.printer.concretesyntaxmodel.CsmElement.space;
import static com.github.javaparser.printer.concretesyntaxmodel.CsmElement.string;
import static com.github.javaparser.printer.concretesyntaxmodel.CsmElement.token;

public abstract class AbstractVersionsTask extends DefaultTask {

    static {
        replaceDefaultJavaParserClassCsm();
    }

    /*
     * The default JavaParser CSM which it uses to format any new declarations added to a class
     * inserts two newlines after each declaration. Our version classes only have one newline.
     * In order to get javaparser lexical printer to use our format, we have to completely replace
     * the statically declared CSM pattern using hacky reflection
     * to access the static map where these are stored, and insert a replacement that is identical
     * apart from only one newline at the end of each member declaration, rather than two.
     */
    private static void replaceDefaultJavaParserClassCsm() {
        try {
            Field classCsms = ConcreteSyntaxModel.class.getDeclaredField("concreteSyntaxModelByClass");
            classCsms.setAccessible(true);
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Map<Class, CsmElement> csms = (Map) classCsms.get(null);

            // copied from the static initializer in ConcreteSyntaxModel
            csms.put(
                ClassOrInterfaceDeclaration.class,
                sequence(
                    comment(),
                    list(ObservableProperty.ANNOTATIONS, newline(), none(), newline()),
                    list(ObservableProperty.MODIFIERS, space(), none(), space()),
                    conditional(
                        ObservableProperty.INTERFACE,
                        FLAG,
                        token(GeneratedJavaParserConstants.INTERFACE),
                        token(GeneratedJavaParserConstants.CLASS)
                    ),
                    space(),
                    child(ObservableProperty.NAME),
                    list(
                        TYPE_PARAMETERS,
                        sequence(comma(), space()),
                        string(GeneratedJavaParserConstants.LT),
                        string(GeneratedJavaParserConstants.GT)
                    ),
                    list(
                        ObservableProperty.EXTENDED_TYPES,
                        sequence(string(GeneratedJavaParserConstants.COMMA), space()),
                        sequence(space(), token(GeneratedJavaParserConstants.EXTENDS), space()),
                        none()
                    ),
                    list(
                        ObservableProperty.IMPLEMENTED_TYPES,
                        sequence(string(GeneratedJavaParserConstants.COMMA), space()),
                        sequence(space(), token(GeneratedJavaParserConstants.IMPLEMENTS), space()),
                        none()
                    ),
                    space(),
                    block(sequence(newline(), list(ObservableProperty.MEMBERS, sequence(newline()/*, newline()*/), newline(), newline())))
                )
            );
        } catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static final Logger LOGGER = Logging.getLogger(AbstractVersionsTask.class);

    static final String INDEX_VERSION_TYPE = "IndexVersion";

    static final String SERVER_MODULE_PATH = "server/src/main/java/";

    static final String VERSION_FILE_PATH = SERVER_MODULE_PATH + "org/elasticsearch/Version.java";
    static final String INDEX_VERSIONS_FILE_PATH = SERVER_MODULE_PATH + "org/elasticsearch/index/IndexVersions.java";

    static final String SERVER_RESOURCES_PATH = "server/src/main/resources/";
    static final String INDEX_VERSIONS_RECORD = SERVER_RESOURCES_PATH + "org/elasticsearch/index/IndexVersions.csv";

    final Path rootDir;

    protected AbstractVersionsTask(BuildLayout layout) {
        rootDir = layout.getRootDirectory().toPath();
    }

    static Map<String, Integer> splitVersionIds(List<String> version) {
        return version.stream().map(l -> {
            var split = l.split(":");
            if (split.length != 2) throw new IllegalArgumentException("Invalid tag format [" + l + "]");
            return split;
        }).collect(Collectors.toMap(l -> l[0], l -> Integer.parseInt(l[1])));
    }

    static OptionalInt findSingleIntegerExpr(FieldDeclaration field) {
        var ints = field.findAll(IntegerLiteralExpr.class);
        switch (ints.size()) {
            case 0 -> {
                return OptionalInt.empty();
            }
            case 1 -> {
                return OptionalInt.of(ints.get(0).asNumber().intValue());
            }
            default -> {
                LOGGER.warn("Multiple integers found in version field declaration [{}]", field); // and ignore it
                return OptionalInt.empty();
            }
        }
    }

    static void writeOutNewContents(Path file, CompilationUnit unit) throws IOException {
        if (unit.containsData(LexicalPreservingPrinter.NODE_TEXT_DATA) == false) {
            throw new IllegalArgumentException("CompilationUnit has no lexical information for output");
        }
        Files.writeString(file, LexicalPreservingPrinter.print(unit), StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
    }
}
