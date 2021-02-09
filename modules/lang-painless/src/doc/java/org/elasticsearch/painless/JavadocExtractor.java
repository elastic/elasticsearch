/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless;

import com.github.javaparser.StaticJavaParser;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.Modifier;
import com.github.javaparser.ast.body.ConstructorDeclaration;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.body.VariableDeclarator;
import com.github.javaparser.ast.comments.Comment;
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;
import com.github.javaparser.javadoc.Javadoc;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

public class JavadocExtractor {

    private final JavaClassResolver resolver;

    private static final String GPLv2 = "* This code is free software; you can redistribute it and/or modify it"+
        "\n * under the terms of the GNU General Public License version 2 only, as"+
        "\n * published by the Free Software Foundation.";

    public JavadocExtractor(JavaClassResolver resolver) {
        this.resolver = resolver;
    }


    public ParsedJavaClass parseClass(String className) throws IOException {
        InputStream classStream = resolver.openClassFile(className);
        ParsedJavaClass parsed = new ParsedJavaClass(GPLv2);
        if (classStream == null) {
            return parsed;
        }
        ClassFileVisitor visitor = new ClassFileVisitor();
        CompilationUnit cu = StaticJavaParser.parse(classStream);
        visitor.visit(cu, parsed);
        return parsed;
    }

    public static class ParsedJavaClass {
        public final Map<MethodSignature, ParsedMethod> methods;
        public final Map<String, String> fields;
        public final Map<List<String>, ParsedMethod> constructors;
        private final String license;
        private boolean valid = false;
        private boolean validated = false;

        public ParsedJavaClass(String license) {
            methods = new HashMap<>();
            fields = new HashMap<>();
            constructors = new HashMap<>();
            this.license = license;
        }

        public void validateLicense(Optional<Comment> license) {
            if (validated) {
                throw new IllegalStateException("Cannot double validate the license");
            }
            this.valid = license.map(Comment::getContent).orElse("").contains(this.license);
            validated = true;
        }

        public ParsedMethod getMethod(String name, List<String> parameterTypes) {
            return methods.get(new MethodSignature(name, parameterTypes));
        }

        @Override
        public String toString() {
            return "ParsedJavaClass{" +
                "methods=" + methods +
                '}';
        }

        public void putMethod(MethodDeclaration declaration) {
            if (valid == false) {
                return;
            }
            methods.put(
                MethodSignature.fromDeclaration(declaration),
                new ParsedMethod(
                        declaration.getJavadoc().map(Javadoc::toText).orElse(""),
                        declaration.getParameters()
                                .stream()
                                .map(p -> p.getName().asString())
                                .collect(Collectors.toList())
                )
            );
        }

        public void putConstructor(ConstructorDeclaration declaration) {
            if (valid == false) {
                return;
            }
            constructors.put(
                declaration.getParameters().stream().map(p -> stripTypeParameters(p.getType().asString())).collect(Collectors.toList()),
                new ParsedMethod(
                        declaration.getJavadoc().map(Javadoc::toText).orElse(""),
                        declaration.getParameters()
                                .stream()
                                .map(p -> p.getName().asString())
                                .collect(Collectors.toList())
                )
            );
        }

        private static String stripTypeParameters(String type) {
            int start = 0;
            int count = 0;
            for (int i=0; i<type.length(); i++) {
                char c = type.charAt(i);
                if (c == '<') {
                    if (start == 0) {
                        start = i;
                    }
                    count++;
                } else if (c == '>') {
                    count--;
                    if (count == 0) {
                        return type.substring(0, start);
                    }
                }
            }
            return type;
        }

        public ParsedMethod getConstructor(List<String> parameterTypes) {
            return constructors.get(parameterTypes);
        }

        public String getField(String name) {
            return fields.get(name);
        }

        public void putField(FieldDeclaration declaration) {
            if (valid == false) {
                return;
            }
            for (VariableDeclarator var : declaration.getVariables()) {
                fields.put(var.getNameAsString(), declaration.getJavadoc().map(Javadoc::toText).orElse(""));
            }
        }
    }

    public static class MethodSignature {
        public final String name;
        public final List<String> parameterTypes;

        public MethodSignature(String name, List<String> parameterTypes) {
            this.name = name;
            this.parameterTypes = parameterTypes;
        }

        public static MethodSignature fromDeclaration(MethodDeclaration declaration) {
            return new MethodSignature(
                    declaration.getNameAsString(),
                    declaration.getParameters()
                            .stream()
                            .map(p -> p.getType().asString())
                            .collect(Collectors.toList())
            );
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if ((o instanceof MethodSignature) == false) return false;
            MethodSignature that = (MethodSignature) o;
            return Objects.equals(name, that.name) &&
                Objects.equals(parameterTypes, that.parameterTypes);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, parameterTypes);
        }
    }

    public static class ParsedMethod {
        public final String javadoc;
        public final List<String> parameterNames;

        public ParsedMethod(String javadoc, List<String> parameterNames) {
            this.javadoc = javadoc;
            this.parameterNames = parameterNames;
        }
    }

    private static class ClassFileVisitor extends VoidVisitorAdapter<ParsedJavaClass> {
        @Override
        public void visit(CompilationUnit compilationUnit, ParsedJavaClass parsed) {
            parsed.validateLicense(compilationUnit.getComment());
            super.visit(compilationUnit, parsed);
        }

        @Override
        public void visit(MethodDeclaration methodDeclaration, ParsedJavaClass parsed) {
            parsed.putMethod(methodDeclaration);
        }

        @Override
        public void visit(FieldDeclaration fieldDeclaration, ParsedJavaClass parsed) {
            if (fieldDeclaration.hasModifier(Modifier.Keyword.PUBLIC) == false) {
                return;
            }
            parsed.putField(fieldDeclaration);
        }

        @Override
        public void visit(ConstructorDeclaration constructorDeclaration, ParsedJavaClass parsed) {
            parsed.putConstructor(constructorDeclaration);
        }
    }
}
