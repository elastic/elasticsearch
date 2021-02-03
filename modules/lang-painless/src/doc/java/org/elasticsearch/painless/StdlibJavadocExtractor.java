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
import com.github.javaparser.ast.visitor.VoidVisitorAdapter;
import com.github.javaparser.javadoc.Javadoc;
import org.elasticsearch.common.SuppressForbidden;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class StdlibJavadocExtractor {
    private final Path root;

    public StdlibJavadocExtractor(Path root) {
        this.root = root;
    }

    @SuppressForbidden(reason = "resolve class file from java src directory with environment")
    private File openClassFile(String className) {
        int dollarPosition = className.indexOf("$");
        if (dollarPosition >= 0) {
            className = className.substring(0, dollarPosition);
        }
        String[] packages = className.split("\\.");
        String path = String.join("/", packages);
        Path classPath = root.resolve(path + ".java");
        return classPath.toFile();
    }

    public ParsedJavaClass parseClass(String className) throws IOException {
        ParsedJavaClass pj = new ParsedJavaClass();
        if (className.contains(".") && className.startsWith("java")) { // TODO(stu): handle primitives & not stdlib
            ClassFileVisitor visitor = new ClassFileVisitor();
            CompilationUnit cu = StaticJavaParser.parse(openClassFile(className));
            visitor.visit(cu, pj);
        }
        return pj;
    }

    public static class ParsedJavaClass {
        public final Map<MethodSignature, ParsedMethod> methods;
        public final Map<String, String> fields;
        public final Map<List<String>, ParsedMethod> constructors;

        public ParsedJavaClass() {
            methods = new HashMap<>();
            fields = new HashMap<>();
            constructors = new HashMap<>();
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
