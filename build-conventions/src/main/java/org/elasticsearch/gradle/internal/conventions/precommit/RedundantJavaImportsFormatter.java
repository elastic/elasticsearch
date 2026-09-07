/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.conventions.precommit;

import com.diffplug.spotless.FormatterFunc;
import com.github.javaparser.JavaParser;
import com.github.javaparser.ParseResult;
import com.github.javaparser.ParserConfiguration;
import com.github.javaparser.ast.CompilationUnit;
import com.github.javaparser.ast.ImportDeclaration;
import com.github.javaparser.ast.Node;
import com.github.javaparser.ast.body.ClassOrInterfaceDeclaration;
import com.github.javaparser.ast.body.ConstructorDeclaration;
import com.github.javaparser.ast.body.EnumDeclaration;
import com.github.javaparser.ast.body.FieldDeclaration;
import com.github.javaparser.ast.body.MethodDeclaration;
import com.github.javaparser.ast.body.Parameter;
import com.github.javaparser.ast.body.RecordDeclaration;
import com.github.javaparser.ast.body.TypeDeclaration;
import com.github.javaparser.ast.body.VariableDeclarator;
import com.github.javaparser.ast.expr.AnnotationExpr;
import com.github.javaparser.ast.expr.Expression;
import com.github.javaparser.ast.expr.LambdaExpr;
import com.github.javaparser.ast.expr.MethodCallExpr;
import com.github.javaparser.ast.expr.NameExpr;
import com.github.javaparser.ast.expr.ObjectCreationExpr;
import com.github.javaparser.ast.expr.VariableDeclarationExpr;
import com.github.javaparser.ast.stmt.BlockStmt;
import com.github.javaparser.ast.stmt.CatchClause;
import com.github.javaparser.ast.stmt.ExpressionStmt;
import com.github.javaparser.ast.stmt.ForEachStmt;
import com.github.javaparser.ast.stmt.ForStmt;
import com.github.javaparser.ast.stmt.Statement;
import com.github.javaparser.ast.stmt.TryStmt;
import com.github.javaparser.ast.type.ClassOrInterfaceType;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Removes redundant Java imports. google-java-format's {@code removeUnusedImports} only checks
 * whether the imported simple name appears as a token in the file. That heuristic cannot tell
 * a used identifier from:
 * <ul>
 *   <li>a redundant static import of a method already inherited from a test base class
 *       ({@code import static org.junit.Assert.assertTrue} in an {@code ESTestCase} subclass)</li>
 *   <li>a redundant nested-type import when the enclosing type is already in scope via
 *       {@code extends}/{@code implements} ({@code import Outer.Inner} in a class that extends
 *       {@code Outer})</li>
 *   <li>an unused static method import whose simple name collides with a local variable or field
 *       ({@code import static org.hamcrest.Matchers.in} next to {@code StreamInput in})</li>
 * </ul>
 * Analysis is source-only (no classpath). Inheritance is resolved from types declared in the same
 * file plus the {@code extends}/{@code implements} clauses of the enclosing types of each usage.
 */
public final class RedundantJavaImportsFormatter implements FormatterFunc, Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Bump this when the rewrite rules change so Spotless invalidates its up-to-date cache.
     */
    private final int revision = 4;

    private static final List<String> REDUNDANT_TEST_STATIC_PREFIXES = List.of(
        "org.junit.Assert.",
        "org.junit.Assume.",
        "org.hamcrest.MatcherAssert.",
        "com.carrotsearch.randomizedtesting.RandomizedTest."
    );

    private static final Set<String> REDUNDANT_TEST_STATIC_TYPES = Set.of(
        "org.junit.Assert",
        "org.junit.Assume",
        "org.hamcrest.MatcherAssert",
        "com.carrotsearch.randomizedtesting.RandomizedTest"
    );

    @Override
    public String apply(String unix) {
        return format(unix);
    }

    static String format(String unix) {
        if (!unix.contains("import ")) {
            return unix;
        }
        ParseResult<CompilationUnit> parsed = parser().parse(unix);
        if (parsed.isSuccessful() == false || parsed.getResult().isEmpty()) {
            return unix;
        }
        CompilationUnit cu = parsed.getResult().get();
        if (cu.getImports().isEmpty()) {
            return unix;
        }

        Map<String, Set<String>> directSupers = directSupersByType(cu);
        boolean extendsTestBase = extendsTestBase(cu);
        List<ImportDeclaration> redundant = new ArrayList<>();
        for (ImportDeclaration imp : cu.getImports()) {
            if (isRedundant(imp, cu, directSupers, extendsTestBase)) {
                redundant.add(imp);
            }
        }
        if (redundant.isEmpty()) {
            return unix;
        }
        return removeImportLines(unix, redundant);
    }

    private static JavaParser parser() {
        ParserConfiguration configuration = new ParserConfiguration();
        configuration.setLanguageLevel(ParserConfiguration.LanguageLevel.BLEEDING_EDGE);
        return new JavaParser(configuration);
    }

    private static boolean isRedundant(
        ImportDeclaration imp,
        CompilationUnit cu,
        Map<String, Set<String>> directSupers,
        boolean extendsTestBase
    ) {
        String name = imp.getNameAsString();
        if (imp.isStatic()) {
            if (extendsTestBase && isRedundantTestFrameworkStaticImport(imp, name)) {
                return true;
            }
            String simpleName = simpleName(name);
            return isStaticMethodName(simpleName) && isUsedAsStaticImport(cu, simpleName) == false;
        }
        if (isNestedTypeImport(name) == false) {
            return false;
        }
        String nestedSimpleName = simpleName(name);
        String enclosingSimpleName = enclosingTypeSimpleName(name);
        List<Node> usages = typeUsages(cu, nestedSimpleName);
        if (usages.isEmpty()) {
            // google-java-format already drops imports whose simple name is absent as a token.
            // If we find no AST usage, keep the import. It may be an annotation, method
            // reference, javadoc {@link}, or another node type this scan does not cover.
            return false;
        }
        for (Node usage : usages) {
            if (inherits(usage, enclosingSimpleName, directSupers) == false) {
                return false;
            }
        }
        return true;
    }

    private static boolean isRedundantTestFrameworkStaticImport(ImportDeclaration imp, String name) {
        if (imp.isAsterisk()) {
            return REDUNDANT_TEST_STATIC_TYPES.contains(name);
        }
        for (String prefix : REDUNDANT_TEST_STATIC_PREFIXES) {
            if (name.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    private static boolean extendsTestBase(CompilationUnit cu) {
        for (TypeDeclaration<?> type : cu.getTypes()) {
            if (type instanceof ClassOrInterfaceDeclaration clazz) {
                for (ClassOrInterfaceType extended : clazz.getExtendedTypes()) {
                    if (isTestBaseName(extended.getNameAsString())) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    private static boolean isTestBaseName(String simpleName) {
        return simpleName.equals("RandomizedTest") || simpleName.equals("LuceneTestCase") || simpleName.endsWith("TestCase");
    }

    private static boolean isStaticMethodName(String simpleName) {
        return simpleName.isEmpty() == false && Character.isLowerCase(simpleName.charAt(0));
    }

    private static boolean isUsedAsUnscopedMethodCall(CompilationUnit cu, String simpleName) {
        for (MethodCallExpr call : cu.findAll(MethodCallExpr.class)) {
            if (simpleName.equals(call.getNameAsString()) && call.getScope().isEmpty()) {
                return true;
            }
        }
        return false;
    }

    /**
     * A lowercase static import is in use if it is invoked as a method ({@code rarely()}) or
     * referenced as an identifier that is not shadowed by a local/parameter/field in scope
     * ({@code deprecationLogger.critical(...)}).
     */
    private static boolean isUsedAsStaticImport(CompilationUnit cu, String simpleName) {
        if (isUsedAsUnscopedMethodCall(cu, simpleName)) {
            return true;
        }
        for (NameExpr nameExpr : cu.findAll(NameExpr.class)) {
            if (simpleName.equals(nameExpr.getNameAsString()) && isShadowed(nameExpr, simpleName) == false) {
                return true;
            }
        }
        return false;
    }

    private static boolean isShadowed(NameExpr usage, String simpleName) {
        Node current = usage.getParentNode().orElse(null);
        while (current != null) {
            if (current instanceof MethodDeclaration method) {
                if (hasParameterNamed(method.getParameters(), simpleName)) {
                    return true;
                }
            } else if (current instanceof ConstructorDeclaration constructor) {
                if (hasParameterNamed(constructor.getParameters(), simpleName)) {
                    return true;
                }
            } else if (current instanceof LambdaExpr lambda) {
                if (hasParameterNamed(lambda.getParameters(), simpleName)) {
                    return true;
                }
            } else if (current instanceof CatchClause catchClause) {
                if (simpleName.equals(catchClause.getParameter().getNameAsString())) {
                    return true;
                }
            } else if (current instanceof ForEachStmt forEach) {
                if (hasVariableNamed(forEach.getVariable().getVariables(), simpleName)) {
                    return true;
                }
            } else if (current instanceof ForStmt forStmt) {
                for (Expression init : forStmt.getInitialization()) {
                    if (init instanceof VariableDeclarationExpr declaration && hasVariableNamed(declaration.getVariables(), simpleName)) {
                        return true;
                    }
                }
            } else if (current instanceof TryStmt tryStmt) {
                for (Expression resource : tryStmt.getResources()) {
                    if (resource instanceof VariableDeclarationExpr declaration
                        && hasVariableNamed(declaration.getVariables(), simpleName)) {
                        return true;
                    }
                }
            } else if (current instanceof BlockStmt block) {
                if (hasLocalVariableNamedBefore(block, usage, simpleName)) {
                    return true;
                }
            } else if (current instanceof ClassOrInterfaceDeclaration clazz) {
                if (hasFieldNamed(clazz.getFields(), simpleName)) {
                    return true;
                }
            } else if (current instanceof RecordDeclaration recordDecl) {
                if (hasParameterNamed(recordDecl.getParameters(), simpleName)) {
                    return true;
                }
            }
            current = current.getParentNode().orElse(null);
        }
        return false;
    }

    private static boolean hasParameterNamed(Iterable<Parameter> parameters, String simpleName) {
        for (Parameter parameter : parameters) {
            if (simpleName.equals(parameter.getNameAsString())) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasVariableNamed(Iterable<VariableDeclarator> variables, String simpleName) {
        for (VariableDeclarator variable : variables) {
            if (simpleName.equals(variable.getNameAsString())) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasFieldNamed(Iterable<FieldDeclaration> fields, String simpleName) {
        for (FieldDeclaration field : fields) {
            if (hasVariableNamed(field.getVariables(), simpleName)) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasLocalVariableNamedBefore(BlockStmt block, NameExpr usage, String simpleName) {
        for (Statement statement : block.getStatements()) {
            if (isAncestor(statement, usage)) {
                return false;
            }
            if (statement instanceof ExpressionStmt expressionStmt
                && expressionStmt.getExpression() instanceof VariableDeclarationExpr declaration
                && hasVariableNamed(declaration.getVariables(), simpleName)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isNestedTypeImport(String name) {
        int typeSegments = 0;
        for (String segment : name.split("\\.")) {
            if (segment.isEmpty() == false && Character.isUpperCase(segment.charAt(0))) {
                typeSegments++;
            }
        }
        return typeSegments >= 2;
    }

    private static String simpleName(String name) {
        int lastDot = name.lastIndexOf('.');
        return lastDot < 0 ? name : name.substring(lastDot + 1);
    }

    private static String enclosingTypeSimpleName(String name) {
        int lastDot = name.lastIndexOf('.');
        String enclosing = lastDot < 0 ? name : name.substring(0, lastDot);
        return simpleName(enclosing);
    }

    private static List<Node> typeUsages(CompilationUnit cu, String simpleName) {
        List<Node> usages = new ArrayList<>();
        for (ClassOrInterfaceType type : cu.findAll(ClassOrInterfaceType.class)) {
            if (simpleName.equals(type.getNameAsString())) {
                usages.add(type);
            }
        }
        for (NameExpr nameExpr : cu.findAll(NameExpr.class)) {
            if (simpleName.equals(nameExpr.getNameAsString())) {
                usages.add(nameExpr);
            }
        }
        for (AnnotationExpr annotation : cu.findAll(AnnotationExpr.class)) {
            if (simpleName.equals(annotation.getName().getIdentifier())) {
                usages.add(annotation);
            }
        }
        return usages;
    }

    private static Map<String, Set<String>> directSupersByType(CompilationUnit cu) {
        Map<String, Set<String>> directSupers = new HashMap<>();
        for (ClassOrInterfaceDeclaration type : cu.findAll(ClassOrInterfaceDeclaration.class)) {
            addSupers(directSupers, type.getNameAsString(), type.getExtendedTypes(), type.getImplementedTypes());
        }
        for (RecordDeclaration type : cu.findAll(RecordDeclaration.class)) {
            addSupers(directSupers, type.getNameAsString(), List.of(), type.getImplementedTypes());
        }
        for (EnumDeclaration type : cu.findAll(EnumDeclaration.class)) {
            addSupers(directSupers, type.getNameAsString(), List.of(), type.getImplementedTypes());
        }
        return directSupers;
    }

    private static void addSupers(
        Map<String, Set<String>> directSupers,
        String typeName,
        Iterable<ClassOrInterfaceType> extended,
        Iterable<ClassOrInterfaceType> implemented
    ) {
        Set<String> supers = directSupers.computeIfAbsent(typeName, key -> new HashSet<>());
        for (ClassOrInterfaceType type : extended) {
            supers.add(type.getNameAsString());
        }
        for (ClassOrInterfaceType type : implemented) {
            supers.add(type.getNameAsString());
        }
    }

    private static boolean inherits(Node usage, String enclosingSimpleName, Map<String, Set<String>> directSupers) {
        Set<String> inherited = inheritedNames(usage, directSupers);
        return inherited.contains(enclosingSimpleName);
    }

    private static Set<String> inheritedNames(Node usage, Map<String, Set<String>> directSupers) {
        Set<String> inherited = new HashSet<>();
        Node child = usage;
        Node current = usage.getParentNode().orElse(null);
        while (current != null) {
            if (current instanceof ClassOrInterfaceDeclaration clazz) {
                inherited.addAll(transitiveSupers(clazz.getNameAsString(), directSupers));
            } else if (current instanceof RecordDeclaration recordDecl) {
                inherited.addAll(transitiveSupers(recordDecl.getNameAsString(), directSupers));
            } else if (current instanceof EnumDeclaration enumDecl) {
                inherited.addAll(transitiveSupers(enumDecl.getNameAsString(), directSupers));
            } else if (current instanceof ObjectCreationExpr creation && creation.getAnonymousClassBody().isPresent()) {
                Node origin = child;
                if (creation.getAnonymousClassBody().get().stream().anyMatch(bodyDecl -> isAncestor(bodyDecl, origin))) {
                    String superName = creation.getType().getNameAsString();
                    inherited.add(superName);
                    inherited.addAll(transitiveSupers(superName, directSupers));
                }
            }
            child = current;
            current = current.getParentNode().orElse(null);
        }
        return inherited;
    }

    private static Set<String> transitiveSupers(String typeName, Map<String, Set<String>> directSupers) {
        Set<String> result = new HashSet<>();
        ArrayDeque<String> queue = new ArrayDeque<>();
        queue.add(typeName);
        while (queue.isEmpty() == false) {
            String current = queue.removeFirst();
            for (String parent : directSupers.getOrDefault(current, Set.of())) {
                if (result.add(parent)) {
                    queue.add(parent);
                }
            }
        }
        return result;
    }

    private static boolean isAncestor(Node ancestor, Node node) {
        Node current = node;
        while (current != null) {
            if (current == ancestor) {
                return true;
            }
            current = current.getParentNode().orElse(null);
        }
        return false;
    }

    private static String removeImportLines(String unix, List<ImportDeclaration> redundant) {
        String result = unix;
        for (ImportDeclaration imp : redundant) {
            StringBuilder pattern = new StringBuilder("^import\\s+");
            if (imp.isStatic()) {
                pattern.append("static\\s+");
            }
            pattern.append(Pattern.quote(imp.getNameAsString()));
            pattern.append("\\s*;\\R?");
            result = Pattern.compile(pattern.toString(), Pattern.MULTILINE).matcher(result).replaceFirst("");
        }
        return result.replaceAll("\\n{3,}", "\n\n");
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof RedundantJavaImportsFormatter other && other.revision == revision;
    }

    @Override
    public int hashCode() {
        return revision;
    }
}
