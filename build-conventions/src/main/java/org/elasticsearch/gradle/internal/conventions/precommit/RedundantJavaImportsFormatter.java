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
import com.github.javaparser.ast.body.BodyDeclaration;
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
    private final int revision = 7;

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
        List<ImportDeclaration> redundant = new ArrayList<>();
        for (ImportDeclaration imp : cu.getImports()) {
            if (isRedundant(imp, cu, directSupers)) {
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

    private static boolean isRedundant(ImportDeclaration imp, CompilationUnit cu, Map<String, Set<String>> directSupers) {
        String name = imp.getNameAsString();
        if (imp.isStatic()) {
            if (isRedundantTestFrameworkStaticImport(imp, name) && isInheritedByEveryUsage(imp, cu, directSupers)) {
                return true;
            }
            if (imp.isAsterisk()) {
                return false;
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
            if (inheritsInBody(usage, enclosingSimpleName, directSupers) == false) {
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

    /**
     * True when every usage of this test-framework static import sits in a type that already
     * inherits the method (including nested classes of those types). Usages in a sibling helper
     * type keep the import. An asterisk import is redundant only when every unscoped method call
     * in the file is inside such a type.
     * <p>
     * JUnit {@code Assert}/{@code Assume} and Hamcrest {@code MatcherAssert} methods are inherited
     * from {@code LuceneTestCase}, {@code org.junit.Assert}, and typical {@code *TestCase} types.
     * {@code RandomizedTest} and {@code RestClientTestCase} do not declare those methods, so their
     * subclasses keep the static imports.
     * <p>
     * {@code RandomizedTest} helpers such as {@code randomAsciiAlphanumOfLengthBetween()} are
     * inherited only by types that extend {@code RandomizedTest} itself. {@code LuceneTestCase}
     * extends {@code Assert}, not {@code RandomizedTest}, so {@code ESTestCase} subclasses do not
     * inherit those helpers unless they redeclare them.
     */
    private static boolean isInheritedByEveryUsage(
        ImportDeclaration imp,
        CompilationUnit cu,
        Map<String, Set<String>> directSupers
    ) {
        boolean assertStyle = isAssertStyleImport(imp.getNameAsString());
        if (imp.isAsterisk()) {
            for (MethodCallExpr call : cu.findAll(MethodCallExpr.class)) {
                if (call.getScope().isEmpty() && inheritsTestMethods(call, directSupers, assertStyle) == false) {
                    return false;
                }
            }
            return true;
        }
        List<Node> usages = staticImportUsages(cu, simpleName(imp.getNameAsString()));
        if (usages.isEmpty()) {
            return false;
        }
        for (Node usage : usages) {
            if (inheritsTestMethods(usage, directSupers, assertStyle) == false) {
                return false;
            }
        }
        return true;
    }

    private static boolean isAssertStyleImport(String name) {
        return name.equals("org.junit.Assert")
            || name.equals("org.junit.Assume")
            || name.equals("org.hamcrest.MatcherAssert")
            || name.startsWith("org.junit.Assert.")
            || name.startsWith("org.junit.Assume.")
            || name.startsWith("org.hamcrest.MatcherAssert.");
    }

    private static boolean inheritsTestMethods(Node usage, Map<String, Set<String>> directSupers, boolean assertStyle) {
        for (String inherited : inheritedNames(usage, directSupers)) {
            if (assertStyle ? inheritsAssertMethods(inherited) : inheritsRandomizedTestMethods(inherited)) {
                return true;
            }
        }
        return false;
    }

    /**
     * {@code RestClientTestCase} extends {@code RandomizedTest} and is named like a test base, but
     * it does not inherit JUnit {@code Assert} methods. Without a classpath we cannot prove that
     * every {@code *TestCase} does; this is the known exception in this repository.
     */
    private static boolean inheritsAssertMethods(String simpleName) {
        if (simpleName.equals("RandomizedTest") || simpleName.equals("RestClientTestCase")) {
            return false;
        }
        return simpleName.equals("LuceneTestCase") || simpleName.equals("Assert") || simpleName.endsWith("TestCase");
    }

    /**
     * Lucene's {@code LuceneTestCase} extends {@code Assert}, not {@code RandomizedTest}.
     * Only types that actually extend {@code RandomizedTest} inherit its helpers.
     */
    private static boolean inheritsRandomizedTestMethods(String simpleName) {
        return simpleName.equals("RandomizedTest");
    }

    private static boolean isStaticMethodName(String simpleName) {
        return simpleName.isEmpty() == false && Character.isLowerCase(simpleName.charAt(0));
    }

    /**
     * A lowercase static import is in use if it is invoked as a method ({@code rarely()}) or
     * referenced as an identifier that is not shadowed by a local/parameter/field in scope
     * ({@code deprecationLogger.critical(...)}).
     */
    private static boolean isUsedAsStaticImport(CompilationUnit cu, String simpleName) {
        return staticImportUsages(cu, simpleName).isEmpty() == false;
    }

    private static List<Node> staticImportUsages(CompilationUnit cu, String simpleName) {
        List<Node> usages = new ArrayList<>();
        for (MethodCallExpr call : cu.findAll(MethodCallExpr.class)) {
            if (simpleName.equals(call.getNameAsString()) && call.getScope().isEmpty()) {
                usages.add(call);
            }
        }
        for (NameExpr nameExpr : cu.findAll(NameExpr.class)) {
            if (simpleName.equals(nameExpr.getNameAsString()) && isShadowed(nameExpr, simpleName) == false) {
                usages.add(nameExpr);
            }
        }
        return usages;
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

    /**
     * Inherited member types are in scope in the body of the inheriting type (and, for records,
     * the component list), not in that type's own modifiers, type parameters, or
     * {@code extends}/{@code implements} clause. Nested type declarations are members of the
     * enclosing type, so a usage in a nested type's header can still be in scope via the outer
     * type if the outer type inherits the enclosing name.
     */
    private static boolean inheritsInBody(Node usage, String enclosingSimpleName, Map<String, Set<String>> directSupers) {
        Node current = usage.getParentNode().orElse(null);
        while (current != null) {
            if (current instanceof ClassOrInterfaceDeclaration clazz) {
                if (typeInherits(clazz.getNameAsString(), enclosingSimpleName, directSupers) && isInTypeBody(clazz, usage)) {
                    return true;
                }
            } else if (current instanceof RecordDeclaration recordDecl) {
                if (typeInherits(recordDecl.getNameAsString(), enclosingSimpleName, directSupers)
                    && (isInTypeBody(recordDecl, usage) || isInRecordComponents(recordDecl, usage))) {
                    return true;
                }
            } else if (current instanceof EnumDeclaration enumDecl) {
                if (typeInherits(enumDecl.getNameAsString(), enclosingSimpleName, directSupers) && isInTypeBody(enumDecl, usage)) {
                    return true;
                }
            } else if (current instanceof ObjectCreationExpr creation && creation.getAnonymousClassBody().isPresent()) {
                if (creation.getAnonymousClassBody().get().stream().anyMatch(bodyDecl -> isAncestor(bodyDecl, usage))) {
                    String superName = creation.getType().getNameAsString();
                    if (superName.equals(enclosingSimpleName) || typeInherits(superName, enclosingSimpleName, directSupers)) {
                        return true;
                    }
                }
            }
            current = current.getParentNode().orElse(null);
        }
        return false;
    }

    private static boolean typeInherits(String typeName, String enclosingSimpleName, Map<String, Set<String>> directSupers) {
        return transitiveSupers(typeName, directSupers).contains(enclosingSimpleName);
    }

    private static boolean isInTypeBody(TypeDeclaration<?> type, Node usage) {
        for (BodyDeclaration<?> member : type.getMembers()) {
            if (isAncestor(member, usage)) {
                return true;
            }
        }
        return false;
    }

    private static boolean isInRecordComponents(RecordDeclaration recordDecl, Node usage) {
        for (Parameter parameter : recordDecl.getParameters()) {
            if (isAncestor(parameter, usage)) {
                return true;
            }
        }
        return false;
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
        boolean removed = false;
        for (ImportDeclaration imp : redundant) {
            StringBuilder pattern = new StringBuilder("^import\\s+");
            if (imp.isStatic()) {
                pattern.append("static\\s+");
            }
            pattern.append(Pattern.quote(imp.getNameAsString()));
            if (imp.isAsterisk()) {
                pattern.append("\\.\\*");
            }
            pattern.append("\\s*;\\R?");
            String next = Pattern.compile(pattern.toString(), Pattern.MULTILINE).matcher(result).replaceFirst("");
            if (next.equals(result) == false) {
                result = next;
                removed = true;
            }
        }
        if (removed == false) {
            return unix;
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
