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
import com.github.javaparser.ast.comments.Comment;
import com.github.javaparser.ast.expr.AnnotationExpr;
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
import java.util.function.Function;
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
 * <p>
 * Exceptions worth knowing:
 * <ul>
 *   <li>{@code RestClientTestCase} is named like a test base but does not inherit JUnit
 *       {@code Assert} methods</li>
 *   <li>{@code LuceneTestCase} extends {@code Assert}, not {@code RandomizedTest}, so
 *       {@code RandomizedTest} helpers are inherited only by types that extend
 *       {@code RandomizedTest} itself</li>
 *   <li>{@code LuceneTestCase} redeclares {@code assumeTrue}/{@code assumeFalse}/
 *       {@code assumeNoException} but not {@code assumeThat}/{@code assumeNotNull}.
 *       {@code PackagingTestCase} extends {@code Assert}, so it does not inherit those
 *       Lucene assume methods</li>
 *   <li>Inherited nested types are in scope in the class <em>body</em> (and record components),
 *       not in modifiers / {@code extends} / {@code implements}</li>
 *   <li>Javadoc {@code {@link Nested}} still needs the import when the link sits outside an
 *       inheriting body</li>
 * </ul>
 */
public final class RedundantJavaImportsFormatter implements FormatterFunc, Serializable {
    private static final long serialVersionUID = 1L;

    /**
     * Bump this when the rewrite rules change so Spotless invalidates its up-to-date cache.
     */
    private final int revision = 10;

    private static final List<String> REDUNDANT_TEST_STATIC_TYPES = List.of(
        "org.junit.Assert",
        "org.hamcrest.MatcherAssert",
        "com.carrotsearch.randomizedtesting.RandomizedTest"
    );

    // LuceneTestCase redeclares these
    private static final Set<String> LUCENE_INHERITED_ASSUME_METHODS = Set.of("assumeTrue", "assumeFalse", "assumeNoException");

    @Override
    public String apply(String unix) {
        return format(unix);
    }

    static String format(String unix) {
        if (unix.contains("import ") == false) {
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
        List<ImportDeclaration> redundant = cu.getImports().stream().filter(imp -> isRedundant(imp, cu, directSupers)).toList();
        return redundant.isEmpty() ? unix : removeImportLines(unix, redundant);
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
            return isStaticMethodName(simpleName) && staticImportUsages(cu, simpleName).isEmpty();
        }
        if (isNestedTypeImport(name) == false) {
            return false;
        }
        String nestedSimpleName = simpleName(name);
        List<Node> usages = typeUsages(cu, nestedSimpleName);
        // No AST usage: google-java-format already handles a missing token. Keep the import;
        // it may be javadoc, a method reference this scan misses, or similar.
        if (usages.isEmpty()) {
            return false;
        }
        String enclosingSimpleName = enclosingTypeSimpleName(name);
        return usages.stream().allMatch(usage -> inheritsInBody(usage, enclosingSimpleName, directSupers))
            && javadocLinksSimpleName(cu, nestedSimpleName) == false;
    }

    private static boolean javadocLinksSimpleName(CompilationUnit cu, String simpleName) {
        Pattern link = Pattern.compile("\\{@link\\s+" + Pattern.quote(simpleName) + "(?:[#\\s}]|$)");
        for (Comment comment : cu.getAllContainedComments()) {
            if (link.matcher(comment.getContent()).find()) {
                return true;
            }
        }
        return cu.getComment().filter(comment -> link.matcher(comment.getContent()).find()).isPresent();
    }

    private static boolean isRedundantTestFrameworkStaticImport(ImportDeclaration imp, String name) {
        if (imp.isAsterisk()) {
            return REDUNDANT_TEST_STATIC_TYPES.contains(name);
        }
        return isLuceneInheritedAssumeImport(name)
            || REDUNDANT_TEST_STATIC_TYPES.stream().anyMatch(type -> name.startsWith(type + "."));
    }

    private static boolean isLuceneInheritedAssumeImport(String name) {
        return name.startsWith("org.junit.Assume.") && LUCENE_INHERITED_ASSUME_METHODS.contains(simpleName(name));
    }

    private static boolean isInheritedByEveryUsage(ImportDeclaration imp, CompilationUnit cu, Map<String, Set<String>> directSupers) {
        String name = imp.getNameAsString();
        if (imp.isAsterisk()) {
            return cu.findAll(MethodCallExpr.class)
                .stream()
                .filter(call -> call.getScope().isEmpty())
                .allMatch(call -> inheritsTestMethods(call, directSupers, name));
        }
        List<Node> usages = staticImportUsages(cu, simpleName(name));
        return usages.isEmpty() == false && usages.stream().allMatch(usage -> inheritsTestMethods(usage, directSupers, name));
    }

    private static boolean isAssertStyleImport(String name) {
        return name.equals("org.junit.Assert")
            || name.startsWith("org.junit.Assert.")
            || name.equals("org.hamcrest.MatcherAssert")
            || name.startsWith("org.hamcrest.MatcherAssert.");
    }

    private static boolean inheritsTestMethods(Node usage, Map<String, Set<String>> directSupers, String importName) {
        for (Enclosing enclosing : enclosingTypes(usage)) {
            if (enclosing.anonymous() && matchesTestHost(enclosing.typeName(), importName)) {
                return true;
            }
            for (String inherited : transitiveSupers(enclosing.typeName(), directSupers)) {
                if (matchesTestHost(inherited, importName)) {
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean matchesTestHost(String simpleName, String importName) {
        if (isLuceneInheritedAssumeImport(importName)) {
            return inheritsLuceneAssumeMethods(simpleName);
        }
        if (isAssertStyleImport(importName)) {
            return inheritsAssertMethods(simpleName);
        }
        return simpleName.equals("RandomizedTest");
    }

    private static boolean inheritsAssertMethods(String simpleName) {
        return isLuceneTestCaseHost(simpleName) || simpleName.equals("Assert");
    }

    private static boolean inheritsLuceneAssumeMethods(String simpleName) {
        // PackagingTestCase extends Assert, not LuceneTestCase.
        return isLuceneTestCaseHost(simpleName) && simpleName.equals("PackagingTestCase") == false;
    }

    private static boolean isLuceneTestCaseHost(String simpleName) {
        if (simpleName.equals("RandomizedTest") || simpleName.equals("RestClientTestCase")) {
            return false;
        }
        return simpleName.equals("LuceneTestCase") || simpleName.endsWith("TestCase");
    }

    private static boolean isStaticMethodName(String simpleName) {
        return simpleName.isEmpty() == false && Character.isLowerCase(simpleName.charAt(0));
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
        for (Node current = usage.getParentNode().orElse(null); current != null; current = current.getParentNode().orElse(null)) {
            if (current instanceof MethodDeclaration method && anyNamed(method.getParameters(), Parameter::getNameAsString, simpleName)) {
                return true;
            }
            if (current instanceof ConstructorDeclaration constructor
                && anyNamed(constructor.getParameters(), Parameter::getNameAsString, simpleName)) {
                return true;
            }
            if (current instanceof LambdaExpr lambda && anyNamed(lambda.getParameters(), Parameter::getNameAsString, simpleName)) {
                return true;
            }
            if (current instanceof CatchClause catchClause && simpleName.equals(catchClause.getParameter().getNameAsString())) {
                return true;
            }
            if (current instanceof ForEachStmt forEach
                && anyNamed(forEach.getVariable().getVariables(), VariableDeclarator::getNameAsString, simpleName)) {
                return true;
            }
            if (current instanceof ForStmt forStmt
                && forStmt.getInitialization()
                    .stream()
                    .anyMatch(
                        init -> init instanceof VariableDeclarationExpr declaration
                            && anyNamed(declaration.getVariables(), VariableDeclarator::getNameAsString, simpleName)
                    )) {
                return true;
            }
            if (current instanceof TryStmt tryStmt
                && tryStmt.getResources()
                    .stream()
                    .anyMatch(
                        resource -> resource instanceof VariableDeclarationExpr declaration
                            && anyNamed(declaration.getVariables(), VariableDeclarator::getNameAsString, simpleName)
                    )) {
                return true;
            }
            if (current instanceof BlockStmt block && hasLocalVariableNamedBefore(block, usage, simpleName)) {
                return true;
            }
            if (current instanceof ClassOrInterfaceDeclaration clazz && hasFieldNamed(clazz.getFields(), simpleName)) {
                return true;
            }
            if (current instanceof RecordDeclaration recordDecl
                && anyNamed(recordDecl.getParameters(), Parameter::getNameAsString, simpleName)) {
                return true;
            }
        }
        return false;
    }

    private static <T> boolean anyNamed(Iterable<T> nodes, Function<T, String> name, String simpleName) {
        for (T node : nodes) {
            if (simpleName.equals(name.apply(node))) {
                return true;
            }
        }
        return false;
    }

    private static boolean hasFieldNamed(Iterable<FieldDeclaration> fields, String simpleName) {
        for (FieldDeclaration field : fields) {
            if (anyNamed(field.getVariables(), VariableDeclarator::getNameAsString, simpleName)) {
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
                && anyNamed(declaration.getVariables(), VariableDeclarator::getNameAsString, simpleName)) {
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
        return simpleName(lastDot < 0 ? name : name.substring(0, lastDot));
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
            addSupers(directSupers, type.getNameAsString(), type.getImplementedTypes());
        }
        for (EnumDeclaration type : cu.findAll(EnumDeclaration.class)) {
            addSupers(directSupers, type.getNameAsString(), type.getImplementedTypes());
        }
        return directSupers;
    }

    @SafeVarargs
    private static void addSupers(Map<String, Set<String>> directSupers, String typeName, Iterable<ClassOrInterfaceType>... lists) {
        Set<String> supers = directSupers.computeIfAbsent(typeName, key -> new HashSet<>());
        for (Iterable<ClassOrInterfaceType> list : lists) {
            for (ClassOrInterfaceType type : list) {
                supers.add(type.getNameAsString());
            }
        }
    }

    // usageInBody: members / record components, not the type's own header.
    private record Enclosing(String typeName, boolean usageInBody, boolean anonymous) {}

    private static List<Enclosing> enclosingTypes(Node usage) {
        List<Enclosing> enclosing = new ArrayList<>();
        for (Node current = usage.getParentNode().orElse(null); current != null; current = current.getParentNode().orElse(null)) {
            if (current instanceof ClassOrInterfaceDeclaration clazz) {
                enclosing.add(new Enclosing(clazz.getNameAsString(), isInTypeBody(clazz, usage), false));
            } else if (current instanceof RecordDeclaration recordDecl) {
                enclosing.add(
                    new Enclosing(
                        recordDecl.getNameAsString(),
                        isInTypeBody(recordDecl, usage) || isInRecordComponents(recordDecl, usage),
                        false
                    )
                );
            } else if (current instanceof EnumDeclaration enumDecl) {
                enclosing.add(new Enclosing(enumDecl.getNameAsString(), isInTypeBody(enumDecl, usage), false));
            } else if (current instanceof ObjectCreationExpr creation
                && creation.getAnonymousClassBody().isPresent()
                && creation.getAnonymousClassBody().get().stream().anyMatch(bodyDecl -> isAncestor(bodyDecl, usage))) {
                enclosing.add(new Enclosing(creation.getType().getNameAsString(), true, true));
            }
        }
        return enclosing;
    }

    private static boolean inheritsInBody(Node usage, String enclosingSimpleName, Map<String, Set<String>> directSupers) {
        for (Enclosing enclosing : enclosingTypes(usage)) {
            if (enclosing.usageInBody() == false) {
                continue;
            }
            if (enclosing.anonymous() && enclosing.typeName().equals(enclosingSimpleName)) {
                return true;
            }
            if (transitiveSupers(enclosing.typeName(), directSupers).contains(enclosingSimpleName)) {
                return true;
            }
        }
        return false;
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
        for (Node current = node; current != null; current = current.getParentNode().orElse(null)) {
            if (current == ancestor) {
                return true;
            }
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
        return removed ? result.replaceAll("\\n{3,}", "\n\n") : unix;
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
