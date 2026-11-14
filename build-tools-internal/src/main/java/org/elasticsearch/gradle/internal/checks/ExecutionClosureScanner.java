/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.checks;

import org.codehaus.groovy.ast.ASTNode;
import org.codehaus.groovy.ast.CodeVisitorSupport;
import org.codehaus.groovy.ast.Parameter;
import org.codehaus.groovy.ast.expr.ClosureExpression;
import org.codehaus.groovy.ast.expr.DeclarationExpression;
import org.codehaus.groovy.ast.expr.Expression;
import org.codehaus.groovy.ast.expr.MapExpression;
import org.codehaus.groovy.ast.expr.MethodCallExpression;
import org.codehaus.groovy.ast.expr.PropertyExpression;
import org.codehaus.groovy.ast.expr.TupleExpression;
import org.codehaus.groovy.ast.expr.VariableExpression;
import org.codehaus.groovy.ast.stmt.BlockStatement;
import org.codehaus.groovy.ast.stmt.CatchStatement;
import org.codehaus.groovy.ast.stmt.ForStatement;
import org.codehaus.groovy.control.SourceUnit;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A deliberately narrow, syntax-only ratchet, not a proof of configuration-cache compatibility.
 * Parses without resolving imports or executing scripts. Only known task execution closures and
 * direct model references are checked; aliases of model objects and arbitrary helper calls are
 * outside this check. Configuration closures and unrelated receivers must remain usable.
 * Qualified accesses are counted at their model receiver, not again at file()/sourceSets.
 */
final class ExecutionClosureScanner extends CodeVisitorSupport {
    static final String RULE_ID = "execution-closure-project-reference";
    private static final Set<String> EXECUTION_METHODS = Set.of("doFirst", "doLast", "onlyIf");
    private static final Set<String> MODEL_PROPERTIES = Set.of("project", "rootProject", "gradle", "sourceSets");
    private final Deque<Map<String, Boolean>> scopes = new ArrayDeque<>();
    private final List<Finding> findings = new ArrayList<>();
    private boolean taskContext;
    private int executionDepth;

    record Finding(int line, int column, String reference) {}

    static List<Finding> scan(String name, String content) {
        SourceUnit source = SourceUnit.create(name, content);
        source.parse();
        source.completePhase();
        source.nextPhase();
        source.convert();
        ExecutionClosureScanner visitor = new ExecutionClosureScanner();
        source.getAST().getStatementBlock().visit(visitor);
        source.getAST().getMethods().forEach(method -> {
            visitor.scopes.push(new HashMap<>());
            visitor.parameters(method.getParameters(), false);
            method.getCode().visit(visitor);
            visitor.scopes.pop();
        });
        return visitor.findings.stream()
            .sorted(java.util.Comparator.comparingInt(Finding::line).thenComparingInt(Finding::column))
            .toList();
    }

    @Override
    public void visitBlockStatement(BlockStatement block) {
        scopes.push(new HashMap<>());
        super.visitBlockStatement(block);
        scopes.pop();
    }

    @Override
    public void visitDeclarationExpression(DeclarationExpression declaration) {
        // The initializer resolves before the new local is in scope. Keep block-local names out of siblings.
        declaration.getRightExpression().visit(this);
        if (declaration.isMultipleAssignmentDeclaration()) {
            declaration.getTupleExpression().getExpressions().forEach(expression -> bind(expression, false));
        } else {
            bind(declaration.getVariableExpression(), isTask(declaration.getRightExpression()));
        }
    }

    private void bind(Expression expression, boolean task) {
        if (expression instanceof VariableExpression variable) {
            scopes.peek().put(variable.getName(), task || isTaskType(variable.getType().getName()));
        }
    }

    private static boolean isTaskType(String name) {
        return name.equals("org.gradle.api.Task")
            || name.equals("Task")
            || name.equals("DefaultTask")
            || name.equals("org.gradle.api.DefaultTask");
    }

    private Boolean binding(String name) {
        for (Map<String, Boolean> scope : scopes) {
            if (scope.containsKey(name)) {
                return scope.get(name);
            }
        }
        return null;
    }

    private boolean isTasks(Expression expression) {
        if (expression instanceof VariableExpression variable) {
            return variable.getName().equals("tasks") && binding("tasks") == null;
        }
        if (expression instanceof PropertyExpression property) {
            return property.getPropertyAsString() != null
                && property.getPropertyAsString().equals("tasks")
                && isScriptOrModel(property.getObjectExpression());
        }
        return false;
    }

    private boolean isTask(Expression expression) {
        if (expression instanceof VariableExpression variable) {
            return Boolean.TRUE.equals(binding(variable.getName())) || isTaskType(variable.getType().getName());
        }
        if (expression instanceof PropertyExpression property) {
            return isTasks(property.getObjectExpression());
        }
        if (expression instanceof MethodCallExpression call) {
            return call.getMethodAsString() != null
                && Set.of("register", "named", "getByName", "withType", "matching").contains(call.getMethodAsString())
                && (isTasks(call.getObjectExpression()) || isTask(call.getObjectExpression()));
        }
        return false;
    }

    private boolean isModelVariable(Expression expression) {
        return expression instanceof VariableExpression variable
            && Set.of("project", "rootProject").contains(variable.getName())
            && binding(variable.getName()) == null;
    }

    private boolean isScriptOrModel(Expression expression) {
        return (expression instanceof VariableExpression variable && variable.isThisExpression()) || isModelVariable(expression);
    }

    private boolean implicitReceiver(MethodCallExpression call) {
        return call.isImplicitThis() || isScriptOrModel(call.getObjectExpression());
    }

    @Override
    public void visitMethodCallExpression(MethodCallExpression call) {
        String method = call.getMethodAsString();
        boolean execution = method != null
            && EXECUTION_METHODS.contains(method)
            && ((call.isImplicitThis() && taskContext) || isTask(call.getObjectExpression()));
        boolean legacyTask = "task".equals(method) && call.isImplicitThis() && binding("task") == null;
        boolean configuration = (method != null
            && Set.of("register", "named", "configure", "configureEach", "withType", "all").contains(method)
            && (isTasks(call.getObjectExpression()) || isTask(call.getObjectExpression()))) || legacyTask;
        if (executionDepth > 0
            && method != null
            && implicitReceiver(call)
            && isModelVariable(call.getObjectExpression()) == false
            && (MODEL_PROPERTIES.contains(method) || "file".equals(method))
            && (call.isImplicitThis() == false || binding(method) == null)) {
            add(call, method + "()");
        }
        call.getObjectExpression().visit(this);
        call.getMethod().visit(this);
        List<Expression> arguments = arguments(call);
        if (legacyTask
            && arguments.size() == 1
            && arguments.get(0) instanceof MethodCallExpression declaration
            && isLegacyTaskDeclaration(declaration)) {
            // Groovy parses `task foo(type: Exec) { ... }` as task(foo(map, closure)).
            // Only unwrap this immediate declaration shape, not arbitrary nested foo calls.
            visitArguments(arguments(declaration), false, true);
        } else {
            visitArguments(arguments, execution, configuration);
        }
    }

    private static List<Expression> arguments(MethodCallExpression call) {
        return call.getArguments() instanceof TupleExpression tuple ? tuple.getExpressions() : List.of(call.getArguments());
    }

    private static boolean isLegacyTaskDeclaration(MethodCallExpression call) {
        if (call.isImplicitThis() == false || call.getMethodAsString() == null) {
            return false;
        }
        List<Expression> arguments = arguments(call);
        return (arguments.size() == 1 || arguments.size() == 2 && arguments.get(0) instanceof MapExpression)
            && arguments.get(arguments.size() - 1) instanceof ClosureExpression;
    }

    private void visitArguments(List<Expression> arguments, boolean execution, boolean configuration) {
        for (Expression argument : arguments) {
            if (argument instanceof ClosureExpression closure && (execution || configuration)) {
                boolean previousTaskContext = taskContext;
                taskContext = true;
                if (execution) {
                    executionDepth++;
                }
                visitClosure(closure, true);
                if (execution) {
                    executionDepth--;
                }
                taskContext = previousTaskContext;
            } else {
                argument.visit(this);
            }
        }
    }

    @Override
    public void visitClosureExpression(ClosureExpression closure) {
        visitClosure(closure, false);
    }

    private void visitClosure(ClosureExpression closure, boolean taskParameters) {
        scopes.push(new HashMap<>());
        parameters(closure.getParameters(), taskParameters);
        if (closure.isParameterSpecified() == false) {
            scopes.peek().put("it", taskParameters);
        }
        closure.getCode().visit(this);
        scopes.pop();
    }

    private void parameters(Parameter[] parameters, boolean taskParameters) {
        if (parameters != null) {
            for (Parameter parameter : parameters) {
                scopes.peek().put(parameter.getName(), taskParameters || isTaskType(parameter.getType().getName()));
            }
        }
    }

    @Override
    public void visitForLoop(ForStatement loop) {
        loop.getCollectionExpression().visit(this);
        scopes.push(new HashMap<>());
        parameters(new Parameter[] { loop.getVariable() }, false);
        loop.getLoopBlock().visit(this);
        scopes.pop();
    }

    @Override
    public void visitCatchStatement(CatchStatement statement) {
        scopes.push(new HashMap<>());
        parameters(new Parameter[] { statement.getVariable() }, false);
        statement.getCode().visit(this);
        scopes.pop();
    }

    @Override
    public void visitVariableExpression(VariableExpression variable) {
        if (executionDepth > 0 && MODEL_PROPERTIES.contains(variable.getName()) && binding(variable.getName()) == null) {
            add(variable, variable.getName());
        }
    }

    @Override
    public void visitPropertyExpression(PropertyExpression property) {
        if (executionDepth > 0
            && property.getPropertyAsString() != null
            && MODEL_PROPERTIES.contains(property.getPropertyAsString())
            && isModelVariable(property.getObjectExpression()) == false
            && (isScriptOrModel(property.getObjectExpression()) || isTask(property.getObjectExpression()))) {
            add(property, property.getPropertyAsString());
        }
        // A qualified access such as project.sourceSets is counted at its model receiver only.
        // Property names, including map keys, are constants rather than free variable references.
        property.getObjectExpression().visit(this);
        property.getProperty().visit(this);
    }

    private void add(ASTNode node, String reference) {
        findings.add(new Finding(node.getLineNumber(), node.getColumnNumber(), reference));
    }
}
