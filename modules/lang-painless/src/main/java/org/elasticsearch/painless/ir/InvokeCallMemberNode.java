/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.ir;

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.lookup.PainlessClassBinding;
import org.elasticsearch.painless.lookup.PainlessInstanceBinding;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.phase.IRTreeVisitor;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;

public class InvokeCallMemberNode extends ArgumentsNode {

    /* ---- begin node data ---- */

    private String name;
    private LocalFunction function;
    private PainlessMethod method;
    private PainlessMethod thisMethod;
    private PainlessClassBinding classBinding;
    private PainlessInstanceBinding instanceBinding;
    private boolean isStatic;

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setFunction(LocalFunction function) {
        this.function = function;
    }

    public LocalFunction getFunction() {
        return function;
    }

    public void setMethod(PainlessMethod method) {
        this.method = method;
    }

    public PainlessMethod getMethod() {
        return method;
    }

    public void setThisMethod(PainlessMethod thisMethod) {
        this.thisMethod = thisMethod;
    }

    public PainlessMethod getThisMethod() {
        return thisMethod;
    }

    public void setClassBinding(PainlessClassBinding classBinding) {
        this.classBinding = classBinding;
    }

    public PainlessClassBinding getClassBinding() {
        return classBinding;
    }

    public void setInstanceBinding(PainlessInstanceBinding instanceBinding) {
        this.instanceBinding = instanceBinding;
    }

    public PainlessInstanceBinding getInstanceBinding() {
        return instanceBinding;
    }

    public void setStatic(boolean isStatic) {
        this.isStatic = isStatic;
    }

    public boolean isStatic() {
        return isStatic;
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitInvokeCallMember(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        for (ExpressionNode argumentNode : getArgumentNodes()) {
            argumentNode.visit(irTreeVisitor, scope);
        }
    }

    /* ---- end visitor ---- */

    public InvokeCallMemberNode(Location location) {
        super(location);
    }

}
