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
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.phase.IRTreeVisitor;

import java.util.List;

public class FunctionNode extends IRNode {

    /* ---- begin tree structure ---- */

    private BlockNode blockNode;

    public void setBlockNode(BlockNode blockNode) {
        this.blockNode = blockNode;
    }

    public BlockNode getBlockNode() {
        return blockNode;
    }

    /* ---- end tree structure, begin node data ---- */

    private String name;
    private Class<?> returnType;
    private List<Class<?>> typeParameters = List.of();
    private List<String> parameterNames = List.of();
    private int maxLoopCounter;
    private boolean isStatic;
    private boolean varArgs;
    private boolean synthetic;
    private boolean instanceCapture;

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setReturnType(Class<?> returnType) {
        this.returnType = returnType;
    }

    public Class<?> getReturnType() {
        return returnType;
    }

    public String getReturnCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(returnType);
    }

    public void setTypeParameters(List<Class<?>> typeParameters) {
        this.typeParameters = List.copyOf(typeParameters);
    }

    public List<Class<?>> getTypeParameters() {
        return typeParameters;
    }

    public void setParameterNames(List<String> parameterNames) {
        this.parameterNames = List.copyOf(parameterNames);
    }

    public List<String> getParameterNames() {
        return parameterNames;
    }

    public void setMaxLoopCounter(int maxLoopCounter) {
        this.maxLoopCounter = maxLoopCounter;
    }

    public int getMaxLoopCounter() {
        return maxLoopCounter;
    }

    public void setStatic(boolean isStatic) {
        this.isStatic = isStatic;
    }

    public boolean isStatic() {
        return isStatic;
    }

    public void setVarArgs(boolean varArgs) {
        this.varArgs = varArgs;
    }

    public boolean isVarArgs() {
        return varArgs;
    }

    public void setSynthetic(boolean synthetic) {
        this.synthetic = synthetic;
    }

    public boolean isSynthetic() {
        return synthetic;
    }

    public void setInstanceCapture(boolean instanceCapture) {
        this.instanceCapture = instanceCapture;
    }

    public boolean hasInstanceCapture() {
        return instanceCapture;
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitFunction(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        getBlockNode().visit(irTreeVisitor, scope);
    }

    /* ---- end visitor ---- */

    public FunctionNode(Location location) {
        super(location);
    }

}
