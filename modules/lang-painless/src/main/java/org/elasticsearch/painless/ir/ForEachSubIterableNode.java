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
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.phase.IRTreeVisitor;

/**
 * Represents a for-each loop for iterables.
 */
public class ForEachSubIterableNode extends ConditionNode {

    /* ---- begin node data ---- */

    private Class<?> variableType;
    private String variableName;
    private PainlessCast cast;
    private Class<?> iterableType;
    private String iterableName;
    private PainlessMethod method;

    public void setVariableType(Class<?> variableType) {
        this.variableType = variableType;
    }

    public Class<?> getVariableType() {
        return variableType;
    }

    public String getVariableCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(variableType);
    }

    public void setVariableName(String variableName) {
        this.variableName = variableName;
    }

    public String getVariableName() {
        return variableName;
    }

    public void setCast(PainlessCast cast) {
        this.cast = cast;
    }

    public PainlessCast getCast() {
        return cast;
    }

    public void setIterableType(Class<?> iterableType) {
        this.iterableType = iterableType;
    }

    public Class<?> getIterableType() {
        return iterableType;
    }

    public String getIterableCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(iterableType);
    }

    public void setIterableName(String iterableName) {
        this.iterableName = iterableName;
    }

    public String getIterableName() {
        return iterableName;
    }

    public void setMethod(PainlessMethod method) {
        this.method = method;
    }

    public PainlessMethod getMethod() {
        return method;
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitForEachSubIterableLoop(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        getConditionNode().visit(irTreeVisitor, scope);
        getBlockNode().visit(irTreeVisitor, scope);
    }

    /* ---- end visitor ---- */

    public ForEachSubIterableNode(Location location) {
        super(location);
    }

}
