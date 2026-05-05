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
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.phase.IRTreeVisitor;

public class UnaryMathNode extends UnaryNode {

    /* ---- begin node data ---- */

    private Operation operation;
    private Class<?> unaryType;
    private int flags;

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setUnaryType(Class<?> unaryType) {
        this.unaryType = unaryType;
    }

    public Class<?> getUnaryType() {
        return unaryType;
    }

    public String getUnaryCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(unaryType);
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public int getFlags() {
        return flags;
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitUnaryMath(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        getChildNode().visit(irTreeVisitor, scope);
    }

    /* ---- end visitor ---- */

    public UnaryMathNode(Location location) {
        super(location);
    }

}
