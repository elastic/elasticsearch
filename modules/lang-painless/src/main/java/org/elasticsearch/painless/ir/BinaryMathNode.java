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

public class BinaryMathNode extends BinaryNode {

    /* ---- begin node data ---- */

    private Operation operation;
    private Class<?> binaryType;
    private Class<?> shiftType;
    private int flags;
    private int regexLimit;

    public void setOperation(Operation operation) {
        this.operation = operation;
    }

    public Operation getOperation() {
        return operation;
    }

    public void setBinaryType(Class<?> binaryType) {
        this.binaryType = binaryType;
    }

    public Class<?> getBinaryType() {
        return binaryType;
    }

    public String getBinaryCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(binaryType);
    }

    public void setShiftType(Class<?> shiftType) {
        this.shiftType = shiftType;
    }

    public Class<?> getShiftType() {
        return shiftType;
    }

    public void setFlags(int flags) {
        this.flags = flags;
    }

    public int getFlags() {
        return flags;
    }

    public void setRegexLimit(int regexLimit) {
        this.regexLimit = regexLimit;
    }

    public int getRegexLimit() {
        return regexLimit;
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitBinaryMath(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        getLeftNode().visit(irTreeVisitor, scope);
        getRightNode().visit(irTreeVisitor, scope);
    }

    /* ---- end visitor ---- */

    public BinaryMathNode(Location location) {
        super(location);
    }

}
