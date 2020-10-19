/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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

    public String getShiftCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(shiftType);
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
