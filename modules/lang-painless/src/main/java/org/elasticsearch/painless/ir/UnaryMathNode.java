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

public class UnaryMathNode extends UnaryNode {

    /* ---- begin node data ---- */

    private Operation operation;
    private Class<?> unaryType;
    private boolean cat;
    private boolean originallyExplicit; // record whether there was originally an explicit cast

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

    public void setCat(boolean cat) {
        this.cat = cat;
    }

    public boolean getCat() {
        return cat;
    }

    public void setOriginallyExplicit(boolean originallyExplicit) {
        this.originallyExplicit = originallyExplicit;
    }

    public boolean getOriginallyExplicit() {
        return originallyExplicit;
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
