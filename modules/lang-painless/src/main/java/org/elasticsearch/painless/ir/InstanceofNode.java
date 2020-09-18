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

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.phase.IRTreeVisitor;
import org.elasticsearch.painless.symbol.WriteScope;

public class InstanceofNode extends UnaryNode {

    /* ---- begin node data ---- */

    private Class<?> instanceType;

    public void setInstanceType(Class<?> instanceType) {
        this.instanceType = instanceType;
    }

    public Class<?> getInstanceType() {
        return instanceType;
    }

    public String getInstanceCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(instanceType);
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitInstanceof(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        getChildNode().visit(irTreeVisitor, scope);
    }

    /* ---- end visitor ---- */

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, WriteScope writeScope) {
        getChildNode().write(classWriter, methodWriter, writeScope);

        if (instanceType == def.class) {
            methodWriter.writePop(MethodWriter.getType(getExpressionType()).getSize());
            methodWriter.push(true);
        } else if (getChildNode().getExpressionType().isPrimitive()) {
            methodWriter.writePop(MethodWriter.getType(getExpressionType()).getSize());
            methodWriter.push(PainlessLookupUtility.typeToBoxedType(instanceType).isAssignableFrom(
                    PainlessLookupUtility.typeToBoxedType(getChildNode().getExpressionType())));
        } else {
            methodWriter.instanceOf(MethodWriter.getType(PainlessLookupUtility.typeToBoxedType(instanceType)));
        }
    }
}
