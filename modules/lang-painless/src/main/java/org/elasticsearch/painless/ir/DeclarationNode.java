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
import org.elasticsearch.painless.phase.IRTreeVisitor;
import org.elasticsearch.painless.symbol.WriteScope;
import org.elasticsearch.painless.symbol.WriteScope.Variable;
import org.objectweb.asm.Opcodes;

public class DeclarationNode extends StatementNode {

    /* ---- begin tree structure ---- */

    private ExpressionNode expressionNode;

    public void setExpressionNode(ExpressionNode expressionNode) {
        this.expressionNode = expressionNode;
    }

    public ExpressionNode getExpressionNode() {
        return expressionNode;
    }

    /* ---- end tree structure, begin node data ---- */

    protected String name;
    protected Class<?> declarationType;

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setDeclarationType(Class<?> declarationType) {
        this.declarationType = declarationType;
    }

    public Class<?> getDeclarationType() {
        return declarationType;
    }

    public String getDeclarationCanonicalTypeName() {
        return PainlessLookupUtility.typeToCanonicalTypeName(declarationType);
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitDeclaration(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        if (expressionNode != null) {
            expressionNode.visit(irTreeVisitor, scope);
        }
    }

    /* ---- end visitor ---- */

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, WriteScope writeScope) {
        methodWriter.writeStatementOffset(location);

        Variable variable = writeScope.defineVariable(declarationType, name);

        if (expressionNode == null) {
            Class<?> sort = variable.getType();

            if (sort == void.class || sort == boolean.class || sort == byte.class ||
                    sort == short.class || sort == char.class || sort == int.class) {
                methodWriter.push(0);
            } else if (sort == long.class) {
                methodWriter.push(0L);
            } else if (sort == float.class) {
                methodWriter.push(0F);
            } else if (sort == double.class) {
                methodWriter.push(0D);
            } else {
                methodWriter.visitInsn(Opcodes.ACONST_NULL);
            }
        } else {
            expressionNode.write(classWriter, methodWriter, writeScope);
        }

        methodWriter.visitVarInsn(variable.getAsmType().getOpcode(Opcodes.ISTORE), variable.getSlot());
    }
}
