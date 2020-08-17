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
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.phase.IRTreeVisitor;
import org.elasticsearch.painless.symbol.WriteScope;
import org.objectweb.asm.Type;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.painless.symbol.WriteScope.Variable;

public class InvokeCallDefNode extends ArgumentsNode {

    /* ---- begin node data ---- */

    private String name;

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    /* ---- end node data, begin visitor ---- */

    @Override
    public <Scope> void visit(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        irTreeVisitor.visitInvokeCallDef(this, scope);
    }

    @Override
    public <Scope> void visitChildren(IRTreeVisitor<Scope> irTreeVisitor, Scope scope) {
        for (ExpressionNode argumentNode : getArgumentNodes()) {
            argumentNode.visit(irTreeVisitor, scope);
        }
    }

    /* ---- end visitor ---- */

    /**
     * Writes an invokedynamic instruction for a call with an unknown receiver type.
     */
    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, WriteScope writeScope) {
        methodWriter.writeDebugInfo(location);

        // its possible to have unknown functional interfaces
        // as arguments that require captures; the set of
        // captures with call arguments is ambiguous so
        // additional information is encoded to indicate
        // which are values are arguments and which are captures
        StringBuilder defCallRecipe = new StringBuilder();
        List<Object> boostrapArguments = new ArrayList<>();
        List<Class<?>> typeParameters = new ArrayList<>();
        int capturedCount = 0;

        // add an Object class as a placeholder type for the receiver
        typeParameters.add(Object.class);

        for (int i = 0; i < getArgumentNodes().size(); ++i) {
            ExpressionNode argumentNode = getArgumentNodes().get(i);
            argumentNode.write(classWriter, methodWriter, writeScope);

            typeParameters.add(argumentNode.getExpressionType());

            // handle the case for unknown functional interface
            // to hint at which values are the call's arguments
            // versus which values are captures
            if (argumentNode instanceof DefInterfaceReferenceNode) {
                DefInterfaceReferenceNode defInterfaceReferenceNode = (DefInterfaceReferenceNode)argumentNode;
                boostrapArguments.add(defInterfaceReferenceNode.getDefReferenceEncoding());

                // the encoding uses a char to indicate the number of captures
                // where the value is the number of current arguments plus the
                // total number of captures for easier capture count tracking
                // when resolved at runtime
                char encoding = (char)(i + capturedCount);
                defCallRecipe.append(encoding);
                capturedCount += defInterfaceReferenceNode.getCaptures().size();

                for (String capturedName : defInterfaceReferenceNode.getCaptures()) {
                    Variable capturedVariable = writeScope.getVariable(capturedName);
                    typeParameters.add(capturedVariable.getType());
                }
            }
        }

        Type[] asmParameterTypes = new Type[typeParameters.size()];

        for (int index = 0; index < asmParameterTypes.length; ++index) {
            asmParameterTypes[index] = MethodWriter.getType(typeParameters.get(index));
        }

        Type methodType = Type.getMethodType(MethodWriter.getType(getExpressionType()), asmParameterTypes);

        boostrapArguments.add(0, defCallRecipe.toString());
        methodWriter.invokeDefCall(name, methodType, DefBootstrap.METHOD_CALL, boostrapArguments.toArray());
    }
}
