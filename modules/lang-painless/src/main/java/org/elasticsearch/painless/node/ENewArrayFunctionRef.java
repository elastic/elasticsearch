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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.FunctionRef;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.DefInterfaceReferenceNode;
import org.elasticsearch.painless.ir.FunctionNode;
import org.elasticsearch.painless.ir.NewArrayNode;
import org.elasticsearch.painless.ir.ReturnNode;
import org.elasticsearch.painless.ir.TypedInterfaceReferenceNode;
import org.elasticsearch.painless.ir.VariableNode;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Collections;
import java.util.Objects;

/**
 * Represents a function reference.
 */
public class ENewArrayFunctionRef extends AExpression {

    protected final String type;

    public ENewArrayFunctionRef(Location location, String type) {
        super(location);

        this.type = Objects.requireNonNull(type);
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        if (input.write) {
            throw createError(new IllegalArgumentException(
                    "cannot assign a value to new array function reference with target type [ + " + type  + "]"));
        }

        if (input.read == false) {
            throw createError(new IllegalArgumentException(
                    "not a statement: new array function reference with target type [" + type + "] not used"));
        }

        Output output = new Output();

        Class<?> clazz = scriptRoot.getPainlessLookup().canonicalTypeNameToType(this.type);

        if (clazz == null) {
            throw createError(new IllegalArgumentException("Not a type [" + this.type + "]."));
        }

        String name = scriptRoot.getNextSyntheticName("newarray");
        scriptRoot.getFunctionTable().addFunction(name, clazz, Collections.singletonList(int.class), true, true);

        if (input.expected == null) {
            output.actual = String.class;
            String defReferenceEncoding = "Sthis." + name + ",0";

            DefInterfaceReferenceNode defInterfaceReferenceNode = new DefInterfaceReferenceNode();

            defInterfaceReferenceNode.setLocation(location);
            defInterfaceReferenceNode.setExpressionType(output.actual);
            defInterfaceReferenceNode.setDefReferenceEncoding(defReferenceEncoding);

            output.expressionNode = defInterfaceReferenceNode;
        } else {
            FunctionRef ref = FunctionRef.create(scriptRoot.getPainlessLookup(), scriptRoot.getFunctionTable(),
                    location, input.expected, "this", name, 0);
            output.actual = input.expected;

            TypedInterfaceReferenceNode typedInterfaceReferenceNode = new TypedInterfaceReferenceNode();

            typedInterfaceReferenceNode.setLocation(location);
            typedInterfaceReferenceNode.setExpressionType(output.actual);
            typedInterfaceReferenceNode.setReference(ref);

            output.expressionNode = typedInterfaceReferenceNode;
        }

        VariableNode variableNode = new VariableNode();
        variableNode.setLocation(location);
        variableNode.setExpressionType(int.class);
        variableNode.setName("size");

        NewArrayNode newArrayNode = new NewArrayNode();
        newArrayNode.setLocation(location);
        newArrayNode.setExpressionType(clazz);
        newArrayNode.setInitialize(false);

        newArrayNode.addArgumentNode(variableNode);

        ReturnNode returnNode = new ReturnNode();
        returnNode.setLocation(location);
        returnNode.setExpressionNode(newArrayNode);

        BlockNode blockNode = new BlockNode();
        blockNode.setAllEscape(true);
        blockNode.setStatementCount(1);
        blockNode.addStatementNode(returnNode);

        FunctionNode functionNode = new FunctionNode();
        functionNode.setMaxLoopCounter(0);
        functionNode.setName(name);
        functionNode.setReturnType(clazz);
        functionNode.addTypeParameter(int.class);
        functionNode.addParameterName("size");
        functionNode.setStatic(true);
        functionNode.setVarArgs(false);
        functionNode.setSynthetic(true);
        functionNode.setBlockNode(blockNode);

        classNode.addFunctionNode(functionNode);

        return output;
    }
}
