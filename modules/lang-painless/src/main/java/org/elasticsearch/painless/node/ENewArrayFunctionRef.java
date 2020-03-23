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
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.FunctionNode;
import org.elasticsearch.painless.ir.NewArrayFuncRefNode;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a function reference.
 */
public class ENewArrayFunctionRef extends AExpression implements ILambda {

    protected final String type;

    // TODO: #54015
    private String defPointer;

    public ENewArrayFunctionRef(Location location, String type) {
        super(location);

        this.type = Objects.requireNonNull(type);
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        Output output = new Output();

        SReturn code = new SReturn(location, new ENewArray(location, type, Arrays.asList(new EVariable(location, "size")), false));
        SFunction function = new SFunction(
                location, type, scriptRoot.getNextSyntheticName("newarray"),
                Collections.singletonList("int"), Collections.singletonList("size"),
                new SBlock(location, Collections.singletonList(code)), true, true, true, false);
        function.generateSignature(scriptRoot.getPainlessLookup());
        FunctionNode functionNode = function.writeFunction(classNode, scriptRoot);
        scriptRoot.getFunctionTable().addFunction(function.name, function.returnType, function.typeParameters, true, true);

        FunctionRef ref;

        if (input.expected == null) {
            ref = null;
            output.actual = String.class;
            defPointer = "Sthis." + function.name + ",0";
        } else {
            defPointer = null;
            ref = FunctionRef.create(scriptRoot.getPainlessLookup(), scriptRoot.getFunctionTable(),
                    location, input.expected, "this", function.name, 0);
            output.actual = input.expected;
        }

        classNode.addFunctionNode(functionNode);

        NewArrayFuncRefNode newArrayFuncRefNode = new NewArrayFuncRefNode();

        newArrayFuncRefNode.setLocation(location);
        newArrayFuncRefNode.setExpressionType(output.actual);
        newArrayFuncRefNode.setFuncRef(ref);

        output.expressionNode = newArrayFuncRefNode;

        return output;
    }

    @Override
    public String getPointer() {
        return defPointer;
    }

    @Override
    public List<Class<?>> getCaptures() {
        return Collections.emptyList();
    }

    @Override
    public String toString() {
        return singleLineToString(type + "[]", "new");
    }
}
