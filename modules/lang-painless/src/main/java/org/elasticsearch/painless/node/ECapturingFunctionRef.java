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
import org.elasticsearch.painless.Scope.Variable;
import org.elasticsearch.painless.ir.CapturingFuncRefNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Represents a capturing function reference.  For member functions that require a this reference, ie not static.
 */
public final class ECapturingFunctionRef extends AExpression implements ILambda {
    private final String variable;
    private final String call;

    private FunctionRef ref;
    private Variable captured;
    private String defPointer;

    public ECapturingFunctionRef(Location location, String variable, String call) {
        super(location);

        this.variable = Objects.requireNonNull(variable);
        this.call = Objects.requireNonNull(call);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        captured = scope.getVariable(location, variable);
        if (expected == null) {
            if (captured.getType() == def.class) {
                // dynamic implementation
                defPointer = "D" + variable + "." + call + ",1";
            } else {
                // typed implementation
                defPointer = "S" + captured.getCanonicalTypeName() + "." + call + ",1";
            }
            actual = String.class;
        } else {
            defPointer = null;
            // static case
            if (captured.getType() != def.class) {
                ref = FunctionRef.create(scriptRoot.getPainlessLookup(), scriptRoot.getFunctionTable(), location,
                        expected, captured.getCanonicalTypeName(), call, 1);
            }
            actual = expected;
        }
    }

    @Override
    CapturingFuncRefNode write(ClassNode classNode) {
        CapturingFuncRefNode capturingFuncRefNode = new CapturingFuncRefNode();

        capturingFuncRefNode.setLocation(location);
        capturingFuncRefNode.setExpressionType(actual);
        capturingFuncRefNode.setCapturedName(captured.getName());
        capturingFuncRefNode.setName(call);
        capturingFuncRefNode.setPointer(defPointer);
        capturingFuncRefNode.setFuncRef(ref);;

        return capturingFuncRefNode;
    }

    @Override
    public String getPointer() {
        return defPointer;
    }

    @Override
    public List<Class<?>> getCaptures() {
        return Collections.singletonList(captured.getType());
    }

    @Override
    public String toString() {
        return singleLineToString(variable, call);
    }
}
