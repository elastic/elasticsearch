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

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.FunctionRef;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.ScriptRoot;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

import java.util.Objects;
import java.util.Set;

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
    void extractVariables(Set<String> variables) {
        variables.add(variable);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Locals locals) {
        captured = locals.getVariable(location, variable);
        if (expected == null) {
            if (captured.clazz == def.class) {
                // dynamic implementation
                defPointer = "D" + variable + "." + call + ",1";
            } else {
                // typed implementation
                defPointer = "S" + PainlessLookupUtility.typeToCanonicalTypeName(captured.clazz) + "." + call + ",1";
            }
            actual = String.class;
        } else {
            defPointer = null;
            // static case
            if (captured.clazz != def.class) {
                ref = FunctionRef.create(scriptRoot.getPainlessLookup(), scriptRoot.getFunctionTable(), location,
                        expected, PainlessLookupUtility.typeToCanonicalTypeName(captured.clazz), call, 1);
            }
            actual = expected;
        }
    }

    @Override
    void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);
        if (defPointer != null) {
            // dynamic interface: push captured parameter on stack
            // TODO: don't do this: its just to cutover :)
            methodWriter.push((String)null);
            methodWriter.visitVarInsn(MethodWriter.getType(captured.clazz).getOpcode(Opcodes.ILOAD), captured.getSlot());
        } else if (ref == null) {
            // typed interface, dynamic implementation
            methodWriter.visitVarInsn(MethodWriter.getType(captured.clazz).getOpcode(Opcodes.ILOAD), captured.getSlot());
            Type methodType = Type.getMethodType(MethodWriter.getType(expected), MethodWriter.getType(captured.clazz));
            methodWriter.invokeDefCall(call, methodType, DefBootstrap.REFERENCE, PainlessLookupUtility.typeToCanonicalTypeName(expected));
        } else {
            // typed interface, typed implementation
            methodWriter.visitVarInsn(MethodWriter.getType(captured.clazz).getOpcode(Opcodes.ILOAD), captured.getSlot());
            methodWriter.invokeLambdaCall(ref);
        }
    }

    @Override
    public String getPointer() {
        return defPointer;
    }

    @Override
    public Type[] getCaptures() {
        return new Type[] { MethodWriter.getType(captured.clazz) };
    }

    @Override
    public String toString() {
        return singleLineToString(variable, call);
    }
}
