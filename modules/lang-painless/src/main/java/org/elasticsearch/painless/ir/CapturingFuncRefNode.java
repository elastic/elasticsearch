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
import org.elasticsearch.painless.FunctionRef;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;

public class CapturingFuncRefNode extends ExpressionNode {

    /* ---- begin tree structure ---- */

    @Override
    public CapturingFuncRefNode setTypeNode(TypeNode typeNode) {
        super.setTypeNode(typeNode);
        return this;
    }

    /* ---- end tree structure, begin node data ---- */

    protected String name;
    protected Variable captured;
    protected FunctionRef funcRef;
    protected String pointer;

    public CapturingFuncRefNode setName(String name) {
        this.name = name;
        return this;
    }

    public String getName() {
        return name;
    }

    public CapturingFuncRefNode setCaptured(Variable captured) {
        this.captured = captured;
        return this;
    }

    public Variable getCaptured() {
        return captured;
    }

    public CapturingFuncRefNode setFuncRef(FunctionRef funcRef) {
        this.funcRef = funcRef;
        return this;
    }

    public FunctionRef getFuncRef() {
        return funcRef;
    }

    public CapturingFuncRefNode setPointer(String pointer) {
        this.pointer = pointer;
        return this;
    }

    public String getPointer() {
        return pointer;
    }

    @Override
    public CapturingFuncRefNode setLocation(Location location) {
        super.setLocation(location);
        return this;
    }

    /* ---- end node data ---- */

    public CapturingFuncRefNode() {
        // do nothing
    }

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);
        if (pointer != null) {
            // dynamic interface: placeholder for run-time lookup
            methodWriter.push((String)null);
            methodWriter.visitVarInsn(MethodWriter.getType(captured.clazz).getOpcode(Opcodes.ILOAD), captured.getSlot());
        } else if (funcRef == null) {
            // typed interface, dynamic implementation
            methodWriter.visitVarInsn(MethodWriter.getType(captured.clazz).getOpcode(Opcodes.ILOAD), captured.getSlot());
            Type methodType = Type.getMethodType(MethodWriter.getType(getType()), MethodWriter.getType(captured.clazz));
            methodWriter.invokeDefCall(name, methodType, DefBootstrap.REFERENCE, getCanonicalTypeName());
        } else {
            // typed interface, typed implementation
            methodWriter.visitVarInsn(MethodWriter.getType(captured.clazz).getOpcode(Opcodes.ILOAD), captured.getSlot());
            methodWriter.invokeLambdaCall(funcRef);
        }
    }
}
