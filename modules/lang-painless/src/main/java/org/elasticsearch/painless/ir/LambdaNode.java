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
import org.elasticsearch.painless.FunctionRef;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.symbol.ScopeTable;
import org.elasticsearch.painless.symbol.ScopeTable.Variable;
import org.objectweb.asm.Opcodes;

import java.util.ArrayList;
import java.util.List;

public class LambdaNode extends ExpressionNode {

    /* ---- begin node data ---- */

    private final List<String> captures = new ArrayList<>();
    private FunctionRef funcRef;

    public void addCapture(String capture) {
        captures.add(capture);
    }

    public List<String> getCaptures() {
        return captures;
    }

    public void setFuncRef(FunctionRef funcRef) {
        this.funcRef = funcRef;
    }

    public FunctionRef getFuncRef() {
        return funcRef;
    }

    /* ---- end node data ---- */

    @Override
    protected void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals, ScopeTable scopeTable) {
        methodWriter.writeDebugInfo(location);

        if (funcRef != null) {
            methodWriter.writeDebugInfo(location);
            // load captures
            for (String capture : captures) {
                Variable variable = scopeTable.getVariable(capture);
                methodWriter.visitVarInsn(variable.getAsmType().getOpcode(Opcodes.ILOAD), variable.getSlot());
            }

            methodWriter.invokeLambdaCall(funcRef);
        } else {
            // placeholder
            methodWriter.push((String)null);
            // load captures
            for (String capture : captures) {
                Variable variable = scopeTable.getVariable(capture);
                methodWriter.visitVarInsn(variable.getAsmType().getOpcode(Opcodes.ILOAD), variable.getSlot());
            }
        }
    }
}
