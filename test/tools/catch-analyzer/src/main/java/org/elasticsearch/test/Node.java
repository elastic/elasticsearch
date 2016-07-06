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

package org.elasticsearch.test;

import org.objectweb.asm.Opcodes;
import org.objectweb.asm.Type;
import org.objectweb.asm.tree.AbstractInsnNode;
import org.objectweb.asm.tree.MethodInsnNode;
import org.objectweb.asm.tree.analysis.AnalyzerException;
import org.objectweb.asm.tree.analysis.BasicValue;
import org.objectweb.asm.tree.analysis.Frame;
import org.objectweb.asm.tree.analysis.Interpreter;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/** Node in the flow graph. contains set of outgoing edges for the next instructions possible */
class Node extends Frame<BasicValue> {
    Set<Integer> edges = new HashSet<>();
    MethodAnalyzer parent;
    
    Node(int nLocals, int nStack, MethodAnalyzer parent) {
        super(nLocals, nStack);
        this.parent = parent;
    }
    
    Node(Frame<? extends BasicValue> src, MethodAnalyzer parent) {
        super(src);
        this.parent = parent;
    }
    
    @Override
    public void execute(AbstractInsnNode insn, Interpreter<BasicValue> interpreter) throws AnalyzerException {
        // special handling for throwable constructor. if you do new Exception(originalException),
        // the stack value is replaced with originalException, so it survives.
        if (insn.getOpcode() == Opcodes.INVOKESPECIAL) {
            MethodInsnNode node = (MethodInsnNode) insn;
            Type ownerType = Type.getObjectType(node.owner);
            // actually calling a throwable ctor
            if ("<init>".equals(node.name) && ThrowableInterpreter.isThrowable(ownerType, parent.loader)) {
                // but, not if we are ourselves a throwable ctor invoking another ctor!
                if ("<init>".equals(parent.name) && 
                      (ownerType.getInternalName().equals(parent.owner) || 
                       ownerType.getInternalName().equals(parent.superName))) {
                    super.execute(insn, interpreter);
                    return;
                }
                // actually creating a throwable
                List<BasicValue> values = new ArrayList<BasicValue>();
                String desc = ((MethodInsnNode) insn).desc;
                // inspect the arguments, see if original exception is being passed
                boolean sawOriginal = false;
                for (int i = Type.getArgumentTypes(desc).length; i > 0; --i) {
                    BasicValue v = pop();
                    if (v == ThrowableInterpreter.ORIGINAL_THROWABLE) {
                        sawOriginal = true;
                    }
                    values.add(0, v);
                }
                values.add(0, pop());
                interpreter.naryOperation(insn, values);
                // replace the top stack value (from new), if we saw the original
                if (sawOriginal) {
                    pop();
                    push(ThrowableInterpreter.ORIGINAL_THROWABLE);
                }
                return;
            }
        }
        super.execute(insn, interpreter);
    }
    
    // just for debugging
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("edges=" + edges + "\n");
        sb.append("locals=\n");
        for (int i = 0; i < getLocals(); i++) {
            sb.append("\t");
            sb.append(i + "=" + getLocal(i).getType() + "\n");
        }
        sb.append("stack=\n");
        for (int i = 0; i < getStackSize(); i++) {
            sb.append("\t");
            sb.append(i + "=" + getStack(i).getType() + "\n");
        }
        return sb.toString();
    }
}
