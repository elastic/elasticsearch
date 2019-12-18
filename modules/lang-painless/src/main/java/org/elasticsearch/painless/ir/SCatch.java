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
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.ScriptRoot;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;

import java.util.Objects;
import java.util.Set;

public class CatchNode extends StatementNode {

    /* --- begin tree structure --- */

    protected DeclarationNode declarationNode;

    public void setChildNode(ExpressionNode declarationNode) {
        this.declarationNode = declarationNode;
    }

    public ExpressionNode getChildNode() {
        return declarationNode;
    }

    /* --- end tree structure, begin node data --- */

    protected final Location location;
    protected boolean allEscape;

    /* --- end node date --- */

    public CatchNode(Location location) {
        this.location = Objects.requireNonNull(location);
    }



    Label begin = null;
    Label end = null;
    Label exception = null;

    @Override
    public void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeStatementOffset(location);

        Label jump = new Label();

        methodWriter.mark(jump);
        methodWriter.visitVarInsn(
                MethodWriter.getType(declaration.variable.clazz).getOpcode(Opcodes.ISTORE), declaration.variable.getSlot());

        if (block != null) {
            block.continu = continu;
            block.brake = brake;
            block.write(classWriter, methodWriter, globals);
        }

        methodWriter.visitTryCatchBlock(begin, end, jump, MethodWriter.getType(declaration.variable.clazz).getInternalName());

        if (exception != null && (block == null || !block.allEscape)) {
            methodWriter.goTo(exception);
        }
    }
}
