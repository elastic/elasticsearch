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

package org.elasticsearch.painless.tree.node;

import org.elasticsearch.painless.compiler.CompilerSettings;
import org.elasticsearch.painless.compiler.Definition;
import org.elasticsearch.painless.tree.utility.Variables;
import org.elasticsearch.painless.tree.utility.Variables.Variable;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.GeneratorAdapter;

public class STrap extends AStatement {
    protected final String type;
    protected final String name;
    protected final AStatement block;

    protected Variable variable;

    protected Label begin;
    protected Label end;
    protected Label exception;

    public STrap(final String location, final String type, final String name, final AStatement block) {
        super(location);

        this.type = type;
        this.name = name;
        this.block = block;
    }

    @Override
    protected void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        variable = variables.addVariable(location, type, name);

        try {
            variable.type.clazz.asSubclass(Exception.class);
        } catch (final ClassCastException cce) {
            throw new IllegalArgumentException(error("Not an exception type [" + variable.type.name + "]."));
        }

        if (block != null) {
            block.lastSource = lastSource;
            block.inLoop = inLoop;
            block.lastLoop = lastLoop;

            block.analyze(settings, definition, variables);

            methodEscape = block.methodEscape;
            loopEscape = block.loopEscape;
            allEscape = block.allEscape;
            anyContinue = block.anyContinue;
            anyBreak = block.anyBreak;
            statementCount = block.statementCount;
        }
    }

    @Override
    protected void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        final Label jump = new Label();

        adapter.mark(jump);
        adapter.visitVarInsn(variable.type.type.getOpcode(Opcodes.ISTORE), variable.slot);

        if (block != null) {
            block.continu = continu;
            block.brake = brake;
            block.write(settings, definition, adapter);
        }

        adapter.visitTryCatchBlock(begin, end, jump, variable.type.type.getInternalName());

        if (exception != null && !block.allEscape) {
            adapter.goTo(exception);
        }
    }
}
