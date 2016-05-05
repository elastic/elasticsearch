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

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.tree.analyzer.Variables;
import org.elasticsearch.painless.tree.analyzer.Variables.Variable;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.GeneratorAdapter;

public class SDeclaration extends AStatement {
    protected final String type;
    protected final String name;
    protected AExpression expression;

    protected Variable variable;

    public SDeclaration(final String location, final String type, final String name, final AExpression expression) {
        super(location);

        this.type = type;
        this.name = name;
        this.expression = expression;
    }

    @Override
    protected void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        variable = variables.addVariable(location, type, name);

        if (expression != null) {
            expression.expected = variable.type;
            expression.analyze(settings, definition, variables);
            expression = expression.cast(settings, definition, variables);
        }
    }

    @Override
    protected void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        final org.objectweb.asm.Type type = variable.type.type;
        final Sort sort = variable.type.sort;

        final boolean initialize = expression == null;

        if (!initialize) {
            expression.write(settings, definition, adapter);
        }

        switch (sort) {
            case VOID:   throw new IllegalStateException(error("Illegal tree structure."));
            case BOOL:
            case BYTE:
            case SHORT:
            case CHAR:
            case INT:    if (initialize) adapter.push(0);    break;
            case LONG:   if (initialize) adapter.push(0L);   break;
            case FLOAT:  if (initialize) adapter.push(0.0F); break;
            case DOUBLE: if (initialize) adapter.push(0.0);  break;
            default:     if (initialize) adapter.visitInsn(Opcodes.ACONST_NULL);
        }

        adapter.visitVarInsn(type.getOpcode(Opcodes.ISTORE), variable.slot);
    }
}
