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

import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Variables;
import org.elasticsearch.painless.Variables.Variable;
import org.objectweb.asm.Opcodes;
import org.elasticsearch.painless.MethodWriter;

/**
 * Represents a variable load/store.
 */
public final class LVariable extends ALink {

    final String name;

    int slot;

    public LVariable(int line, String location, String name) {
        super(line, location, 0);

        this.name = name;
    }

    @Override
    ALink analyze(Variables variables) {
        if (before != null) {
            throw new IllegalStateException(error("Illegal tree structure."));
        }

        Type type = null;

        try {
            type = Definition.getType(name);
        } catch (final IllegalArgumentException exception) {
            // Do nothing.
        }

        if (type != null) {
            statik = true;
            after = type;
        } else {
            final Variable variable = variables.getVariable(location, name);

            if (store && variable.readonly) {
                throw new IllegalArgumentException(error("Variable [" + variable.name + "] is read-only."));
            }

            slot = variable.slot;
            after = variable.type;
        }

        return this;
    }

    @Override
    void write(MethodWriter adapter) {
        // Do nothing.
    }

    @Override
    void load(MethodWriter adapter) {
        adapter.visitVarInsn(after.type.getOpcode(Opcodes.ILOAD), slot);
    }

    @Override
    void store(MethodWriter adapter) {
        adapter.visitVarInsn(after.type.getOpcode(Opcodes.ISTORE), slot);
    }
}
