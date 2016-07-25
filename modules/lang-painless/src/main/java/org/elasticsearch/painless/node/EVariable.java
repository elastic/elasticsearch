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

import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.Variable;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.objectweb.asm.Opcodes;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a variable load/store.
 */
public final class EVariable extends AStoreable {

    private final String name;

    private Variable variable = null;

    public EVariable(Location location, String name) {
        super(location);

        this.name = Objects.requireNonNull(name);
    }

    @Override
    void extractVariables(Set<String> variables) {
        variables.add(name);
    }

    @Override
    void analyze(Locals locals) {
        variable = locals.getVariable(location, name);

        if (write && variable.readonly) {
            throw createError(new IllegalArgumentException("Variable [" + variable.name + "] is read-only."));
        }

        actual = variable.type;
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.visitVarInsn(actual.type.getOpcode(Opcodes.ILOAD), variable.getSlot());
    }

    @Override
    int accessElementCount() {
        return 0;
    }

    @Override
    boolean isDefOptimized() {
        return false;
    }

    @Override
    void updateActual(Type actual) {
        throw new IllegalArgumentException("Illegal tree structure.");
    }

    @Override
    void setup(MethodWriter writer, Globals globals) {
        // Do nothing.
    }

    @Override
    void load(MethodWriter writer, Globals globals) {
        writer.visitVarInsn(actual.type.getOpcode(Opcodes.ILOAD), variable.getSlot());
    }

    @Override
    void store(MethodWriter writer, Globals globals) {
        writer.visitVarInsn(actual.type.getOpcode(Opcodes.ISTORE), variable.getSlot());
    }
}
