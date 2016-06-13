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

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Locals.Variable;
import org.objectweb.asm.Opcodes;

/**
 * Represents a variable load/store.
 */
public final class LVariable extends ALink {

    final String name;

    int slot;

    public LVariable(Location location, String name) {
        super(location, 0);

        this.name = name;
    }

    @Override
    ALink analyze(Locals locals) {
        if (before != null) {
            throw createError(new IllegalArgumentException("Illegal variable [" + name + "] access with target already defined."));
        }

        Variable variable = locals.getVariable(location, name);

        if (store && variable.readonly) {
            throw createError(new IllegalArgumentException("Variable [" + variable.name + "] is read-only."));
        }

        slot = variable.slot;
        after = variable.type;

        return this;
    }

    @Override
    void write(MethodWriter writer) {
        // Do nothing.
    }

    @Override
    void load(MethodWriter writer) {
        writer.visitVarInsn(after.type.getOpcode(Opcodes.ILOAD), slot);
    }

    @Override
    void store(MethodWriter writer) {
        writer.visitVarInsn(after.type.getOpcode(Opcodes.ISTORE), slot);
    }
}
