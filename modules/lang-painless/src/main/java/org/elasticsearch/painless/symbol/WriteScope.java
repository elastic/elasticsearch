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

package org.elasticsearch.painless.symbol;

import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.objectweb.asm.Type;

import java.util.HashMap;
import java.util.Map;

public class WriteScope {

    public static class Variable {

        protected final Class<?> type;
        protected final Type asmType;
        protected final String name;
        protected final int slot;

        public Variable(Class<?> type, String name, int slot) {
            this.type = type;
            this.asmType = MethodWriter.getType(type);
            this.name = name;
            this.slot = slot;
        }

        public Class<?> getType() {
            return type;
        }

        public String getCanonicalTypeName() {
            return PainlessLookupUtility.typeToCanonicalTypeName(type);
        }

        public Type getAsmType() {
            return asmType;
        }

        public String getName() {
            return name;
        }

        public int getSlot() {
            return slot;
        }
    }

    protected final WriteScope parent;
    protected final Map<String, Variable> variables = new HashMap<>();
    protected int nextSlot;

    public WriteScope() {
        this.parent = null;
        this.nextSlot = 0;
    }

    protected WriteScope(WriteScope parent, int nextSlot) {
        this.parent = parent;
        this.nextSlot = nextSlot;
    }

    public WriteScope newScope() {
        return new WriteScope(this, nextSlot);
    }

    public Variable defineVariable(Class<?> type, String name) {
        Variable variable = new Variable(type, name, nextSlot);
        nextSlot += variable.getAsmType().getSize();
        variables.put(name, variable);

        return variable;
    }

    /**
     * Prepends the character '#' to the variable name. The '#' is
     * reserved and ensures that these internal variables aren't
     * accessed by a normal consumer.
     */
    public Variable defineInternalVariable(Class<?> type, String name) {
        return defineVariable(type, "#" + name);
    }

    public Variable getVariable(String name) {
        Variable variable = variables.get(name);

        if (variable == null && parent != null) {
            variable = parent.getVariable(name);
        }

        return variable;
    }

    /**
     * Prepends the character '#' to the variable name. The '#' is
     * reserved and ensures that these internal variables aren't
     * accessed by a normal consumer.
     */
    public Variable getInternalVariable(String name) {
        return getVariable("#" + name);
    }
}
