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

import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.ScriptRoot;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Represents an array load/store and defers to a child subnode.
 */
public final class PBrace extends AStoreable {

    private AExpression index;

    private AStoreable sub = null;

    public PBrace(Location location, AExpression prefix, AExpression index) {
        super(location, prefix);

        this.index = Objects.requireNonNull(index);
    }

    @Override
    void extractVariables(Set<String> variables) {
        prefix.extractVariables(variables);
        index.extractVariables(variables);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Locals locals) {
        prefix.analyze(scriptRoot, locals);
        prefix.expected = prefix.actual;
        prefix = prefix.cast(scriptRoot, locals);

        if (prefix.actual.isArray()) {
            sub = new PSubBrace(location, prefix.actual, index);
        } else if (prefix.actual == def.class) {
            sub = new PSubDefArray(location, index);
        } else if (Map.class.isAssignableFrom(prefix.actual)) {
            sub = new PSubMapShortcut(location, prefix.actual, index);
        } else if (List.class.isAssignableFrom(prefix.actual)) {
            sub = new PSubListShortcut(location, prefix.actual, index);
        } else {
            throw createError(new IllegalArgumentException("Illegal array access on type " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(prefix.actual) + "]."));
        }

        sub.write = write;
        sub.read = read;
        sub.expected = expected;
        sub.explicit = explicit;
        sub.analyze(scriptRoot, locals);
        actual = sub.actual;
    }

    @Override
    void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        prefix.write(classWriter, methodWriter, globals);
        sub.write(classWriter, methodWriter, globals);
    }

    @Override
    boolean isDefOptimized() {
        return sub.isDefOptimized();
    }

    @Override
    void updateActual(Class<?> actual) {
        sub.updateActual(actual);
        this.actual = actual;
    }

    @Override
    int accessElementCount() {
        return sub.accessElementCount();
    }

    @Override
    void setup(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        prefix.write(classWriter, methodWriter, globals);
        sub.setup(classWriter, methodWriter, globals);
    }

    @Override
    void load(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        sub.load(classWriter, methodWriter, globals);
    }

    @Override
    void store(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        sub.store(classWriter, methodWriter, globals);
    }

    @Override
    public String toString() {
        return singleLineToString(prefix, index);
    }
}
