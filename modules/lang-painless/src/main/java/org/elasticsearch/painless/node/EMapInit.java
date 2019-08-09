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

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

import java.util.HashMap;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

/**
 * Represents a map initialization shortcut.
 */
public final class EMapInit extends AExpression {
    private final List<AExpression> keys;
    private final List<AExpression> values;

    private PainlessConstructor constructor = null;
    private PainlessMethod method = null;

    public EMapInit(Location location, List<AExpression> keys, List<AExpression> values) {
        super(location);

        this.keys = keys;
        this.values = values;
    }

    @Override
    void storeSettings(CompilerSettings settings) {
        for (AExpression key : keys) {
            key.storeSettings(settings);
        }

        for (AExpression value : values) {
            value.storeSettings(settings);
        }
    }

    @Override
    void extractVariables(Set<String> variables) {
        for (AExpression key : keys) {
            key.extractVariables(variables);
        }

        for (AExpression value : values) {
            value.extractVariables(variables);
        }
    }

    @Override
    void analyze(Locals locals) {
        if (!read) {
            throw createError(new IllegalArgumentException("Must read from map initializer."));
        }

        actual = HashMap.class;

        constructor = locals.getPainlessLookup().lookupPainlessConstructor(actual, 0);

        if (constructor == null) {
            throw createError(new IllegalArgumentException(
                    "constructor [" + typeToCanonicalTypeName(actual) + ", <init>/0] not found"));
        }

        method = locals.getPainlessLookup().lookupPainlessMethod(actual, false, "put", 2);

        if (method == null) {
            throw createError(new IllegalArgumentException("method [" + typeToCanonicalTypeName(actual) + ", put/2] not found"));
        }

        if (keys.size() != values.size()) {
            throw createError(new IllegalStateException("Illegal tree structure."));
        }

        for (int index = 0; index < keys.size(); ++index) {
            AExpression expression = keys.get(index);

            expression.expected = def.class;
            expression.internal = true;
            expression.analyze(locals);
            keys.set(index, expression.cast(locals));
        }

        for (int index = 0; index < values.size(); ++index) {
            AExpression expression = values.get(index);

            expression.expected = def.class;
            expression.internal = true;
            expression.analyze(locals);
            values.set(index, expression.cast(locals));
        }
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        writer.newInstance(MethodWriter.getType(actual));
        writer.dup();
        writer.invokeConstructor(
                    Type.getType(constructor.javaConstructor.getDeclaringClass()), Method.getMethod(constructor.javaConstructor));

        for (int index = 0; index < keys.size(); ++index) {
            AExpression key = keys.get(index);
            AExpression value = values.get(index);

            writer.dup();
            key.write(writer, globals);
            value.write(writer, globals);
            writer.invokeMethodCall(method);
            writer.pop();
        }
    }

    @Override
    public String toString() {
        return singleLineToString(pairwiseToString(keys, values));
    }
}
