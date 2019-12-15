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
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.objectweb.asm.Type;
import org.objectweb.asm.commons.Method;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

/**
 * Represents a list initialization shortcut.
 */
public final class EListInit extends AExpression {
    private final List<AExpression> values;

    private PainlessConstructor constructor = null;
    private PainlessMethod method = null;

    public EListInit(Location location, List<AExpression> values) {
        super(location);

        this.values = values;
    }

    @Override
    void extractVariables(Set<String> variables) {
        for (AExpression value : values) {
            value.extractVariables(variables);
        }
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Locals locals) {
        if (!read) {
            throw createError(new IllegalArgumentException("Must read from list initializer."));
        }

        actual = ArrayList.class;

        constructor = scriptRoot.getPainlessLookup().lookupPainlessConstructor(actual, 0);

        if (constructor == null) {
            throw createError(new IllegalArgumentException(
                    "constructor [" + typeToCanonicalTypeName(actual) + ", <init>/0] not found"));
        }

        method = scriptRoot.getPainlessLookup().lookupPainlessMethod(actual, false, "add", 1);

        if (method == null) {
            throw createError(new IllegalArgumentException("method [" + typeToCanonicalTypeName(actual) + ", add/1] not found"));
        }

        for (int index = 0; index < values.size(); ++index) {
            AExpression expression = values.get(index);

            expression.expected = def.class;
            expression.internal = true;
            expression.analyze(scriptRoot, locals);
            values.set(index, expression.cast(scriptRoot, locals));
        }
    }

    @Override
    void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        methodWriter.newInstance(MethodWriter.getType(actual));
        methodWriter.dup();
        methodWriter.invokeConstructor(
                    Type.getType(constructor.javaConstructor.getDeclaringClass()), Method.getMethod(constructor.javaConstructor));

        for (AExpression value : values) {
            methodWriter.dup();
            value.write(classWriter, methodWriter, globals);
            methodWriter.invokeMethodCall(method);
            methodWriter.pop();
        }
    }

    @Override
    public String toString() {
        return singleLineToString(values);
    }
}
