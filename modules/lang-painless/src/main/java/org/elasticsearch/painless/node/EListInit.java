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
import org.elasticsearch.painless.Definition.Method;
import org.elasticsearch.painless.Definition.MethodKey;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;

import java.util.List;
import java.util.Set;

/**
 * Represents a list initialization shortcut.
 */
public final class EListInit extends AExpression {
    private final List<AExpression> values;

    private Method constructor = null;
    private Method method = null;

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
    void analyze(Locals locals) {
        if (!read) {
            throw createError(new IllegalArgumentException("Must read from list initializer."));
        }

        actual = locals.getDefinition().ArrayListType;

        constructor = actual.struct.constructors.get(new MethodKey("<init>", 0));

        if (constructor == null) {
            throw createError(new IllegalStateException("Illegal tree structure."));
        }

        method = actual.struct.methods.get(new MethodKey("add", 1));

        if (method == null) {
            throw createError(new IllegalStateException("Illegal tree structure."));
        }

        for (int index = 0; index < values.size(); ++index) {
            AExpression expression = values.get(index);

            expression.expected = locals.getDefinition().DefType;
            expression.internal = true;
            expression.analyze(locals);
            values.set(index, expression.cast(locals));
        }
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        writer.newInstance(actual.type);
        writer.dup();
        writer.invokeConstructor(constructor.owner.type, constructor.method);

        for (AExpression value : values) {
            writer.dup();
            value.write(writer, globals);
            method.write(writer);
            writer.pop();
        }
    }

    @Override
    public String toString() {
        return singleLineToString(values);
    }
}
