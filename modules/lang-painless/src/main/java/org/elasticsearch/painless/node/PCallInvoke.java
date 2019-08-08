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
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

/**
 * Represents a method call and defers to a child subnode.
 */
public final class PCallInvoke extends AExpression {

    private final String name;
    private final boolean nullSafe;
    private final List<AExpression> arguments;

    private AExpression sub = null;

    public PCallInvoke(Location location, AExpression prefix, String name, boolean nullSafe, List<AExpression> arguments) {
        super(location, prefix);

        this.name = Objects.requireNonNull(name);
        this.nullSafe = nullSafe;
        this.arguments = Objects.requireNonNull(arguments);
    }

    @Override
    void storeSettings(CompilerSettings settings) {
        prefix.storeSettings(settings);

        for (AExpression argument : arguments) {
            argument.storeSettings(settings);
        }
    }

    @Override
    void extractVariables(Set<String> variables) {
        prefix.extractVariables(variables);

        for (AExpression argument : arguments) {
            argument.extractVariables(variables);
        }
    }

    @Override
    void analyze(Locals locals) {
        prefix.analyze(locals);
        prefix.expected = prefix.actual;
        prefix = prefix.cast(locals);

        if (prefix.actual == def.class) {
            sub = new PSubDefCall(location, name, arguments);
        } else {
            PainlessMethod method =
                    locals.getPainlessLookup().lookupPainlessMethod(prefix.actual, prefix instanceof EStatic, name, arguments.size());

            if (method == null) {
                throw createError(new IllegalArgumentException(
                        "method [" + typeToCanonicalTypeName(prefix.actual) + ", " + name + "/" + arguments.size() + "] not found"));
            }

            sub = new PSubCallInvoke(location, method, prefix.actual, arguments);
        }

        if (nullSafe) {
            sub = new PSubNullSafeCallInvoke(location, sub);
        }

        sub.expected = expected;
        sub.explicit = explicit;
        sub.analyze(locals);
        actual = sub.actual;

        statement = true;
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        prefix.write(writer, globals);
        sub.write(writer, globals);
    }

    @Override
    public String toString() {
        return singleLineToStringWithOptionalArgs(arguments, prefix, name);
    }
}
