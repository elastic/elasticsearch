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
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.DotNode;
import org.elasticsearch.painless.lookup.PainlessField;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;

/**
 * Represents a field load/store and defers to a child subnode.
 */
public final class PField extends AStoreable {

    private final boolean nullSafe;
    private final String value;

    private AStoreable sub = null;

    public PField(Location location, AExpression prefix, boolean nullSafe, String value) {
        super(location, prefix);

        this.nullSafe = nullSafe;
        this.value = Objects.requireNonNull(value);
    }

    @Override
    Output analyze(ScriptRoot scriptRoot, Scope scope, AExpression.Input input) {
        AStoreable.Input storeableInput = new AStoreable.Input();
        storeableInput.read = input.read;
        storeableInput.expected = input.expected;
        storeableInput.explicit = input.explicit;
        storeableInput.internal = input.internal;

        return analyze(scriptRoot, scope, storeableInput);
    }

    @Override
    Output analyze(ScriptRoot scriptRoot, Scope scope, AStoreable.Input input) {
        this.input = input;
        output = new Output();

        Output prefixOutput = prefix.analyze(scriptRoot, scope, new Input());
        prefix.input.expected = prefixOutput.actual;
        prefix.cast();

        if (prefixOutput.actual.isArray()) {
            sub = new PSubArrayLength(location, PainlessLookupUtility.typeToCanonicalTypeName(prefixOutput.actual), value);
        } else if (prefixOutput.actual == def.class) {
            sub = new PSubDefField(location, value);
        } else {
            PainlessField field = scriptRoot.getPainlessLookup().lookupPainlessField(prefixOutput.actual, prefix instanceof EStatic, value);

            if (field == null) {
                PainlessMethod getter;
                PainlessMethod setter;

                getter = scriptRoot.getPainlessLookup().lookupPainlessMethod(prefixOutput.actual, false,
                        "get" + Character.toUpperCase(value.charAt(0)) + value.substring(1), 0);

                if (getter == null) {
                    getter = scriptRoot.getPainlessLookup().lookupPainlessMethod(prefixOutput.actual, false,
                            "is" + Character.toUpperCase(value.charAt(0)) + value.substring(1), 0);
                }

                setter = scriptRoot.getPainlessLookup().lookupPainlessMethod(prefixOutput.actual, false,
                        "set" + Character.toUpperCase(value.charAt(0)) + value.substring(1), 0);

                if (getter != null || setter != null) {
                    sub = new PSubShortcut(
                            location, value, PainlessLookupUtility.typeToCanonicalTypeName(prefixOutput.actual), getter, setter);
                } else {
                    EConstant index = new EConstant(location, value);
                    index.analyze(scriptRoot, scope, new Input());

                    if (Map.class.isAssignableFrom(prefixOutput.actual)) {
                        sub = new PSubMapShortcut(location, prefixOutput.actual, index);
                    }

                    if (List.class.isAssignableFrom(prefixOutput.actual)) {
                        sub = new PSubListShortcut(location, prefixOutput.actual, index);
                    }
                }

                if (sub == null) {
                    throw createError(new IllegalArgumentException(
                            "field [" + typeToCanonicalTypeName(prefixOutput.actual) + ", " + value + "] not found"));
                }
            } else {
                sub = new PSubField(location, field);
            }
        }

        if (nullSafe) {
            sub = new PSubNullSafeField(location, sub);
        }

        Input subInput = new Input();
        subInput.write = input.write;
        subInput.read = input.read;
        subInput.expected = input.expected;
        subInput.explicit = input.explicit;
        Output subOutput = sub.analyze(scriptRoot, scope, subInput);
        output.actual = subOutput.actual;

        return output;
    }

    @Override
    DotNode write(ClassNode classNode) {
        DotNode dotNode = new DotNode();

        dotNode.setLeftNode(prefix.cast(prefix.write(classNode)));
        dotNode.setRightNode(sub.write(classNode));

        dotNode.setLocation(location);
        dotNode.setExpressionType(output.actual);

        return dotNode;
    }

    @Override
    boolean isDefOptimized() {
        return sub.isDefOptimized();
    }

    @Override
    void updateActual(Class<?> actual) {
        sub.updateActual(actual);
        this.output.actual = actual;
    }

    @Override
    public String toString() {
        if (nullSafe) {
            return singleLineToString("nullSafe", prefix, value);
        }
        return singleLineToString(prefix, value);
    }
}
