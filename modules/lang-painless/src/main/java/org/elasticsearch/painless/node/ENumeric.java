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
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents a non-decimal numeric constant.
 */
public class ENumeric extends AExpression {

    protected final String value;
    protected final int radix;

    public ENumeric(Location location, String value, int radix) {
        super(location);

        this.value = Objects.requireNonNull(value);
        this.radix = radix;
    }

    @Override
    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input) {
        return analyze(classNode, scriptRoot, scope, input, false);
    }

    Output analyze(ClassNode classNode, ScriptRoot scriptRoot, Scope scope, Input input, boolean negate) {
        if (input.write) {
            throw createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to numeric constant [" + value + "]"));
        }

        if (input.read == false) {
            throw createError(new IllegalArgumentException("not a statement: numeric constant [" + value + "] not used"));
        }

        Output output = new Output();
        Object constant;

        String value = negate ? "-" + this.value : this.value;

        if (value.endsWith("d") || value.endsWith("D")) {
            if (radix != 10) {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }

            try {
                constant = Double.parseDouble(value.substring(0, value.length() - 1));
                output.actual = double.class;
            } catch (NumberFormatException exception) {
                throw createError(new IllegalArgumentException("Invalid double constant [" + value + "]."));
            }
        } else if (value.endsWith("f") || value.endsWith("F")) {
            if (radix != 10) {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }

            try {
                constant = Float.parseFloat(value.substring(0, value.length() - 1));
                output.actual = float.class;
            } catch (NumberFormatException exception) {
                throw createError(new IllegalArgumentException("Invalid float constant [" + value + "]."));
            }
        } else if (value.endsWith("l") || value.endsWith("L")) {
            try {
                constant = Long.parseLong(value.substring(0, value.length() - 1), radix);
                output.actual = long.class;
            } catch (NumberFormatException exception) {
                throw createError(new IllegalArgumentException("Invalid long constant [" + value + "]."));
            }
        } else {
            try {
                Class<?> sort = input.expected == null ? int.class : input.expected;
                int integer = Integer.parseInt(value, radix);

                if (sort == byte.class && integer >= Byte.MIN_VALUE && integer <= Byte.MAX_VALUE) {
                    constant = (byte)integer;
                    output.actual = byte.class;
                } else if (sort == char.class && integer >= Character.MIN_VALUE && integer <= Character.MAX_VALUE) {
                    constant = (char)integer;
                    output.actual = char.class;
                } else if (sort == short.class && integer >= Short.MIN_VALUE && integer <= Short.MAX_VALUE) {
                    constant = (short)integer;
                    output.actual = short.class;
                } else {
                    constant = integer;
                    output.actual = int.class;
                }
            } catch (NumberFormatException exception) {
                try {
                    // Check if we can parse as a long. If so then hint that the user might prefer that.
                    Long.parseLong(value, radix);
                    throw createError(new IllegalArgumentException("Invalid int constant [" + value + "]. If you want a long constant "
                            + "then change it to [" + value + "L]."));
                } catch (NumberFormatException longNoGood) {
                    // Ignored
                }
                throw createError(new IllegalArgumentException("Invalid int constant [" + value + "]."));
            }
        }

        ConstantNode constantNode = new ConstantNode();
        constantNode.setLocation(location);
        constantNode.setExpressionType(output.actual);
        constantNode.setConstant(constant);

        output.expressionNode = constantNode;

        return output;
    }
}
