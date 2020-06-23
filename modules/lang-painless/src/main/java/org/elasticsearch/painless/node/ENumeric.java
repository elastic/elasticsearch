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
import org.elasticsearch.painless.symbol.SemanticScope;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ConstantNode;

import java.util.Objects;

/**
 * Represents a non-decimal numeric constant.
 */
public class ENumeric extends AExpression {

    private final String numeric;
    private final int radix;

    public ENumeric(int identifier, Location location, String numeric, int radix) {
        super(identifier, location);

        this.numeric = Objects.requireNonNull(numeric);
        this.radix = radix;
    }

    public String getNumeric() {
        return numeric;
    }

    public int getRadix() {
        return radix;
    }

    @Override
    Output analyze(ClassNode classNode, SemanticScope semanticScope, Input input) {
        return analyze(input, false);
    }

    Output analyze(Input input, boolean negate) {
        if (input.write) {
            throw createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to numeric constant [" + numeric + "]"));
        }

        if (input.read == false) {
            throw createError(new IllegalArgumentException("not a statement: numeric constant [" + numeric + "] not used"));
        }

        Output output = new Output();
        Object constant;

        String numeric = negate ? "-" + this.numeric : this.numeric;

        if (numeric.endsWith("d") || numeric.endsWith("D")) {
            if (radix != 10) {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }

            try {
                constant = Double.parseDouble(numeric.substring(0, numeric.length() - 1));
                output.actual = double.class;
            } catch (NumberFormatException exception) {
                throw createError(new IllegalArgumentException("Invalid double constant [" + numeric + "]."));
            }
        } else if (numeric.endsWith("f") || numeric.endsWith("F")) {
            if (radix != 10) {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }

            try {
                constant = Float.parseFloat(numeric.substring(0, numeric.length() - 1));
                output.actual = float.class;
            } catch (NumberFormatException exception) {
                throw createError(new IllegalArgumentException("Invalid float constant [" + numeric + "]."));
            }
        } else if (numeric.endsWith("l") || numeric.endsWith("L")) {
            try {
                constant = Long.parseLong(numeric.substring(0, numeric.length() - 1), radix);
                output.actual = long.class;
            } catch (NumberFormatException exception) {
                throw createError(new IllegalArgumentException("Invalid long constant [" + numeric + "]."));
            }
        } else {
            try {
                Class<?> sort = input.expected == null ? int.class : input.expected;
                int integer = Integer.parseInt(numeric, radix);

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
                    Long.parseLong(numeric, radix);
                    throw createError(new IllegalArgumentException("Invalid int constant [" + numeric + "]. If you want a long constant "
                            + "then change it to [" + numeric + "L]."));
                } catch (NumberFormatException longNoGood) {
                    // Ignored
                }
                throw createError(new IllegalArgumentException("Invalid int constant [" + numeric + "]."));
            }
        }

        ConstantNode constantNode = new ConstantNode();
        constantNode.setLocation(getLocation());
        constantNode.setExpressionType(output.actual);
        constantNode.setConstant(constant);

        output.expressionNode = constantNode;

        return output;
    }
}
