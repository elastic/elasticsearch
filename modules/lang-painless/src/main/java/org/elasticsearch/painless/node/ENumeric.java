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
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Location;

import java.util.Objects;
import java.util.Set;

import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.MethodWriter;

/**
 * Represents a non-decimal numeric constant.
 */
public final class ENumeric extends AExpression {

    private final String value;
    private int radix;

    public ENumeric(Location location, String value, int radix) {
        super(location);

        this.value = Objects.requireNonNull(value);
        this.radix = radix;
    }

    @Override
    void extractVariables(Set<String> variables) {
        // Do nothing.
    }

    @Override
    void analyze(Locals locals) {
        if (!read) {
            throw createError(new IllegalArgumentException("Must read from constant [" + value + "]."));
        }

        if (value.endsWith("d") || value.endsWith("D")) {
            if (radix != 10) {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }

            try {
                constant = Double.parseDouble(value.substring(0, value.length() - 1));
                actual = locals.getDefinition().doubleType;
            } catch (NumberFormatException exception) {
                throw createError(new IllegalArgumentException("Invalid double constant [" + value + "]."));
            }
        } else if (value.endsWith("f") || value.endsWith("F")) {
            if (radix != 10) {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }

            try {
                constant = Float.parseFloat(value.substring(0, value.length() - 1));
                actual = locals.getDefinition().floatType;
            } catch (NumberFormatException exception) {
                throw createError(new IllegalArgumentException("Invalid float constant [" + value + "]."));
            }
        } else if (value.endsWith("l") || value.endsWith("L")) {
            try {
                constant = Long.parseLong(value.substring(0, value.length() - 1), radix);
                actual = locals.getDefinition().longType;
            } catch (NumberFormatException exception) {
                throw createError(new IllegalArgumentException("Invalid long constant [" + value + "]."));
            }
        } else {
            try {
                Class<?> sort = expected == null ? int.class : expected.clazz;
                int integer = Integer.parseInt(value, radix);

                if (sort == byte.class && integer >= Byte.MIN_VALUE && integer <= Byte.MAX_VALUE) {
                    constant = (byte)integer;
                    actual = locals.getDefinition().byteType;
                } else if (sort == char.class && integer >= Character.MIN_VALUE && integer <= Character.MAX_VALUE) {
                    constant = (char)integer;
                    actual = locals.getDefinition().charType;
                } else if (sort == short.class && integer >= Short.MIN_VALUE && integer <= Short.MAX_VALUE) {
                    constant = (short)integer;
                    actual = locals.getDefinition().shortType;
                } else {
                    constant = integer;
                    actual = locals.getDefinition().intType;
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
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    public String toString() {
        if (radix != 10) {
            return singleLineToString(value, radix);
        }
        return singleLineToString(value);
    }
}
