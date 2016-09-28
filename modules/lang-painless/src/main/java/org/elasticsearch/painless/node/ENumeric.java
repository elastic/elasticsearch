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
import org.elasticsearch.painless.Definition.Sort;

import java.util.Objects;
import java.util.Set;

import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.MethodWriter;

/**
 * Respresents a non-decimal numeric constant.
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
                actual = Definition.DOUBLE_TYPE;
            } catch (NumberFormatException exception) {
                throw createError(new IllegalArgumentException("Invalid double constant [" + value + "]."));
            }
        } else if (value.endsWith("f") || value.endsWith("F")) {
            if (radix != 10) {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }

            try {
                constant = Float.parseFloat(value.substring(0, value.length() - 1));
                actual = Definition.FLOAT_TYPE;
            } catch (NumberFormatException exception) {
                throw createError(new IllegalArgumentException("Invalid float constant [" + value + "]."));
            }
        } else if (value.endsWith("l") || value.endsWith("L")) {
            try {
                constant = Long.parseLong(value.substring(0, value.length() - 1), radix);
                actual = Definition.LONG_TYPE;
            } catch (NumberFormatException exception) {
                throw createError(new IllegalArgumentException("Invalid long constant [" + value + "]."));
            }
        } else {
            try {
                Sort sort = expected == null ? Sort.INT : expected.sort;
                int integer = Integer.parseInt(value, radix);

                if (sort == Sort.BYTE && integer >= Byte.MIN_VALUE && integer <= Byte.MAX_VALUE) {
                    constant = (byte)integer;
                    actual = Definition.BYTE_TYPE;
                } else if (sort == Sort.CHAR && integer >= Character.MIN_VALUE && integer <= Character.MAX_VALUE) {
                    constant = (char)integer;
                    actual = Definition.CHAR_TYPE;
                } else if (sort == Sort.SHORT && integer >= Short.MIN_VALUE && integer <= Short.MAX_VALUE) {
                    constant = (short)integer;
                    actual = Definition.SHORT_TYPE;
                } else {
                    constant = integer;
                    actual = Definition.INT_TYPE;
                }
            } catch (NumberFormatException exception) {
                throw createError(new IllegalArgumentException("Invalid int constant [" + value + "]."));
            }
        }
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }
}
