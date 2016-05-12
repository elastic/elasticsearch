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
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Variables;
import org.objectweb.asm.commons.GeneratorAdapter;

/**
 * Respresents a non-decimal numeric constant.
 */
public final class ENumeric extends AExpression {

    final String value;
    int radix;

    public ENumeric(final String location, final String value, final int radix) {
        super(location);

        this.value = value;
        this.radix = radix;
    }

    @Override
    void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        if (value.endsWith("d") || value.endsWith("D")) {
            if (radix != 10) {
                throw new IllegalStateException(error("Invalid tree structure."));
            }

            try {
                constant = Double.parseDouble(value.substring(0, value.length() - 1));
                actual = definition.doubleType;
            } catch (final NumberFormatException exception) {
                throw new IllegalArgumentException(error("Invalid double constant [" + value + "]."));
            }
        } else if (value.endsWith("f") || value.endsWith("F")) {
            if (radix != 10) {
                throw new IllegalStateException(error("Invalid tree structure."));
            }

            try {
                constant = Float.parseFloat(value.substring(0, value.length() - 1));
                actual = definition.floatType;
            } catch (final NumberFormatException exception) {
                throw new IllegalArgumentException(error("Invalid float constant [" + value + "]."));
            }
        } else if (value.endsWith("l") || value.endsWith("L")) {
            try {
                constant = Long.parseLong(value.substring(0, value.length() - 1), radix);
                actual = definition.longType;
            } catch (final NumberFormatException exception) {
                throw new IllegalArgumentException(error("Invalid long constant [" + value + "]."));
            }
        } else {
            try {
                final Sort sort = expected == null ? Sort.INT : expected.sort;
                final int integer = Integer.parseInt(value, radix);

                if (sort == Sort.BYTE && integer >= Byte.MIN_VALUE && integer <= Byte.MAX_VALUE) {
                    constant = (byte)integer;
                    actual = definition.byteType;
                } else if (sort == Sort.CHAR && integer >= Character.MIN_VALUE && integer <= Character.MAX_VALUE) {
                    constant = (char)integer;
                    actual = definition.charType;
                } else if (sort == Sort.SHORT && integer >= Short.MIN_VALUE && integer <= Short.MAX_VALUE) {
                    constant = (short)integer;
                    actual = definition.shortType;
                } else {
                    constant = integer;
                    actual = definition.intType;
                }
            } catch (final NumberFormatException exception) {
                throw new IllegalArgumentException(error("Invalid int constant [" + value + "]."));
            }
        }
    }

    @Override
    void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        throw new IllegalArgumentException(error("Illegal tree structure."));
    }
}
