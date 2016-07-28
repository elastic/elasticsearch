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
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.MethodWriter;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a decimal constant.
 */
public final class EDecimal extends AExpression {

    private final String value;

    public EDecimal(Location location, String value) {
        super(location);

        this.value = Objects.requireNonNull(value);
    }

    @Override
    void extractVariables(Set<String> variables) {}

    @Override
    void analyze(Locals locals) {
        if (!read) {
            throw createError(new IllegalArgumentException("Must read from constant [" + value + "]."));
        }

        if (value.endsWith("f") || value.endsWith("F")) {
            try {
                constant = Float.parseFloat(value.substring(0, value.length() - 1));
                actual = Definition.FLOAT_TYPE;
            } catch (NumberFormatException exception) {
                throw createError(new IllegalArgumentException("Invalid float constant [" + value + "]."));
            }
        } else {
            try {
                constant = Double.parseDouble(value);
                actual = Definition.DOUBLE_TYPE;
            } catch (NumberFormatException exception) {
                throw createError(new IllegalArgumentException("Invalid double constant [" + value + "]."));
            }
        }
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }
}
