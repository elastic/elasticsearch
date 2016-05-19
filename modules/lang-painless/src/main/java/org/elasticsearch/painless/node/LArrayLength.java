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
import org.elasticsearch.painless.Variables;
import org.elasticsearch.painless.MethodWriter;

/**
 * Represents an array length field load.
 */
public final class LArrayLength extends ALink {

    final String value;

    LArrayLength(final int line, final String location, final String value) {
        super(line, location, -1);

        this.value = value;
    }

    @Override
    ALink analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        if ("length".equals(value)) {
            if (!load) {
                throw new IllegalArgumentException(error("Must read array field [length]."));
            } else if (store) {
                throw new IllegalArgumentException(error("Cannot write to read-only array field [length]."));
            }

            after = definition.intType;
        } else {
            throw new IllegalArgumentException(error("Illegal field access [" + value + "]."));
        }

        return this;
    }

    @Override
    void write(final CompilerSettings settings, final Definition definition, final MethodWriter adapter) {
        // Do nothing.
    }

    @Override
    void load(final CompilerSettings settings, final Definition definition, final MethodWriter adapter) {
        adapter.arrayLength();
    }

    @Override
    void store(final CompilerSettings settings, final Definition definition, final MethodWriter adapter) {
        throw new IllegalStateException(error("Illegal tree structure."));
    }
}
