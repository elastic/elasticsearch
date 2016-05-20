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
import org.elasticsearch.painless.Definition.Cast;
import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.Variables;
import org.elasticsearch.painless.MethodWriter;

/**
 * Represents a cast made in a variable/method chain.
 */
public final class LCast extends ALink {

    final String type;

    Cast cast = null;

    public LCast(final int line, final String location, final String type) {
        super(line, location, -1);

        this.type = type;
    }

    @Override
    ALink analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        if (before == null) {
            throw new IllegalStateException(error("Illegal tree structure."));
        } else if (store) {
            throw new IllegalArgumentException(error("Cannot assign a value to a cast."));
        }

        try {
            after = Definition.getType(type);
        } catch (final IllegalArgumentException exception) {
            throw new IllegalArgumentException(error("Not a type [" + type + "]."));
        }

        cast = AnalyzerCaster.getLegalCast(definition, location, before, after, true);

        return cast != null ? this : null;
    }

    @Override
    void write(final CompilerSettings settings, final Definition definition, final MethodWriter adapter) {
        adapter.writeCast(cast);
    }

    @Override
    void load(final CompilerSettings settings, final Definition definition, final MethodWriter adapter) {
        // Do nothing.
    }

    @Override
    void store(final CompilerSettings settings, final Definition definition, final MethodWriter adapter) {
        throw new IllegalStateException(error("Illegal tree structure."));
    }
}
