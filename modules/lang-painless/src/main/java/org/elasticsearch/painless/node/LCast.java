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
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Definition.Cast;
import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.MethodWriter;

/**
 * Represents a cast made in a variable/method chain.
 */
public final class LCast extends ALink {

    final String type;

    Cast cast = null;

    public LCast(Location location, String type) {
        super(location, -1);

        this.type = type;
    }

    @Override
    ALink analyze(Locals locals) {
        if (before == null) {
            throw createError(new IllegalStateException("Illegal cast without a target."));
        } else if (store) {
            throw createError(new IllegalArgumentException("Cannot assign a value to a cast."));
        }

        try {
            after = Definition.getType(type);
        } catch (IllegalArgumentException exception) {
            throw createError(new IllegalArgumentException("Not a type [" + type + "]."));
        }

        cast = AnalyzerCaster.getLegalCast(location, before, after, true, false);

        return cast != null ? this : null;
    }

    @Override
    void write(MethodWriter writer) {
        writer.writeDebugInfo(location);
        writer.writeCast(cast);
    }

    @Override
    void load(MethodWriter writer) {
        // Do nothing.
    }

    @Override
    void store(MethodWriter writer) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }
}
