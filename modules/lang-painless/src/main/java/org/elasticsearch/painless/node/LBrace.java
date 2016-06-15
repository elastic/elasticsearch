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
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.MethodWriter;

import java.util.List;
import java.util.Map;

/**
 * Represents an array load/store or defers to possible shortcuts.
 */
public final class LBrace extends ALink {

    AExpression index;

    public LBrace(Location location, AExpression index) {
        super(location, 2);

        this.index = index;
    }

    @Override
    ALink analyze(Locals locals) {
        if (before == null) {
            throw createError(new IllegalArgumentException("Illegal array access made without target."));
        }

        Sort sort = before.sort;

        if (sort == Sort.ARRAY) {
            index.expected = Definition.INT_TYPE;
            index.analyze(locals);
            index = index.cast(locals);

            after = Definition.getType(before.struct, before.dimensions - 1);

            return this;
        } else if (sort == Sort.DEF) {
            return new LDefArray(location, index).copy(this).analyze(locals);
        } else if (Map.class.isAssignableFrom(before.clazz)) {
            return new LMapShortcut(location, index).copy(this).analyze(locals);
        } else if (List.class.isAssignableFrom(before.clazz)) {
            return new LListShortcut(location, index).copy(this).analyze(locals);
        }

        throw createError(new IllegalArgumentException("Illegal array access on type [" + before.name + "]."));
    }

    @Override
    void write(MethodWriter writer) {
        index.write(writer);
    }

    @Override
    void load(MethodWriter writer) {
        writer.writeDebugInfo(location);
        writer.arrayLoad(after.type);
    }

    @Override
    void store(MethodWriter writer) {
        writer.writeDebugInfo(location);
        writer.arrayStore(after.type);
    }
}
