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

import org.elasticsearch.painless.Definition.Cast;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.MethodWriter;

/**
 * Represents an implicit cast in most cases, though it will replace
 * explicit casts in the tree for simplicity.  (Internal only.)
 */
final class ECast extends AExpression {

    final String type;
    AExpression child;

    Cast cast = null;

    ECast(Location location, AExpression child, Cast cast) {
        super(location);

        this.type = null;
        this.child = child;

        this.cast = cast;
    }

    @Override
    void analyze(Locals locals) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    void write(MethodWriter writer) {
        child.write(writer);
        writer.writeDebugInfo(location);
        writer.writeCast(cast);
        writer.writeBranch(tru, fals);
    }
}
