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
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.CastNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents a cast that is inserted into the tree replacing other casts.  (Internal only.)  Casts are inserted during semantic checking.
 */
final class ECast extends AExpression {

    private AExpression child;
    private final PainlessCast cast;

    ECast(Location location, AExpression child, PainlessCast cast) {
        super(location);

        this.child = Objects.requireNonNull(child);
        this.cast = Objects.requireNonNull(cast);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    CastNode write(ClassNode classNode) {
        CastNode castNode = new CastNode();

        castNode.setChildNode(child.write(classNode));

        castNode.setLocation(location);
        castNode.setExpressionType(actual);
        castNode.setCast(cast);

        return castNode;
    }

    @Override
    public String toString() {
        return singleLineToString(PainlessLookupUtility.typeToCanonicalTypeName(cast.targetType), child);
    }
}
