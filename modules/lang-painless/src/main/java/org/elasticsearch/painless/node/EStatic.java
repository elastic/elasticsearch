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
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.StaticNode;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;

/**
 * Represents a static type target.
 */
public final class EStatic extends AExpression {

    private final String type;

    public EStatic(Location location, String type) {
        super(location);

        this.type = Objects.requireNonNull(type);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Scope scope) {
        actual = scriptRoot.getPainlessLookup().canonicalTypeNameToType(type);

        if (actual == null) {
            throw createError(new IllegalArgumentException("Not a type [" + type + "]."));
        }
    }

    @Override
    StaticNode write(ClassNode classNode) {
        StaticNode staticNode = new StaticNode();

        staticNode.setLocation(location);
        staticNode.setExpressionType(actual);

        return staticNode;
    }

    @Override
    public String toString() {
        return singleLineToString(type);
    }
}
