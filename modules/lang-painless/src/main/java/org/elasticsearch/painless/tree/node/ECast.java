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

package org.elasticsearch.painless.tree.node;

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Cast;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.tree.analyzer.Caster;
import org.elasticsearch.painless.tree.analyzer.Variables;
import org.objectweb.asm.commons.GeneratorAdapter;

public class ECast extends Expression {
    protected final String type;
    protected Expression child;

    protected Cast cast = null;

    public ECast(final String location, final String type, final Expression child) {
        super(location);

        this.type = type;
        this.child = child;
    }

    protected ECast(final String location, final Expression child, final Cast cast) {
        super(location);

        this.type = null;
        this.child = child;

        this.cast = cast;
    }

    @Override
    protected void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        try {
            actual = definition.getType(this.type);
        } catch (final IllegalArgumentException exception) {
            throw new IllegalArgumentException(error("Not a type [" + this.type + "]."));
        }

        child.expected = actual;
        child.analyze(settings, definition, variables);
        cast = Caster.getLegalCast(definition, location, child.actual, child.expected, true);

        if (cast == null) {
            constant = child.constant;
        } else if (child.constant != null && child.expected.sort.constant) {
            constant = Caster.constCast(location, constant, cast);
        }

        typesafe = child.typesafe && actual.sort != Sort.DEF;
    }

    @Override
    protected Expression cast(final Definition definition) {
        if (cast == null && constant == null) {
            child.expected = expected;

            return child.cast(definition);
        } else {
            return super.cast(definition);
        }
    }

    @Override
    protected void write(final GeneratorAdapter adapter) {

    }
}
