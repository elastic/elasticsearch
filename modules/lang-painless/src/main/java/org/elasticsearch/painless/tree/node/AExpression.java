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
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.tree.analyzer.Caster;
import org.elasticsearch.painless.tree.analyzer.Variables;
import org.objectweb.asm.Label;
import org.objectweb.asm.commons.GeneratorAdapter;

public abstract class AExpression extends BNode {
    protected boolean read = true;
    protected boolean statement = false;

    protected Type expected = null;
    protected Type actual = null;
    protected boolean typesafe = true;
    protected boolean strings = false;

    protected Object constant = null;
    protected boolean isNull = false;

    protected Label tru = null;
    protected Label fals = null;

    public AExpression(final String location) {
        super(location);
    }

    protected abstract void analyze(final CompilerSettings settings, final Definition definition, final Variables variables);
    protected abstract void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter);

    protected AExpression cast(final Definition definition) {
        final AExpression rtn;

        final Cast cast = Caster.getLegalCast(definition, location, actual, expected, !typesafe);

        if (constant != null) {
            if (actual.sort.constant && expected.sort.constant) {
                constant = Caster.constCast(location, constant, cast);
            }

            final AExpression econstant = this instanceof EConstant ? this : new EConstant(location, constant);

            rtn = cast != null ? new ECast(location, econstant, cast) : econstant;
        } else if (cast != null) {
            rtn = new ECast(location, this, cast);
        } else {
            rtn = this;
        }

        rtn.actual = actual;
        rtn.typesafe = typesafe;
        rtn.isNull = isNull;
        rtn.strings = strings;

        return rtn;
    }
}
