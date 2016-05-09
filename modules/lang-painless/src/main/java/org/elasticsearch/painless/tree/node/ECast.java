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

import org.elasticsearch.painless.compiler.CompilerSettings;
import org.elasticsearch.painless.compiler.Definition;
import org.elasticsearch.painless.compiler.Definition.Cast;
import org.elasticsearch.painless.tree.utility.Variables;
import org.elasticsearch.painless.tree.writer.Utility;
import org.objectweb.asm.commons.GeneratorAdapter;

public class ECast extends AExpression {
    protected final String type;
    protected AExpression child;

    protected Cast cast = null;

    protected ECast(final String location, final AExpression child, final Cast cast) {
        super(location);

        this.type = null;
        this.child = child;

        this.cast = cast;
    }

    @Override
    protected void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        throw new IllegalStateException(error("Illegal tree structure."));
    }

    @Override
    protected void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        child.write(settings, definition, adapter);
        Utility.writeCast(adapter, cast);
        Utility.writeBranch(adapter, tru, fals);
    }
}
