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
import org.elasticsearch.painless.tree.analyzer.Variables;
import org.objectweb.asm.commons.GeneratorAdapter;

import static org.elasticsearch.painless.tree.writer.Constants.CLASS_TYPE;
import static org.elasticsearch.painless.tree.writer.Constants.DEFINITION_TYPE;
import static org.elasticsearch.painless.tree.writer.Constants.DEF_ARRAY_LOAD;
import static org.elasticsearch.painless.tree.writer.Constants.DEF_ARRAY_STORE;

public class LDefArray extends ALink {
    protected AExpression index;

    protected LDefArray(final String location, final AExpression index) {
        super(location, 0);

        this.index = index;
    }


    @Override
    protected ALink analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        index.expected = definition.objectType;
        index.analyze(settings, definition, variables);
        index = index.cast(settings, definition, variables);

        after = definition.defType;

        return this;
    }

    @Override
    protected void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        index.write(settings, definition, adapter);
    }

    @Override
    protected void load(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        adapter.loadThis();
        adapter.getField(CLASS_TYPE, "definition", DEFINITION_TYPE);
        adapter.push(index.typesafe);
        adapter.invokeStatic(definition.defobjType.type, DEF_ARRAY_LOAD);
    }

    @Override
    protected void store(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        adapter.loadThis();
        adapter.getField(CLASS_TYPE, "definition", DEFINITION_TYPE);
        adapter.push(index.typesafe);
        adapter.push(typesafe);
        adapter.invokeStatic(definition.defobjType.type, DEF_ARRAY_STORE);
    }
}
