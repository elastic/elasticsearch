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
import org.elasticsearch.painless.tree.writer.Shared;
import org.objectweb.asm.commons.GeneratorAdapter;

import static org.elasticsearch.painless.tree.writer.Constants.CLASS_TYPE;
import static org.elasticsearch.painless.tree.writer.Constants.DEFINITION_TYPE;
import static org.elasticsearch.painless.tree.writer.Constants.DEF_ARRAY_LOAD;
import static org.elasticsearch.painless.tree.writer.Constants.DEF_ARRAY_STORE;

public class LDefArray extends ALink {
    protected AExpression index;

    public LDefArray(final String location, final AExpression index) {
        super(location);

        this.index = index;
    }


    @Override
    protected ALink analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        index.expected = definition.objectType;
        index.analyze(settings, definition, variables);
        index = index.cast(definition);

        after = definition.defType;

        return this;
    }

    @Override
    protected void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        if (strings) {
            Shared.writeNewStrings(adapter);
        }

        index.write(settings, definition, adapter);

        if (store) {
            if (cat) {
                adapter.dup2X1();
                adapter.arrayLoad(before.type);
                Shared.writeAppendStrings(adapter, after.sort);
                expression.write(settings, definition, adapter);
                Shared.writeToStrings(adapter);
                Shared.writeCast(adapter, back);

                if (load) {
                    Shared.writeDup(adapter, after.sort.size, false, true);
                }

                adapter.arrayStore(before.type);
            } else if (operation != null) {
                adapter.dup2();
                load(definition, adapter);

                if (load && post) {
                    Shared.writeDup(adapter, after.sort.size, false, true);
                }

                Shared.writeCast(adapter, there);
                expression.write(settings, definition, adapter);
                Shared.writeBinaryInstruction(settings, definition, adapter, location, there.to, operation);

                if (settings.getNumericOverflow() || !expression.typesafe ||
                    !Shared.writeExactInstruction(definition, adapter, expression.actual.sort, after.sort)) {
                    Shared.writeCast(adapter, back);
                }

                store(definition, adapter);
            } else {
                if (load) {
                    Shared.writeDup(adapter, after.sort.size, false, true);
                }

                store(definition, adapter);
            }
        } else {
            load(definition, adapter);
        }
    }

    protected void load(final Definition definition, final GeneratorAdapter adapter) {
        adapter.loadThis();
        adapter.getField(CLASS_TYPE, "definition", DEFINITION_TYPE);
        adapter.push(index.typesafe);
        adapter.invokeStatic(definition.defobjType.type, DEF_ARRAY_LOAD);
    }

    protected void store(final Definition definition, final GeneratorAdapter adapter) {
        adapter.loadThis();
        adapter.getField(CLASS_TYPE, "definition", DEFINITION_TYPE);
        adapter.push(index.typesafe);
        adapter.push(operation == null && expression.typesafe);
        adapter.invokeStatic(definition.defobjType.type, DEF_ARRAY_STORE);
    }
}
