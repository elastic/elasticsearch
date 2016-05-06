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
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.tree.analyzer.Operation;
import org.elasticsearch.painless.tree.analyzer.Variables;
import org.elasticsearch.painless.tree.writer.Shared;
import org.objectweb.asm.commons.GeneratorAdapter;

import static org.elasticsearch.painless.tree.writer.Constants.CLASS_TYPE;
import static org.elasticsearch.painless.tree.writer.Constants.DEFINITION_TYPE;
import static org.elasticsearch.painless.tree.writer.Constants.DEF_FIELD_LOAD;
import static org.elasticsearch.painless.tree.writer.Constants.DEF_FIELD_STORE;

public class LDefField extends ALink {
    protected final String value;

    public LDefField(final String location, final String value) {
        super(location);

        this.value = value;
    }


    @Override
    protected ALink analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        after = definition.defType;

        return this;
    }

    @Override
    protected void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        if (begincat) {
            Shared.writeNewStrings(adapter);
        }

        if (store) {
            if (endcat) {
                adapter.dupX1();
                load(definition, adapter);
                Shared.writeAppendStrings(adapter, after.sort);

                expression.write(settings, definition, adapter);

                if (!(expression instanceof EBinary) ||
                    ((EBinary)expression).operation != Operation.ADD || expression.actual.sort != Sort.STRING) {
                    Shared.writeAppendStrings(adapter, expression.expected.sort);
                }

                Shared.writeToStrings(adapter);
                Shared.writeCast(adapter, back);

                if (load) {
                    Shared.writeDup(adapter, after.sort.size, true, false);
                }

                store(definition, adapter);
            } else if (operation != null) {
                adapter.dup();
                load(definition, adapter);

                if (load && post) {
                    Shared.writeDup(adapter, after.sort.size, true, false);
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
                    Shared.writeDup(adapter, after.sort.size, true, false);
                }

                store(definition, adapter);
            }
        } else {
            load(definition, adapter);
        }
    }

    protected void load(final Definition definition, final GeneratorAdapter adapter) {
        adapter.push(value);
        adapter.loadThis();
        adapter.getField(CLASS_TYPE, "definition", DEFINITION_TYPE);
        adapter.invokeStatic(definition.defobjType.type, DEF_FIELD_LOAD);
    }

    protected void store(final Definition definition, final GeneratorAdapter adapter) {
        adapter.push(value);
        adapter.loadThis();
        adapter.getField(CLASS_TYPE, "definition", DEFINITION_TYPE);
        adapter.push(operation == null && expression.typesafe);
        adapter.invokeStatic(definition.defobjType.type, DEF_FIELD_STORE);
    }
}
