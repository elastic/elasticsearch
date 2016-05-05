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
import org.elasticsearch.painless.tree.analyzer.Variables;
import org.elasticsearch.painless.tree.writer.Shared;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.List;
import java.util.Map;

public class LBrace extends ALink {
    protected AExpression index;

    public LBrace(final String location, final AExpression index) {
        super(location);

        this.index = index;
    }

    @Override
    protected ALink analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        if (before == null) {
            throw new IllegalStateException(error("Illegal tree structure."));
        }

        final Sort sort = before.sort;

        if (sort == Sort.ARRAY) {
            index.expected = definition.intType;
            index.analyze(settings, definition, variables);
            index = index.cast(settings, definition, variables);

            after = definition.getType(before.struct, before.dimensions - 1);

            return this;
        } else if (sort == Sort.DEF) {
            return new LDefArray(location, index).copy(this).analyze(settings, definition, variables);
        } else {
            try {
                before.clazz.asSubclass(Map.class);

                return new LMapShortcut(location, index).copy(this).analyze(settings, definition, variables);
            } catch (final ClassCastException exception) {
                // Do nothing.
            }

            try {
                before.clazz.asSubclass(List.class);

                return new LListShortcut(location, index).copy(this).analyze(settings, definition, variables);
            } catch (final ClassCastException exception) {
                // Do nothing.
            }
        }

        throw new IllegalArgumentException(error("Illegal array access on type [" + before.name + "]."));
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
                adapter.arrayLoad(before.type);

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

                adapter.arrayStore(before.type);
            } else {
                if (load) {
                    Shared.writeDup(adapter, after.sort.size, false, true);
                }

                adapter.arrayStore(before.type);
            }
        } else {
            adapter.arrayLoad(before.type);
        }
    }
}
