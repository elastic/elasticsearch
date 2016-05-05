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
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.tree.analyzer.Caster;
import org.elasticsearch.painless.tree.analyzer.Operation;
import org.elasticsearch.painless.tree.analyzer.Variables;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.Collections;
import java.util.List;

public class EChain extends AExpression {
    protected final List<ALink> links;
    protected final boolean pre;
    protected final boolean post;
    protected final Operation operation;
    protected AExpression expression;

    public EChain(final String location, final List<ALink> links,
                  final boolean pre, final boolean post, final Operation operation, final AExpression expression) {
        super(location);

        this.links = Collections.unmodifiableList(links);
        this.pre = pre;
        this.post = post;
        this.operation = operation;
        this.expression = expression;
    }

    @Override
    protected void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        ALink previous = null;
        int index = 0;

        while (index < links.size()) {
            final ALink current = links.get(index);

            if (previous != null) {
                current.before = previous.after;

                if (index == 1) {
                    current.statik = previous.statik;
                }
            }

            if (index == links.size() - 1) {
                current.load = read;
                current.store = expression != null || pre || post;
            }

            final ALink analyzed = current.analyze(settings, definition, variables);

            if (analyzed == null) {
                links.remove(index);
            } else {
                if (analyzed != current) {
                    links.set(index, analyzed);
                }

                previous = analyzed;
                ++index;
            }
        }

        if (links.get(0).statik) {
            links.remove(0);
        }

        final ALink first = links.get(0);
        final ALink last = links.get(links.size() - 1);

        if (pre && post) {
            throw new IllegalStateException(error("Illegal tree structure."));
        } else if (pre || post) {
            if (expression != null) {
                throw new IllegalStateException(error("Illegal tree structure."));
            }

            final Sort sort = last.after.sort;

            if (operation == Operation.INCR) {
                if (sort == Sort.DOUBLE) {
                    expression = new EConstant(location, 1D);
                } else if (sort == Sort.FLOAT) {
                    expression = new EConstant(location, 1F);
                } else if (sort == Sort.LONG) {
                    expression = new EConstant(location, 1L);
                } else {
                    expression = new EConstant(location, 1);
                }
            } else if (operation == Operation.DECR) {
                if (sort == Sort.DOUBLE) {
                    expression = new EConstant(location, -1D);
                } else if (sort == Sort.FLOAT) {
                    expression = new EConstant(location, -1F);
                } else if (sort == Sort.LONG) {
                    expression = new EConstant(location, -1L);
                } else {
                    expression = new EConstant(location, -1);
                }
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        if (operation != null) {
            expression.analyze(settings, definition, variables);

            final Type promote;

            if (operation == Operation.MUL) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, true, true);
            } else if (operation == Operation.DIV) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, true, true);
            } else if (operation == Operation.REM) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, true, true);
            } else if (operation == Operation.ADD) {
                promote = Caster.promoteAdd(definition, last.after, expression.actual);
            } else if (operation == Operation.SUB) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, true, true);
            } else if (operation == Operation.LSH) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, false, true);
            } else if (operation == Operation.RSH) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, false, true);
            } else if (operation == Operation.USH) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, false, true);
            } else if (operation == Operation.BWAND) {
                promote = Caster.promoteXor(definition, last.after, expression.actual);
            } else if (operation == Operation.XOR) {
                promote = Caster.promoteXor(definition, last.after, expression.actual);
            } else if (operation == Operation.BWOR) {
                promote = Caster.promoteXor(definition, last.after, expression.actual);
            } else if (operation == Operation.INCR) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, true, true);
            } else if (operation == Operation.DECR) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, true, true);
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }

            if (promote == null) {
                throw new IllegalArgumentException("Cannot apply compound assignment " +
                    "[" + operation.symbol + "]= to types [" + last.after + "] and [" + expression.actual + "].");
            }

            last.cat = operation == Operation.ADD && promote.sort == Sort.STRING;

            if (last.cat) {
                expression.expected = expression.actual;
                expression.strings = true;
                first.strings = true;
            } else {
                expression.expected = promote;
            }

            expression.cast(settings, definition, variables);

            last.expression = expression;
            last.pre = pre;
            last.post = post;
            last.operation = operation;
            last.there = Caster.getLegalCast(definition, location, last.after, promote, expression.typesafe);
            last.back = Caster.getLegalCast(definition, location, promote, last.after, true);

            statement = true;
            actual = read ? last.after : definition.voidType;
            typesafe = actual.sort == Sort.DEF || expression.typesafe;
        } else if (expression != null) {
            expression.expected = last.after;
            expression.analyze(settings, definition, variables);
            expression = expression.cast(settings, definition, variables);
            last.expression = expression;

            statement = true;
            actual = read ? last.after : definition.voidType;
            typesafe = actual.sort == Sort.DEF || expression.typesafe;
        } else {
            constant = last.constant;
            statement = last.statement;
            actual = last.after;
            typesafe = actual.sort == Sort.DEF;
        }
    }

    @Override
    protected void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        for (final ALink link : links) {
            link.write(settings, definition, adapter);
        }
    }
}
