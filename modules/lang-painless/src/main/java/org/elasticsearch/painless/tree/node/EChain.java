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
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.tree.utility.Caster;
import org.elasticsearch.painless.tree.utility.Operation;
import org.elasticsearch.painless.tree.utility.Variables;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.Collections;
import java.util.List;

public class EChain extends Expression {
    protected final List<Link> links;
    protected final boolean pre;
    protected final boolean post;
    protected final Operation store;
    protected Expression expression;

    protected boolean cat = false;
    protected Cast there = null;
    protected Cast back = null;

    public EChain(final String location, final List<Link> links,
                  final boolean pre, final boolean post, final Operation store, final Expression expression) {
        super(location);

        this.links = Collections.unmodifiableList(links);
        this.pre = pre;
        this.post = post;
        this.store = store;
        this.expression = expression;
    }

    @Override
    protected void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        final Link last = links.get(links.size() - 1);
        Link previous = null;

        for (int index = 0; index < links.size(); ++index) {
            Link current = links.get(index);

            if (previous != null) {
                current.before = previous.after;
                current.statik = previous.statik;
            }

            if (current == last) {
                current.load = read;
                current.store = expression != null;
            }

            previous = current.analyze(settings, definition, variables);

            if (previous != current) {
                links.set(index, previous);
            }
        }

        if (pre && post) {
            throw new IllegalStateException(error("Illegal tree structure."));
        } else if (pre || post) {
            if (expression != null) {
                throw new IllegalStateException(error("Illegal tree structure."));
            }

            final Sort sort = last.after.sort;

            if (store == Operation.INCR) {
                if (sort == Sort.DOUBLE) {
                    expression = new EConstant(location, 1D);
                } else if (sort == Sort.FLOAT) {
                    expression = new EConstant(location, 1F);
                } else if (sort == Sort.LONG) {
                    expression = new EConstant(location, 1L);
                } else {
                    expression = new EConstant(location, 1);
                }
            } else if (store == Operation.DECR) {
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

        if (store != null) {
            expression.analyze(settings, definition, variables);

            final Type promote;

            if (store == Operation.MUL) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, true, true);
            } else if (store == Operation.DIV) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, true, true);
            } else if (store == Operation.REM) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, true, true);
            } else if (store == Operation.ADD) {
                promote = Caster.promoteAdd(definition, last.after, expression.actual);
            } else if (store == Operation.SUB) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, true, true);
            } else if (store == Operation.LSH) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, false, true);
            } else if (store == Operation.RSH) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, false, true);
            } else if (store == Operation.USH) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, false, true);
            } else if (store == Operation.AND) {
                promote = Caster.promoteXor(definition, last.after, expression.actual);
            } else if (store == Operation.XOR) {
                promote = Caster.promoteXor(definition, last.after, expression.actual);
            } else if (store == Operation.OR) {
                promote = Caster.promoteXor(definition, last.after, expression.actual);
            } else if (store == Operation.INCR) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, true, true);
            } else if (store == Operation.DECR) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, true, true);
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }

            if (promote == null) {
                throw new IllegalArgumentException("Cannot apply compound assignment to " +
                    "types [" + last.after + "] and [" + expression.actual + "].");
            }

            cat = store == Operation.ADD && promote.sort == Sort.STRING;

            if (cat) {
                expression.expected = expression.actual;
                expression.strings = true;
            } else {
                expression.expected = promote;
            }

            expression.cast(definition);

            there = Caster.getLegalCast(definition, location, last.after, promote, expression.typesafe);
            back = Caster.getLegalCast(definition, location, promote, last.after, true);

            statement = true;
            actual = read ? last.after : definition.voidType;
            typesafe = actual.sort == Sort.DEF || expression.typesafe;
        } else if (expression != null) {
            expression.expected = last.after;
            expression.analyze(settings, definition, variables);
            expression = expression.cast(definition);

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
    protected void write(final GeneratorAdapter adapter) {
        final Link last = links.get(links.size() - 1);

        for (final Link link : links) {
            if (link == last && expression != null) {
                if (cat) {

                }
            } else {
                link.load(adapter);
            }
        }
    }
}
