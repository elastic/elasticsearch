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
import org.elasticsearch.painless.tree.analyzer.Caster;
import org.elasticsearch.painless.tree.analyzer.Operation;
import org.elasticsearch.painless.tree.analyzer.Variables;
import org.elasticsearch.painless.tree.writer.Shared;
import org.objectweb.asm.commons.GeneratorAdapter;

import java.util.List;

public class EChain extends AExpression {
    protected final List<ALink> links;
    protected final boolean pre;
    protected final boolean post;
    protected Operation operation;
    protected AExpression expression;

    protected boolean cat = false;
    protected Type promote = null;
    protected boolean exact = false;
    protected Cast there = null;
    protected Cast back = null;

    public EChain(final String location, final List<ALink> links,
                  final boolean pre, final boolean post, final Operation operation, final AExpression expression) {
        super(location);

        this.links = links;
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

                operation = Operation.ADD;
            } else if (operation == Operation.DECR) {
                if (sort == Sort.DOUBLE) {
                    expression = new EConstant(location, 1D);
                } else if (sort == Sort.FLOAT) {
                    expression = new EConstant(location, 1F);
                } else if (sort == Sort.LONG) {
                    expression = new EConstant(location, 1L);
                } else {
                    expression = new EConstant(location, 1);
                }

                operation = Operation.SUB;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        if (operation != null) {
            expression.analyze(settings, definition, variables);

            if (operation == Operation.MUL) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, true, true);
                exact = true;
            } else if (operation == Operation.DIV) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, true, true);
                exact = true;
            } else if (operation == Operation.REM) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, true, true);
                exact = true;
            } else if (operation == Operation.ADD) {
                promote = Caster.promoteAdd(definition, last.after, expression.actual);
                exact = true;
            } else if (operation == Operation.SUB) {
                promote = Caster.promoteNumeric(definition, last.after, expression.actual, true, true);
                exact = true;
            } else if (operation == Operation.LSH) {
                promote = Caster.promoteNumeric(definition, last.after, false, true);
            } else if (operation == Operation.RSH) {
                promote = Caster.promoteNumeric(definition, last.after, false, true);
            } else if (operation == Operation.USH) {
                promote = Caster.promoteNumeric(definition, last.after, false, true);
            } else if (operation == Operation.BWAND) {
                promote = Caster.promoteXor(definition, last.after, expression.actual);
            } else if (operation == Operation.XOR) {
                promote = Caster.promoteXor(definition, last.after, expression.actual);
            } else if (operation == Operation.BWOR) {
                promote = Caster.promoteXor(definition, last.after, expression.actual);
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }

            if (promote == null) {
                throw new IllegalArgumentException("Cannot apply compound assignment " +
                    "[" + operation.symbol + "]= to types [" + last.after + "] and [" + expression.actual + "].");
            }

            cat = operation == Operation.ADD && promote.sort == Sort.STRING;

            if (cat) {
                if (expression instanceof EBinary && ((EBinary)expression).operation == Operation.ADD &&
                    expression.actual.sort == Sort.STRING) {
                    ((EBinary)expression).cat = true;
                }

                expression.expected = expression.actual;
            } else if (operation == Operation.LSH || operation == Operation.RSH || operation == Operation.USH) {
                expression.expected = definition.intType;
            } else {
                expression.expected = promote;
            }

            expression = expression.cast(settings, definition, variables);

            exact &= settings.getNumericOverflow() && !expression.typesafe;
            there = Caster.getLegalCast(definition, location, last.after, promote, expression.typesafe);
            back = Caster.getLegalCast(definition, location, promote, last.after, true);

            statement = true;
            actual = read ? last.after : definition.voidType;
            typesafe = actual.sort != Sort.DEF && expression.typesafe;
        } else if (expression != null) {
            expression.expected = last.after;
            expression.analyze(settings, definition, variables);
            expression = expression.cast(settings, definition, variables);
            last.typesafe = expression.typesafe;

            statement = true;
            actual = read ? last.after : definition.voidType;
            typesafe = actual.sort != Sort.DEF && expression.typesafe;
        } else {
            constant = last.constant;
            statement = last.statement;
            actual = last.after;
            typesafe = actual.sort != Sort.DEF;
        }
    }

    @Override
    protected void write(final CompilerSettings settings, final Definition definition, final GeneratorAdapter adapter) {
        if (cat) {
            Shared.writeNewStrings(adapter);
        }

        final ALink last = links.get(links.size() - 1);

        for (final ALink link : links) {
            link.write(settings, definition, adapter);

            if (link == last && link.store) {
                if (cat) {
                    Shared.writeDup(adapter, link.size, 1);
                    link.load(settings, definition, adapter);
                    Shared.writeAppendStrings(adapter, link.after.sort);

                    expression.write(settings, definition, adapter);

                    if (!(expression instanceof EBinary) ||
                        ((EBinary)expression).operation != Operation.ADD || expression.actual.sort != Sort.STRING) {
                        Shared.writeAppendStrings(adapter, expression.expected.sort);
                    }

                    Shared.writeToStrings(adapter);
                    Shared.writeCast(adapter, back);

                    if (link.load) {
                        Shared.writeDup(adapter, link.after.sort.size, link.size);
                    }

                    link.store(settings, definition, adapter);
                } else if (operation != null) {
                    Shared.writeDup(adapter, link.size, 0);
                    link.load(settings, definition, adapter);

                    if (link.load && post) {
                        Shared.writeDup(adapter, link.after.sort.size, link.size);
                    }

                    Shared.writeCast(adapter, there);
                    expression.write(settings, definition, adapter);
                    Shared.writeBinaryInstruction(settings, definition, adapter, location, promote, operation);

                    if (!exact || !Shared.writeExactInstruction(definition, adapter, promote.sort, link.after.sort)) {
                        Shared.writeCast(adapter, back);
                    }

                    if (link.load && !post) {
                        Shared.writeDup(adapter, link.after.sort.size, link.size);
                    }

                    link.store(settings, definition, adapter);
                } else {
                    expression.write(settings, definition, adapter);

                    if (link.load) {
                        Shared.writeDup(adapter, link.after.sort.size, link.size);
                    }

                    link.store(settings, definition, adapter);
                }
            } else {
                link.load(settings, definition, adapter);
            }
        }

        Shared.writeBranch(adapter, tru, fals);
    }
}
