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

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Cast;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.Variables;
import org.elasticsearch.painless.MethodWriter;

import java.util.List;

/**
 * Represents the entirety of a variable/method chain for read/write operations.
 */
public final class EChain extends AExpression {

    final List<ALink> links;
    final boolean pre;
    final boolean post;
    Operation operation;
    AExpression expression;

    boolean cat = false;
    Type promote = null;
    Cast there = null;
    Cast back = null;

    public EChain(final int line, final String location, final List<ALink> links,
                  final boolean pre, final boolean post, final Operation operation, final AExpression expression) {
        super(line, location);

        this.links = links;
        this.pre = pre;
        this.post = post;
        this.operation = operation;
        this.expression = expression;
    }

    @Override
    void analyze(final CompilerSettings settings, final Definition definition, final Variables variables) {
        analyzeLinks(settings, definition, variables);
        analyzeIncrDecr();

        if (operation != null) {
            analyzeCompound(settings, definition, variables);
        } else if (expression != null) {
            analyzeWrite(settings, definition, variables);
        } else {
            analyzeRead();
        }
    }

    private void analyzeLinks(final CompilerSettings settings, final Definition definition, final Variables variables) {
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
    }

    private void analyzeIncrDecr() {
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
                    expression = new EConstant(line, location, 1D);
                } else if (sort == Sort.FLOAT) {
                    expression = new EConstant(line, location, 1F);
                } else if (sort == Sort.LONG) {
                    expression = new EConstant(line, location, 1L);
                } else {
                    expression = new EConstant(line, location, 1);
                }

                operation = Operation.ADD;
            } else if (operation == Operation.DECR) {
                if (sort == Sort.DOUBLE) {
                    expression = new EConstant(line, location, 1D);
                } else if (sort == Sort.FLOAT) {
                    expression = new EConstant(line, location, 1F);
                } else if (sort == Sort.LONG) {
                    expression = new EConstant(line, location, 1L);
                } else {
                    expression = new EConstant(line, location, 1);
                }

                operation = Operation.SUB;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }
    }

    private void analyzeCompound(final CompilerSettings settings, final Definition definition, final Variables variables) {
        final ALink last = links.get(links.size() - 1);

        expression.analyze(settings, definition, variables);

        if (operation == Operation.MUL) {
            promote = AnalyzerCaster.promoteNumeric(definition, last.after, expression.actual, true, true);
        } else if (operation == Operation.DIV) {
            promote = AnalyzerCaster.promoteNumeric(definition, last.after, expression.actual, true, true);
        } else if (operation == Operation.REM) {
            promote = AnalyzerCaster.promoteNumeric(definition, last.after, expression.actual, true, true);
        } else if (operation == Operation.ADD) {
            promote = AnalyzerCaster.promoteAdd(definition, last.after, expression.actual);
        } else if (operation == Operation.SUB) {
            promote = AnalyzerCaster.promoteNumeric(definition, last.after, expression.actual, true, true);
        } else if (operation == Operation.LSH) {
            promote = AnalyzerCaster.promoteNumeric(definition, last.after, false, true);
        } else if (operation == Operation.RSH) {
            promote = AnalyzerCaster.promoteNumeric(definition, last.after, false, true);
        } else if (operation == Operation.USH) {
            promote = AnalyzerCaster.promoteNumeric(definition, last.after, false, true);
        } else if (operation == Operation.BWAND) {
            promote = AnalyzerCaster.promoteXor(definition, last.after, expression.actual);
        } else if (operation == Operation.XOR) {
            promote = AnalyzerCaster.promoteXor(definition, last.after, expression.actual);
        } else if (operation == Operation.BWOR) {
            promote = AnalyzerCaster.promoteXor(definition, last.after, expression.actual);
        } else {
            throw new IllegalStateException(error("Illegal tree structure."));
        }

        if (promote == null) {
            throw new ClassCastException("Cannot apply compound assignment " +
                "[" + operation.symbol + "=] to types [" + last.after + "] and [" + expression.actual + "].");
        }

        cat = operation == Operation.ADD && promote.sort == Sort.STRING;

        if (cat) {
            if (expression instanceof EBinary && ((EBinary)expression).operation == Operation.ADD &&
                expression.actual.sort == Sort.STRING) {
                ((EBinary)expression).cat = true;
            }

            expression.expected = expression.actual;
        } else if (operation == Operation.LSH || operation == Operation.RSH || operation == Operation.USH) {
            expression.expected = definition.getType("int");
            expression.explicit = true;
        } else {
            expression.expected = promote;
        }

        expression = expression.cast(settings, definition, variables);

        there = AnalyzerCaster.getLegalCast(definition, location, last.after, promote, false);
        back = AnalyzerCaster.getLegalCast(definition, location, promote, last.after, true);

        this.statement = true;
        this.actual = read ? last.after : definition.getType("void");
    }

    private void analyzeWrite(final CompilerSettings settings, final Definition definition, final Variables variables) {
        final ALink last = links.get(links.size() - 1);

        // If the store node is a DEF node, we remove the cast to DEF from the expression
        // and promote the real type to it:
        if (last instanceof IDefLink) {
            expression.analyze(settings, definition, variables);
            last.after = expression.expected = expression.actual;
        } else {
            // otherwise we adapt the type of the expression to the store type
            expression.expected = last.after;
            expression.analyze(settings, definition, variables);
        }

        expression = expression.cast(settings, definition, variables);

        this.statement = true;
        this.actual = read ? last.after : definition.getType("void");
    }

    private void analyzeRead() {
        final ALink last = links.get(links.size() - 1);

        // If the load node is a DEF node, we adapt its after type to use _this_ expected output type:
        if (last instanceof IDefLink && this.expected != null) {
            last.after = this.expected;
        }

        constant = last.string;
        statement = last.statement;
        actual = last.after;
    }

    @Override
    void write(final CompilerSettings settings, final Definition definition, final MethodWriter adapter) {
        if (cat) {
            adapter.writeNewStrings();
        }

        final ALink last = links.get(links.size() - 1);

        for (final ALink link : links) {
            link.write(settings, definition, adapter);

            if (link == last && link.store) {
                if (cat) {
                    adapter.writeDup(link.size, 1);
                    link.load(settings, definition, adapter);
                    adapter.writeAppendStrings(link.after);

                    expression.write(settings, definition, adapter);

                    if (!(expression instanceof EBinary) ||
                        ((EBinary)expression).operation != Operation.ADD || expression.actual.sort != Sort.STRING) {
                        adapter.writeAppendStrings(expression.actual);
                    }

                    adapter.writeToStrings();
                    adapter.writeCast(back);

                    if (link.load) {
                        adapter.writeDup(link.after.sort.size, link.size);
                    }

                    link.store(settings, definition, adapter);
                } else if (operation != null) {
                    adapter.writeDup(link.size, 0);
                    link.load(settings, definition, adapter);

                    if (link.load && post) {
                        adapter.writeDup(link.after.sort.size, link.size);
                    }

                    adapter.writeCast(there);
                    expression.write(settings, definition, adapter);
                    adapter.writeBinaryInstruction(definition, location, promote, operation);

                    adapter.writeCast(back);

                    if (link.load && !post) {
                        adapter.writeDup(link.after.sort.size, link.size);
                    }

                    link.store(settings, definition, adapter);
                } else {
                    expression.write(settings, definition, adapter);

                    if (link.load) {
                        adapter.writeDup(link.after.sort.size, link.size);
                    }

                    link.store(settings, definition, adapter);
                }
            } else {
                link.load(settings, definition, adapter);
            }
        }

        adapter.writeBranch(tru, fals);
    }
}
