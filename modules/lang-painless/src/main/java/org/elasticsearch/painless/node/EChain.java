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

import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Cast;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Location;
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

    public EChain(Location location, List<ALink> links,
                  boolean pre, boolean post, Operation operation, AExpression expression) {
        super(location);

        this.links = links;
        this.pre = pre;
        this.post = post;
        this.operation = operation;
        this.expression = expression;
    }

    @Override
    void analyze(Variables variables) {
        analyzeLinks(variables);
        analyzeIncrDecr();

        if (operation != null) {
            analyzeCompound(variables);
        } else if (expression != null) {
            analyzeWrite(variables);
        } else {
            analyzeRead();
        }
    }

    private void analyzeLinks(Variables variables) {
        ALink previous = null;
        int index = 0;

        while (index < links.size()) {
            ALink current = links.get(index);

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

            ALink analyzed = current.analyze(variables);

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
        ALink last = links.get(links.size() - 1);

        if (pre && post) {
            throw createError(new IllegalStateException("Illegal tree structure."));
        } else if (pre || post) {
            if (expression != null) {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }

            Sort sort = last.after.sort;

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
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }
    }

    private void analyzeCompound(Variables variables) {
        ALink last = links.get(links.size() - 1);

        expression.analyze(variables);

        if (operation == Operation.MUL) {
            promote = AnalyzerCaster.promoteNumeric(last.after, expression.actual, true);
        } else if (operation == Operation.DIV) {
            promote = AnalyzerCaster.promoteNumeric(last.after, expression.actual, true);
        } else if (operation == Operation.REM) {
            promote = AnalyzerCaster.promoteNumeric(last.after, expression.actual, true);
        } else if (operation == Operation.ADD) {
            promote = AnalyzerCaster.promoteAdd(last.after, expression.actual);
        } else if (operation == Operation.SUB) {
            promote = AnalyzerCaster.promoteNumeric(last.after, expression.actual, true);
        } else if (operation == Operation.LSH) {
            promote = AnalyzerCaster.promoteNumeric(last.after, false);
        } else if (operation == Operation.RSH) {
            promote = AnalyzerCaster.promoteNumeric(last.after, false);
        } else if (operation == Operation.USH) {
            promote = AnalyzerCaster.promoteNumeric(last.after, false);
        } else if (operation == Operation.BWAND) {
            promote = AnalyzerCaster.promoteXor(last.after, expression.actual);
        } else if (operation == Operation.XOR) {
            promote = AnalyzerCaster.promoteXor(last.after, expression.actual);
        } else if (operation == Operation.BWOR) {
            promote = AnalyzerCaster.promoteXor(last.after, expression.actual);
        } else {
            throw createError(new IllegalStateException("Illegal tree structure."));
        }

        if (promote == null) {
            throw createError(new ClassCastException("Cannot apply compound assignment " +
                "[" + operation.symbol + "=] to types [" + last.after + "] and [" + expression.actual + "]."));
        }

        cat = operation == Operation.ADD && promote.sort == Sort.STRING;

        if (cat) {
            if (expression instanceof EBinary && ((EBinary)expression).operation == Operation.ADD &&
                expression.actual.sort == Sort.STRING) {
                ((EBinary)expression).cat = true;
            }

            expression.expected = expression.actual;
        } else if (operation == Operation.LSH || operation == Operation.RSH || operation == Operation.USH) {
            expression.expected = Definition.INT_TYPE;
            expression.explicit = true;
        } else {
            expression.expected = promote;
        }

        expression = expression.cast(variables);

        there = AnalyzerCaster.getLegalCast(location, last.after, promote, false, false);
        back = AnalyzerCaster.getLegalCast(location, promote, last.after, true, false);

        this.statement = true;
        this.actual = read ? last.after : Definition.VOID_TYPE;
    }

    private void analyzeWrite(Variables variables) {
        ALink last = links.get(links.size() - 1);

        // If the store node is a def node, we remove the cast to def from the expression
        // and promote the real type to it:
        if (last instanceof IDefLink) {
            expression.analyze(variables);
            last.after = expression.expected = expression.actual;
        } else {
            // otherwise we adapt the type of the expression to the store type
            expression.expected = last.after;
            expression.analyze(variables);
        }

        expression = expression.cast(variables);

        this.statement = true;
        this.actual = read ? last.after : Definition.VOID_TYPE;
    }

    private void analyzeRead() {
        ALink last = links.get(links.size() - 1);

        // If the load node is a def node, we adapt its after type to use _this_ expected output type:
        if (last instanceof IDefLink && this.expected != null) {
            last.after = this.expected;
        }

        constant = last.string;
        statement = last.statement;
        actual = last.after;
    }

    @Override
    void write(MethodWriter writer) {
        // can cause class cast exception among other things at runtime
        writer.writeDebugInfo(location);

        if (cat) {
            writer.writeNewStrings();
        }

        ALink last = links.get(links.size() - 1);

        for (ALink link : links) {
            link.write(writer);

            if (link == last && link.store) {
                if (cat) {
                    writer.writeDup(link.size, 1);
                    link.load(writer);
                    writer.writeAppendStrings(link.after);

                    expression.write(writer);

                    if (!(expression instanceof EBinary) ||
                        ((EBinary)expression).operation != Operation.ADD || expression.actual.sort != Sort.STRING) {
                        writer.writeAppendStrings(expression.actual);
                    }

                    writer.writeToStrings();
                    writer.writeCast(back);

                    if (link.load) {
                        writer.writeDup(link.after.sort.size, link.size);
                    }

                    link.store(writer);
                } else if (operation != null) {
                    writer.writeDup(link.size, 0);
                    link.load(writer);

                    if (link.load && post) {
                        writer.writeDup(link.after.sort.size, link.size);
                    }

                    writer.writeCast(there);
                    expression.write(writer);
                    writer.writeBinaryInstruction(location, promote, operation);

                    writer.writeCast(back);

                    if (link.load && !post) {
                        writer.writeDup(link.after.sort.size, link.size);
                    }

                    link.store(writer);
                } else {
                    expression.write(writer);

                    if (link.load) {
                        writer.writeDup(link.after.sort.size, link.size);
                    }

                    link.store(writer);
                }
            } else {
                link.load(writer);
            }
        }

        writer.writeBranch(tru, fals);
    }
}
