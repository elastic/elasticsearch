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
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.MethodWriter;

import java.util.Arrays;
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
    Type shiftDistance; // for shifts, the RHS is promoted independently
    Cast there = null;
    Cast back = null;
    
    /** Creates a new RHS-only EChain */
    public EChain(Location location, ALink link) {
        this(location, Arrays.asList(link), false, false, null, null);
    }

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
    void analyze(Locals locals) {
        analyzeLinks(locals);
        analyzeIncrDecr();

        if (operation != null) {
            analyzeCompound(locals);
        } else if (expression != null) {
            analyzeWrite(locals);
        } else {
            analyzeRead();
        }
    }

    private void analyzeLinks(Locals variables) {
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

    private void analyzeCompound(Locals variables) {
        ALink last = links.get(links.size() - 1);

        expression.analyze(variables);
        boolean shift = false;

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
            shiftDistance = AnalyzerCaster.promoteNumeric(expression.actual, false);
            shift = true;
        } else if (operation == Operation.RSH) {
            promote = AnalyzerCaster.promoteNumeric(last.after, false);
            shiftDistance = AnalyzerCaster.promoteNumeric(expression.actual, false);
            shift = true;
        } else if (operation == Operation.USH) {
            promote = AnalyzerCaster.promoteNumeric(last.after, false);
            shiftDistance = AnalyzerCaster.promoteNumeric(expression.actual, false);
            shift = true;
        } else if (operation == Operation.BWAND) {
            promote = AnalyzerCaster.promoteXor(last.after, expression.actual);
        } else if (operation == Operation.XOR) {
            promote = AnalyzerCaster.promoteXor(last.after, expression.actual);
        } else if (operation == Operation.BWOR) {
            promote = AnalyzerCaster.promoteXor(last.after, expression.actual);
        } else {
            throw createError(new IllegalStateException("Illegal tree structure."));
        }

        if (promote == null || (shift && shiftDistance == null)) {
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
        } else if (shift) {
            if (shiftDistance.sort == Sort.LONG) {
                expression.expected = Definition.INT_TYPE;
                expression.explicit = true;   
            } else {
                expression.expected = shiftDistance;
            }
        } else {
            expression.expected = promote;
        }

        expression = expression.cast(variables);

        there = AnalyzerCaster.getLegalCast(location, last.after, promote, false, false);
        back = AnalyzerCaster.getLegalCast(location, promote, last.after, true, false);

        this.statement = true;
        this.actual = read ? last.after : Definition.VOID_TYPE;
    }

    private void analyzeWrite(Locals variables) {
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

    /**
     * Handles writing byte code for variable/method chains for all given possibilities
     * including String concatenation, compound assignment, regular assignment, and simple
     * reads.  Includes proper duplication for chained assignments and assignments that are
     * also read from.
     *
     * Example given 'x[0] += 5;' where x is an array of shorts and x[0] is 1.
     * Note this example has two links -- x (LVariable) and [0] (LBrace).
     * The following steps occur:
     * 1. call link{x}.write(...) -- no op [...]
     * 2. call link{x}.load(...) -- loads the address of the x array onto the stack [..., address(x)]
     * 3. call writer.dup(...) -- dup's the address of the x array onto the stack for later use with store [..., address(x), address(x)]
     * 4. call link{[0]}.write(...) -- load the array index value of the constant int 0 onto the stack [..., address(x), address(x), int(0)]
     * 5. call link{[0]}.load(...) -- load the short value from x[0] onto the stack [..., address(x), short(1)]
     * 6. call writer.writeCast(there) -- casts the short on the stack to an int so it can be added with the rhs [..., address(x), int(1)]
     * 7. call expression.write(...) -- puts the expression's value of the constant int 5 onto the stack [..., address(x), int(1), int(5)]
     * 8. call writer.writeBinaryInstruction(operation) -- writes the int addition instruction [..., address(x), int(6)]
     * 9. call writer.writeCast(back) -- convert the value on the stack back into a short [..., address(x), short(6)]
     * 10. call link{[0]}.store(...) -- store the value on the stack into the 0th index of the array x [...]
     */
    @Override
    void write(MethodWriter writer) {
        writer.writeDebugInfo(location);

        // For the case where the chain represents a String concatenation
        // we must, depending on the Java version, write a StringBuilder or
        // track types going onto the stack.  This must be done before the
        // links in the chain are read because we need the StringBuilder to
        // be placed on the stack ahead of any potential concatenation arguments.
        int catElementStackSize = 0;
        if (cat) {
            catElementStackSize = writer.writeNewStrings();
        }

        ALink last = links.get(links.size() - 1);

        // Go through all the links in the chain first calling write
        // and then load, except for the final link which may be a store.
        // See individual links for more information on what each of the
        // write, load, and store methods do.
        for (ALink link : links) {
            link.write(writer); // call the write method on the link to prepare for a load/store operation

            if (link == last && link.store) {
                if (cat) {
                    // Handle the case where we are doing a compound assignment
                    // representing a String concatenation.

                    writer.writeDup(link.size, catElementStackSize);  // dup the top element and insert it before concat helper on stack
                    link.load(writer);                     // read the current link's value
                    writer.writeAppendStrings(link.after); // append the link's value using the StringBuilder

                    expression.write(writer); // write the bytecode for the rhs expression

                    if (!(expression instanceof EBinary) ||
                        ((EBinary)expression).operation != Operation.ADD || expression.actual.sort != Sort.STRING) {
                        writer.writeAppendStrings(expression.actual); // append the expression's value unless it's also a concatenation
                    }

                    writer.writeToStrings(); // put the value for string concat onto the stack
                    writer.writeCast(back);  // if necessary, cast the String to the lhs actual type

                    if (link.load) {
                        writer.writeDup(link.after.sort.size, link.size); // if this link is also read from dup the value onto the stack
                    }

                    link.store(writer); // store the link's value from the stack in its respective variable/field/array
                } else if (operation != null) {
                    // Handle the case where we are doing a compound assignment that
                    // does not represent a String concatenation.

                    writer.writeDup(link.size, 0); // if necessary, dup the previous link's value to be both loaded from and stored to
                    link.load(writer);             // load the current link's value

                    if (link.load && post) {
                        writer.writeDup(link.after.sort.size, link.size); // dup the value if the link is also
                                                                          // read from and is a post increment
                    }

                    writer.writeCast(there);                                     // if necessary cast the current link's value
                                                                                 // to the promotion type between the lhs and rhs types
                    expression.write(writer);                                    // write the bytecode for the rhs expression
                    // XXX: fix these types, but first we need def compound assignment tests.
                    // its tricky here as there are possibly explicit casts, too.
                    // write the operation instruction for compound assignment
                    if (promote.sort == Sort.DEF) {
                        writer.writeDynamicBinaryInstruction(location, promote, 
                            Definition.DEF_TYPE, Definition.DEF_TYPE, operation, true);
                    } else {
                        writer.writeBinaryInstruction(location, promote, operation);
                    }

                    writer.writeCast(back); // if necessary cast the promotion type value back to the link's type

                    if (link.load && !post) {
                        writer.writeDup(link.after.sort.size, link.size); // dup the value if the link is also
                                                                          // read from and is not a post increment
                    }

                    link.store(writer); // store the link's value from the stack in its respective variable/field/array
                } else {
                    // Handle the case for a simple write.

                    expression.write(writer); // write the bytecode for the rhs expression

                    if (link.load) {
                        writer.writeDup(link.after.sort.size, link.size); // dup the value if the link is also read from
                    }

                    link.store(writer); // store the link's value from the stack in its respective variable/field/array
                }
            } else {
                // Handle the case for a simple read.

                link.load(writer); // read the link's value onto the stack
            }
        }

        writer.writeBranch(tru, fals); // if this is a branch node, write the bytecode to make an appropiate jump
    }
}
