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

import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.Scope;
import org.elasticsearch.painless.ir.BinaryMathNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Represents a binary math expression.
 */
public final class EBinary extends AExpression {

    final Operation operation;
    private AExpression left;
    private AExpression right;

    private Class<?> promote = null;            // promoted type
    private Class<?> shiftDistance = null;      // for shifts, the rhs is promoted independently
    boolean cat = false;
    private boolean originallyExplicit = false; // record whether there was originally an explicit cast

    public EBinary(Location location, Operation operation, AExpression left, AExpression right) {
        super(location);

        this.operation = Objects.requireNonNull(operation);
        this.left = Objects.requireNonNull(left);
        this.right = Objects.requireNonNull(right);
    }

    @Override
    Output analyze(ScriptRoot scriptRoot, Scope scope, Input input) {
        this.input = input;
        output = new Output();

        originallyExplicit = input.explicit;

        Output leftOutput = left.analyze(scriptRoot, scope, new Input());
        Output rightOutput = right.analyze(scriptRoot, scope, new Input());

        /*
         if (operation == Operation.DIV) {
            analyzeDiv(scriptRoot, scope);
        } else if (operation == Operation.REM) {
            analyzeRem(scriptRoot, scope);
        } else if (operation == Operation.ADD) {
            analyzeAdd(scriptRoot, scope);
        } else if (operation == Operation.SUB) {
            analyzeSub(scriptRoot, scope);
        } else if (operation == Operation.FIND) {
            analyzeRegexOp(scriptRoot, scope);
        } else if (operation == Operation.MATCH) {
            analyzeRegexOp(scriptRoot, scope);
        } else if (operation == Operation.LSH) {
            analyzeLSH(scriptRoot, scope);
        } else if (operation == Operation.RSH) {
            analyzeRSH(scriptRoot, scope);
        } else if (operation == Operation.USH) {
            analyzeUSH(scriptRoot, scope);
        } else if (operation == Operation.BWAND) {
            analyzeBWAnd(scriptRoot, scope);
        } else if (operation == Operation.XOR) {
            analyzeXor(scriptRoot, scope);
        } else if (operation == Operation.BWOR) {
            analyzeBWOr(scriptRoot, scope);
        } else {
            throw createError(new IllegalStateException("Illegal tree structure."));
        }
         */

        if (operation == Operation.MUL) {
            promote = AnalyzerCaster.promoteNumeric(leftOutput.actual, rightOutput.actual, true);

            if (promote == null) {
                throw createError(new ClassCastException("Cannot apply multiply [*] to types " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(leftOutput.actual) + "] and " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(rightOutput.actual) + "]."));
            }
        } else if (operation == Operation.DIV) {
            if (promote == null) {
                throw createError(new ClassCastException("Cannot apply divide [/] to types " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(left.actual) + "] and " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(right.actual) + "]."));
            }
        }

        output.actual = promote;

        if (promote == def.class) {
            left.input.expected = leftOutput.actual;
            right.input.expected = rightOutput.actual;
            if (input.expected != null) {
                output.actual = input.expected;
            }
        } else {
            left.input.expected = promote;
            right.input.expected = promote;
        }

        left.cast();
        right.cast();

        return output;
    }

    private void analyzeDiv(ScriptRoot scriptRoot, Scope variables) {
        left.analyze(scriptRoot, variables);
        right.analyze(scriptRoot, variables);

        promote = AnalyzerCaster.promoteNumeric(left.actual, right.actual, true);



        actual = promote;

        if (promote == def.class) {
            left.expected = left.actual;
            right.expected = right.actual;

            if (expected != null) {
                actual = expected;
            }
        } else {
            left.expected = promote;
            right.expected = promote;
        }

        left.cast();
        right.cast();
    }

    private void analyzeRem(ScriptRoot scriptRoot, Scope variables) {
        left.analyze(scriptRoot, variables);
        right.analyze(scriptRoot, variables);

        promote = AnalyzerCaster.promoteNumeric(left.actual, right.actual, true);

        if (promote == null) {
            throw createError(new ClassCastException("Cannot apply remainder [%] to types " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(left.actual) + "] and " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(right.actual) + "]."));
        }

        actual = promote;

        if (promote == def.class) {
            left.expected = left.actual;
            right.expected = right.actual;

            if (expected != null) {
                actual = expected;
            }
        } else {
            left.expected = promote;
            right.expected = promote;
        }

        left.cast();
        right.cast();
    }

    private void analyzeAdd(ScriptRoot scriptRoot, Scope variables) {
        left.analyze(scriptRoot, variables);
        right.analyze(scriptRoot, variables);

        promote = AnalyzerCaster.promoteAdd(left.actual, right.actual);

        if (promote == null) {
            throw createError(new ClassCastException("Cannot apply add [+] to types " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(left.actual) + "] and " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(right.actual) + "]."));
        }

        actual = promote;

        if (promote == String.class) {
            left.expected = left.actual;

            if (left instanceof EBinary && ((EBinary)left).operation == Operation.ADD && left.actual == String.class) {
                ((EBinary)left).cat = true;
            }

            right.expected = right.actual;

            if (right instanceof EBinary && ((EBinary)right).operation == Operation.ADD && right.actual == String.class) {
                ((EBinary)right).cat = true;
            }
        } else if (promote == def.class) {
            left.expected = left.actual;
            right.expected = right.actual;

            if (expected != null) {
                actual = expected;
            }
        } else {
            left.expected = promote;
            right.expected = promote;
        }

        left.cast();
        right.cast();
    }

    private void analyzeSub(ScriptRoot scriptRoot, Scope variables) {
        left.analyze(scriptRoot, variables);
        right.analyze(scriptRoot, variables);

        promote = AnalyzerCaster.promoteNumeric(left.actual, right.actual, true);

        if (promote == null) {
            throw createError(new ClassCastException("Cannot apply subtract [-] to types " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(left.actual) + "] and " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(right.actual) + "]."));
        }

        actual = promote;

        if (promote == def.class) {
            left.expected = left.actual;
            right.expected = right.actual;

            if (expected != null) {
                actual = expected;
            }
        } else {
            left.expected = promote;
            right.expected = promote;
        }

        left.cast();
        right.cast();
    }

    private void analyzeRegexOp(ScriptRoot scriptRoot, Scope variables) {
        left.analyze(scriptRoot, variables);
        right.analyze(scriptRoot, variables);

        left.expected = String.class;
        right.expected = Pattern.class;

        left.cast();
        right.cast();

        promote = boolean.class;
        actual = boolean.class;
    }

    private void analyzeLSH(ScriptRoot scriptRoot, Scope variables) {
        left.analyze(scriptRoot, variables);
        right.analyze(scriptRoot, variables);

        Class<?> lhspromote = AnalyzerCaster.promoteNumeric(left.actual, false);
        Class<?> rhspromote = AnalyzerCaster.promoteNumeric(right.actual, false);

        if (lhspromote == null || rhspromote == null) {
            throw createError(new ClassCastException("Cannot apply left shift [<<] to types " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(left.actual) + "] and " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(right.actual) + "]."));
        }

        actual = promote = lhspromote;
        shiftDistance = rhspromote;

        if (lhspromote == def.class || rhspromote == def.class) {
            left.expected = left.actual;
            right.expected = right.actual;

            if (expected != null) {
                actual = expected;
            }
        } else {
            left.expected = lhspromote;

            if (rhspromote == long.class) {
                right.expected = int.class;
                right.explicit = true;
            } else {
                right.expected = rhspromote;
            }
        }

        left.cast();
        right.cast();
    }

    private void analyzeRSH(ScriptRoot scriptRoot, Scope variables) {
        left.analyze(scriptRoot, variables);
        right.analyze(scriptRoot, variables);

        Class<?> lhspromote = AnalyzerCaster.promoteNumeric(left.actual, false);
        Class<?> rhspromote = AnalyzerCaster.promoteNumeric(right.actual, false);

        if (lhspromote == null || rhspromote == null) {
            throw createError(new ClassCastException("Cannot apply right shift [>>] to types " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(left.actual) + "] and " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(right.actual) + "]."));
        }

        actual = promote = lhspromote;
        shiftDistance = rhspromote;

        if (lhspromote == def.class || rhspromote == def.class) {
            left.expected = left.actual;
            right.expected = right.actual;

            if (expected != null) {
                actual = expected;
            }
        } else {
            left.expected = lhspromote;

            if (rhspromote == long.class) {
                right.expected = int.class;
                right.explicit = true;
            } else {
                right.expected = rhspromote;
            }
        }

        left.cast();
        right.cast();
    }

    private void analyzeUSH(ScriptRoot scriptRoot, Scope variables) {
        left.analyze(scriptRoot, variables);
        right.analyze(scriptRoot, variables);

        Class<?> lhspromote = AnalyzerCaster.promoteNumeric(left.actual, false);
        Class<?> rhspromote = AnalyzerCaster.promoteNumeric(right.actual, false);

        actual = promote = lhspromote;
        shiftDistance = rhspromote;

        if (lhspromote == null || rhspromote == null) {
            throw createError(new ClassCastException("Cannot apply unsigned shift [>>>] to types " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(left.actual) + "] and " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(right.actual) + "]."));
        }

        if (lhspromote == def.class || rhspromote == def.class) {
            left.expected = left.actual;
            right.expected = right.actual;

            if (expected != null) {
                actual = expected;
            }
        } else {
            left.expected = lhspromote;

            if (rhspromote == long.class) {
                right.expected = int.class;
                right.explicit = true;
            } else {
                right.expected = rhspromote;
            }
        }

        left.cast();
        right.cast();
    }

    private void analyzeBWAnd(ScriptRoot scriptRoot, Scope variables) {
        left.analyze(scriptRoot, variables);
        right.analyze(scriptRoot, variables);

        promote = AnalyzerCaster.promoteNumeric(left.actual, right.actual, false);

        if (promote == null) {
            throw createError(new ClassCastException("Cannot apply and [&] to types " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(left.actual) + "] and " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(right.actual) + "]."));
        }

        actual = promote;

        if (promote == def.class) {
            left.expected = left.actual;
            right.expected = right.actual;

            if (expected != null) {
                actual = expected;
            }
        } else {
            left.expected = promote;
            right.expected = promote;
        }

        left.cast();
        right.cast();
    }

    private void analyzeXor(ScriptRoot scriptRoot, Scope variables) {
        left.analyze(scriptRoot, variables);
        right.analyze(scriptRoot, variables);

        promote = AnalyzerCaster.promoteXor(left.actual, right.actual);

        if (promote == null) {
            throw createError(new ClassCastException("Cannot apply xor [^] to types " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(left.actual) + "] and " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(right.actual) + "]."));
        }

        actual = promote;

        if (promote == def.class) {
            left.expected = left.actual;
            right.expected = right.actual;
            if (expected != null) {
                actual = expected;
            }
        } else {
            left.expected = promote;
            right.expected = promote;
        }

        left.cast();
        right.cast();
    }

    private void analyzeBWOr(ScriptRoot scriptRoot, Scope variables) {
        left.analyze(scriptRoot, variables);
        right.analyze(scriptRoot, variables);

        promote = AnalyzerCaster.promoteNumeric(left.actual, right.actual, false);

        if (promote == null) {
            throw createError(new ClassCastException("Cannot apply or [|] to types " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(left.actual) + "] and " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(right.actual) + "]."));
        }

        actual = promote;

        if (promote == def.class) {
            left.expected = left.actual;
            right.expected = right.actual;
            if (expected != null) {
                actual = expected;
            }
        } else {
            left.expected = promote;
            right.expected = promote;
        }

        left.cast();
        right.cast();
    }

    @Override
    BinaryMathNode write(ClassNode classNode) {
        BinaryMathNode binaryMathNode = new BinaryMathNode();

        binaryMathNode.setLeftNode(left.cast(left.write(classNode)));
        binaryMathNode.setRightNode(right.cast(right.write(classNode)));

        binaryMathNode.setLocation(location);
        binaryMathNode.setExpressionType(output.actual);
        binaryMathNode.setBinaryType(promote);
        binaryMathNode.setShiftType(shiftDistance);
        binaryMathNode.setOperation(operation);
        binaryMathNode.setCat(cat);
        binaryMathNode.setOriginallExplicit(originallyExplicit);

        return binaryMathNode;
    }

    @Override
    public String toString() {
        return singleLineToString(left, operation.symbol, right);
    }
}
