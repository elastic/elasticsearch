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
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.Variables;

/**
 * Represents a binary math expression.
 */
public final class EBinary extends AExpression {

    final Operation operation;
    AExpression left;
    AExpression right;

    boolean cat = false;

    public EBinary(int line, int offset, String location, Operation operation, AExpression left, AExpression right) {
        super(line, offset, location);

        this.operation = operation;
        this.left = left;
        this.right = right;
    }

    @Override
    void analyze(Variables variables) {
        if (operation == Operation.MUL) {
            analyzeMul(variables);
        } else if (operation == Operation.DIV) {
            analyzeDiv(variables);
        } else if (operation == Operation.REM) {
            analyzeRem(variables);
        } else if (operation == Operation.ADD) {
            analyzeAdd(variables);
        } else if (operation == Operation.SUB) {
            analyzeSub(variables);
        } else if (operation == Operation.LSH) {
            analyzeLSH(variables);
        } else if (operation == Operation.RSH) {
            analyzeRSH(variables);
        } else if (operation == Operation.USH) {
            analyzeUSH(variables);
        } else if (operation == Operation.BWAND) {
            analyzeBWAnd(variables);
        } else if (operation == Operation.XOR) {
            analyzeXor(variables);
        } else if (operation == Operation.BWOR) {
            analyzeBWOr(variables);
        } else {
            throw new IllegalStateException(error("Illegal tree structure."));
        }
    }

    private void analyzeMul(Variables variables) {
        left.analyze(variables);
        right.analyze(variables);

        Type promote = AnalyzerCaster.promoteNumeric(left.actual, right.actual, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply multiply [*] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(variables);
        right = right.cast(variables);

        if (left.constant != null && right.constant != null) {
            Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant * (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant * (long)right.constant;
            } else if (sort == Sort.FLOAT) {
                constant = (float)left.constant * (float)right.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = (double)left.constant * (double)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    private void analyzeDiv(Variables variables) {
        left.analyze(variables);
        right.analyze(variables);

        Type promote = AnalyzerCaster.promoteNumeric(left.actual, right.actual, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply divide [/] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(variables);
        right = right.cast(variables);

        if (left.constant != null && right.constant != null) {
            Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant / (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant / (long)right.constant;
            } else if (sort == Sort.FLOAT) {
                constant = (float)left.constant / (float)right.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = (double)left.constant / (double)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    private void analyzeRem(Variables variables) {
        left.analyze(variables);
        right.analyze(variables);

        Type promote = AnalyzerCaster.promoteNumeric(left.actual, right.actual, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply remainder [%] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(variables);
        right = right.cast(variables);

        if (left.constant != null && right.constant != null) {
            Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant % (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant % (long)right.constant;
            } else if (sort == Sort.FLOAT) {
                constant = (float)left.constant % (float)right.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = (double)left.constant % (double)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    private void analyzeAdd(Variables variables) {
        left.analyze(variables);
        right.analyze(variables);

        Type promote = AnalyzerCaster.promoteAdd(left.actual, right.actual);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply add [+] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        Sort sort = promote.sort;

        if (sort == Sort.STRING) {
            left.expected = left.actual;

            if (left instanceof EBinary && ((EBinary)left).operation == Operation.ADD && left.actual.sort == Sort.STRING) {
                ((EBinary)left).cat = true;
            }

            right.expected = right.actual;

            if (right instanceof EBinary && ((EBinary)right).operation == Operation.ADD && right.actual.sort == Sort.STRING) {
                ((EBinary)right).cat = true;
            }
        } else {
            left.expected = promote;
            right.expected = promote;
        }

        left = left.cast(variables);
        right = right.cast(variables);

        if (left.constant != null && right.constant != null) {
            if (sort == Sort.INT) {
                constant = (int)left.constant + (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant + (long)right.constant;
            } else if (sort == Sort.FLOAT) {
                constant = (float)left.constant + (float)right.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = (double)left.constant + (double)right.constant;
            } else if (sort == Sort.STRING) {
                constant = "" + left.constant + right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    private void analyzeSub(Variables variables) {
        left.analyze(variables);
        right.analyze(variables);

        Type promote = AnalyzerCaster.promoteNumeric(left.actual, right.actual, true);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply subtract [-] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(variables);
        right = right.cast(variables);

        if (left.constant != null && right.constant != null) {
            Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant - (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant - (long)right.constant;
            } else if (sort == Sort.FLOAT) {
                constant = (float)left.constant - (float)right.constant;
            } else if (sort == Sort.DOUBLE) {
                constant = (double)left.constant - (double)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    private void analyzeLSH(Variables variables) {
        left.analyze(variables);
        right.analyze(variables);

        Type promote = AnalyzerCaster.promoteNumeric(left.actual, false);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply left shift [<<] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = Definition.INT_TYPE;
        right.explicit = true;

        left = left.cast(variables);
        right = right.cast(variables);

        if (left.constant != null && right.constant != null) {
            Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant << (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant << (int)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    private void analyzeRSH(Variables variables) {
        left.analyze(variables);
        right.analyze(variables);

        Type promote = AnalyzerCaster.promoteNumeric(left.actual, false);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply right shift [>>] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = Definition.INT_TYPE;
        right.explicit = true;

        left = left.cast(variables);
        right = right.cast(variables);

        if (left.constant != null && right.constant != null) {
            Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant >> (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant >> (int)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    private void analyzeUSH(Variables variables) {
        left.analyze(variables);
        right.analyze(variables);

        Type promote = AnalyzerCaster.promoteNumeric(left.actual, false);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply unsigned shift [>>>] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = Definition.INT_TYPE;
        right.explicit = true;

        left = left.cast(variables);
        right = right.cast(variables);

        if (left.constant != null && right.constant != null) {
            Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant >>> (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant >>> (int)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    private void analyzeBWAnd(Variables variables) {
        left.analyze(variables);
        right.analyze(variables);

        Type promote = AnalyzerCaster.promoteNumeric(left.actual, right.actual, false);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply and [&] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(variables);
        right = right.cast(variables);

        if (left.constant != null && right.constant != null) {
            Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant & (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant & (long)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    private void analyzeXor(Variables variables) {
        left.analyze(variables);
        right.analyze(variables);

        Type promote = AnalyzerCaster.promoteXor(left.actual, right.actual);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply xor [^] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(variables);
        right = right.cast(variables);

        if (left.constant != null && right.constant != null) {
            Sort sort = promote.sort;

            if (sort == Sort.BOOL) {
                constant = (boolean)left.constant ^ (boolean)right.constant;
            } else if (sort == Sort.INT) {
                constant = (int)left.constant ^ (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant ^ (long)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    private void analyzeBWOr(Variables variables) {
        left.analyze(variables);
        right.analyze(variables);

        Type promote = AnalyzerCaster.promoteNumeric(left.actual, right.actual, false);

        if (promote == null) {
            throw new ClassCastException(error("Cannot apply or [|] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promote;
        right.expected = promote;

        left = left.cast(variables);
        right = right.cast(variables);

        if (left.constant != null && right.constant != null) {
            Sort sort = promote.sort;

            if (sort == Sort.INT) {
                constant = (int)left.constant | (int)right.constant;
            } else if (sort == Sort.LONG) {
                constant = (long)left.constant | (long)right.constant;
            } else {
                throw new IllegalStateException(error("Illegal tree structure."));
            }
        }

        actual = promote;
    }

    @Override
    void write(MethodWriter writer) {
        writer.writeDebugInfo(offset);

        if (actual.sort == Sort.STRING && operation == Operation.ADD) {
            if (!cat) {
                writer.writeNewStrings();
            }

            left.write(writer);

            if (!(left instanceof EBinary) || ((EBinary)left).operation != Operation.ADD || left.actual.sort != Sort.STRING) {
                writer.writeAppendStrings(left.actual);
            }

            right.write(writer);

            if (!(right instanceof EBinary) || ((EBinary)right).operation != Operation.ADD || right.actual.sort != Sort.STRING) {
                writer.writeAppendStrings(right.actual);
            }

            if (!cat) {
                writer.writeToStrings();
            }
        } else {
            left.write(writer);
            right.write(writer);

            writer.writeBinaryInstruction(location, actual, operation);
        }

        writer.writeBranch(tru, fals);
    }
}
