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
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.ir.ComparisonNode;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.symbol.ScriptRoot;

import java.util.Objects;
import java.util.Set;

/**
 * Represents a comparison expression.
 */
public final class EComp extends AExpression {

    private final Operation operation;
    private AExpression left;
    private AExpression right;

    private Class<?> promotedType;

    public EComp(Location location, Operation operation, AExpression left, AExpression right) {
        super(location);

        this.operation = Objects.requireNonNull(operation);
        this.left = Objects.requireNonNull(left);
        this.right = Objects.requireNonNull(right);
    }

    @Override
    void extractVariables(Set<String> variables) {
        left.extractVariables(variables);
        right.extractVariables(variables);
    }

    @Override
    void analyze(ScriptRoot scriptRoot, Locals locals) {
        if (operation == Operation.EQ) {
            analyzeEq(scriptRoot, locals);
        } else if (operation == Operation.EQR) {
            analyzeEqR(scriptRoot, locals);
        } else if (operation == Operation.NE) {
            analyzeNE(scriptRoot, locals);
        } else if (operation == Operation.NER) {
            analyzeNER(scriptRoot, locals);
        } else if (operation == Operation.GTE) {
            analyzeGTE(scriptRoot, locals);
        } else if (operation == Operation.GT) {
            analyzeGT(scriptRoot, locals);
        } else if (operation == Operation.LTE) {
            analyzeLTE(scriptRoot, locals);
        } else if (operation == Operation.LT) {
            analyzeLT(scriptRoot, locals);
        } else {
            throw createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    private void analyzeEq(ScriptRoot scriptRoot, Locals variables) {
        left.analyze(scriptRoot, variables);
        right.analyze(scriptRoot, variables);

        promotedType = AnalyzerCaster.promoteEquality(left.actual, right.actual);

        if (promotedType == null) {
            throw createError(new ClassCastException("Cannot apply equals [==] to types " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(left.actual) + "] and " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(right.actual) + "]."));
        }

        if (promotedType == def.class) {
            left.expected = left.actual;
            right.expected = right.actual;
        } else {
            left.expected = promotedType;
            right.expected = promotedType;
        }

        left = left.cast(scriptRoot, variables);
        right = right.cast(scriptRoot, variables);

        if (left.isNull && right.isNull) {
            throw createError(new IllegalArgumentException("Extraneous comparison of null constants."));
        }

        if ((left.constant != null || left.isNull) && (right.constant != null || right.isNull)) {
            if (promotedType == boolean.class) {
                constant = (boolean)left.constant == (boolean)right.constant;
            } else if (promotedType == int.class) {
                constant = (int)left.constant == (int)right.constant;
            } else if (promotedType == long.class) {
                constant = (long)left.constant == (long)right.constant;
            } else if (promotedType == float.class) {
                constant = (float)left.constant == (float)right.constant;
            } else if (promotedType == double.class) {
                constant = (double)left.constant == (double)right.constant;
            } else if (!left.isNull) {
                constant = left.constant.equals(right.constant);
            } else if (!right.isNull) {
                constant = right.constant.equals(null);
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        actual = boolean.class;
    }

    private void analyzeEqR(ScriptRoot scriptRoot, Locals variables) {
        left.analyze(scriptRoot, variables);
        right.analyze(scriptRoot, variables);

        promotedType = AnalyzerCaster.promoteEquality(left.actual, right.actual);

        if (promotedType == null) {
            throw createError(new ClassCastException("Cannot apply reference equals [===] to types " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(left.actual) + "] and " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(right.actual) + "]."));
        }

        left.expected = promotedType;
        right.expected = promotedType;

        left = left.cast(scriptRoot, variables);
        right = right.cast(scriptRoot, variables);

        if (left.isNull && right.isNull) {
            throw createError(new IllegalArgumentException("Extraneous comparison of null constants."));
        }

        if ((left.constant != null || left.isNull) && (right.constant != null || right.isNull)) {
            if (promotedType == boolean.class) {
                constant = (boolean)left.constant == (boolean)right.constant;
            } else if (promotedType == int.class) {
                constant = (int)left.constant == (int)right.constant;
            } else if (promotedType == long.class) {
                constant = (long)left.constant == (long)right.constant;
            } else if (promotedType == float.class) {
                constant = (float)left.constant == (float)right.constant;
            } else if (promotedType == double.class) {
                constant = (double)left.constant == (double)right.constant;
            } else {
                constant = left.constant == right.constant;
            }
        }

        actual = boolean.class;
    }

    private void analyzeNE(ScriptRoot scriptRoot, Locals variables) {
        left.analyze(scriptRoot, variables);
        right.analyze(scriptRoot, variables);

        promotedType = AnalyzerCaster.promoteEquality(left.actual, right.actual);

        if (promotedType == null) {
            throw createError(new ClassCastException("Cannot apply not equals [!=] to types " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(left.actual) + "] and " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(right.actual) + "]."));
        }

        if (promotedType == def.class) {
            left.expected = left.actual;
            right.expected = right.actual;
        } else {
            left.expected = promotedType;
            right.expected = promotedType;
        }

        left = left.cast(scriptRoot, variables);
        right = right.cast(scriptRoot, variables);

        if (left.isNull && right.isNull) {
            throw createError(new IllegalArgumentException("Extraneous comparison of null constants."));
        }

        if ((left.constant != null || left.isNull) && (right.constant != null || right.isNull)) {
            if (promotedType == boolean.class) {
                constant = (boolean)left.constant != (boolean)right.constant;
            } else if (promotedType == int.class) {
                constant = (int)left.constant != (int)right.constant;
            } else if (promotedType == long.class) {
                constant = (long)left.constant != (long)right.constant;
            } else if (promotedType == float.class) {
                constant = (float)left.constant != (float)right.constant;
            } else if (promotedType == double.class) {
                constant = (double)left.constant != (double)right.constant;
            } else if (!left.isNull) {
                constant = !left.constant.equals(right.constant);
            } else if (!right.isNull) {
                constant = !right.constant.equals(null);
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        actual = boolean.class;
    }

    private void analyzeNER(ScriptRoot scriptRoot, Locals variables) {
        left.analyze(scriptRoot, variables);
        right.analyze(scriptRoot, variables);

        promotedType = AnalyzerCaster.promoteEquality(left.actual, right.actual);

        if (promotedType == null) {
            throw createError(new ClassCastException("Cannot apply reference not equals [!==] to types " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(left.actual) + "] and " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(right.actual) + "]."));
        }

        left.expected = promotedType;
        right.expected = promotedType;

        left = left.cast(scriptRoot, variables);
        right = right.cast(scriptRoot, variables);

        if (left.isNull && right.isNull) {
            throw createError(new IllegalArgumentException("Extraneous comparison of null constants."));
        }

        if ((left.constant != null || left.isNull) && (right.constant != null || right.isNull)) {
            if (promotedType == boolean.class) {
                constant = (boolean)left.constant != (boolean)right.constant;
            } else if (promotedType == int.class) {
                constant = (int)left.constant != (int)right.constant;
            } else if (promotedType == long.class) {
                constant = (long)left.constant != (long)right.constant;
            } else if (promotedType == float.class) {
                constant = (float)left.constant != (float)right.constant;
            } else if (promotedType == double.class) {
                constant = (double)left.constant != (double)right.constant;
            } else {
                constant = left.constant != right.constant;
            }
        }

        actual = boolean.class;
    }

    private void analyzeGTE(ScriptRoot scriptRoot, Locals variables) {
        left.analyze(scriptRoot, variables);
        right.analyze(scriptRoot, variables);

        promotedType = AnalyzerCaster.promoteNumeric(left.actual, right.actual, true);

        if (promotedType == null) {
            throw createError(new ClassCastException("Cannot apply greater than or equals [>=] to types " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(left.actual) + "] and " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(right.actual) + "]."));
        }

        if (promotedType == def.class) {
            left.expected = left.actual;
            right.expected = right.actual;
        } else {
            left.expected = promotedType;
            right.expected = promotedType;
        }

        left = left.cast(scriptRoot, variables);
        right = right.cast(scriptRoot, variables);

        if (left.constant != null && right.constant != null) {
            if (promotedType == int.class) {
                constant = (int)left.constant >= (int)right.constant;
            } else if (promotedType == long.class) {
                constant = (long)left.constant >= (long)right.constant;
            } else if (promotedType == float.class) {
                constant = (float)left.constant >= (float)right.constant;
            } else if (promotedType == double.class) {
                constant = (double)left.constant >= (double)right.constant;
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        actual = boolean.class;
    }

    private void analyzeGT(ScriptRoot scriptRoot, Locals variables) {
        left.analyze(scriptRoot, variables);
        right.analyze(scriptRoot, variables);

        promotedType = AnalyzerCaster.promoteNumeric(left.actual, right.actual, true);

        if (promotedType == null) {
            throw createError(new ClassCastException("Cannot apply greater than [>] to types " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(left.actual) + "] and " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(right.actual) + "]."));
        }

        if (promotedType == def.class) {
            left.expected = left.actual;
            right.expected = right.actual;
        } else {
            left.expected = promotedType;
            right.expected = promotedType;
        }

        left = left.cast(scriptRoot, variables);
        right = right.cast(scriptRoot, variables);

        if (left.constant != null && right.constant != null) {
            if (promotedType == int.class) {
                constant = (int)left.constant > (int)right.constant;
            } else if (promotedType == long.class) {
                constant = (long)left.constant > (long)right.constant;
            } else if (promotedType == float.class) {
                constant = (float)left.constant > (float)right.constant;
            } else if (promotedType == double.class) {
                constant = (double)left.constant > (double)right.constant;
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        actual = boolean.class;
    }

    private void analyzeLTE(ScriptRoot scriptRoot, Locals variables) {
        left.analyze(scriptRoot, variables);
        right.analyze(scriptRoot, variables);

        promotedType = AnalyzerCaster.promoteNumeric(left.actual, right.actual, true);

        if (promotedType == null) {
            throw createError(new ClassCastException("Cannot apply less than or equals [<=] to types " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(left.actual) + "] and " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(right.actual) + "]."));
        }

        if (promotedType == def.class) {
            left.expected = left.actual;
            right.expected = right.actual;
        } else {
            left.expected = promotedType;
            right.expected = promotedType;
        }

        left = left.cast(scriptRoot, variables);
        right = right.cast(scriptRoot, variables);

        if (left.constant != null && right.constant != null) {
            if (promotedType == int.class) {
                constant = (int)left.constant <= (int)right.constant;
            } else if (promotedType == long.class) {
                constant = (long)left.constant <= (long)right.constant;
            } else if (promotedType == float.class) {
                constant = (float)left.constant <= (float)right.constant;
            } else if (promotedType == double.class) {
                constant = (double)left.constant <= (double)right.constant;
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        actual = boolean.class;
    }

    private void analyzeLT(ScriptRoot scriptRoot, Locals variables) {
        left.analyze(scriptRoot, variables);
        right.analyze(scriptRoot, variables);

        promotedType = AnalyzerCaster.promoteNumeric(left.actual, right.actual, true);

        if (promotedType == null) {
            throw createError(new ClassCastException("Cannot apply less than [>=] to types " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(left.actual) + "] and " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(right.actual) + "]."));
        }

        if (promotedType == def.class) {
            left.expected = left.actual;
            right.expected = right.actual;
        } else {
            left.expected = promotedType;
            right.expected = promotedType;
        }

        left = left.cast(scriptRoot, variables);
        right = right.cast(scriptRoot, variables);

        if (left.constant != null && right.constant != null) {
            if (promotedType == int.class) {
                constant = (int)left.constant < (int)right.constant;
            } else if (promotedType == long.class) {
                constant = (long)left.constant < (long)right.constant;
            } else if (promotedType == float.class) {
                constant = (float)left.constant < (float)right.constant;
            } else if (promotedType == double.class) {
                constant = (double)left.constant < (double)right.constant;
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        actual = boolean.class;
    }

    @Override
    ComparisonNode write() {
        ComparisonNode comparisonNode = new ComparisonNode();

        comparisonNode.setLeftNode(left.write());
        comparisonNode.setRightNode(right.write());

        comparisonNode.setLocation(location);
        comparisonNode.setExpressionType(actual);
        comparisonNode.setComparisonType(promotedType);
        comparisonNode.setOperation(operation);

        return comparisonNode;
    }

    @Override
    public String toString() {
        return singleLineToString(left, operation.symbol, right);
    }
}
