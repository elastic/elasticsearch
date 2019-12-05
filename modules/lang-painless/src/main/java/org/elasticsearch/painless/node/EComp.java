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
import org.elasticsearch.painless.ClassWriter;
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.ScriptRoot;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.def;
import org.objectweb.asm.Label;
import org.objectweb.asm.Type;

import java.util.Objects;
import java.util.Set;

import static org.elasticsearch.painless.WriterConstants.EQUALS;
import static org.elasticsearch.painless.WriterConstants.OBJECTS_TYPE;

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
    void write(ClassWriter classWriter, MethodWriter methodWriter, Globals globals) {
        methodWriter.writeDebugInfo(location);

        left.write(classWriter, methodWriter, globals);

        if (!right.isNull) {
            right.write(classWriter, methodWriter, globals);
        }

        Label jump = new Label();
        Label end = new Label();

        boolean eq = (operation == Operation.EQ || operation == Operation.EQR);
        boolean ne = (operation == Operation.NE || operation == Operation.NER);
        boolean lt  = operation == Operation.LT;
        boolean lte = operation == Operation.LTE;
        boolean gt  = operation == Operation.GT;
        boolean gte = operation == Operation.GTE;

        boolean writejump = true;

        Type type = MethodWriter.getType(promotedType);

        if (promotedType == void.class || promotedType == byte.class || promotedType == short.class || promotedType == char.class) {
            throw createError(new IllegalStateException("Illegal tree structure."));
        } else if (promotedType == boolean.class) {
            if (eq) methodWriter.ifCmp(type, MethodWriter.EQ, jump);
            else if (ne) methodWriter.ifCmp(type, MethodWriter.NE, jump);
            else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        } else if (promotedType == int.class || promotedType == long.class || promotedType == float.class || promotedType == double.class) {
            if (eq) methodWriter.ifCmp(type, MethodWriter.EQ, jump);
            else if (ne) methodWriter.ifCmp(type, MethodWriter.NE, jump);
            else if (lt) methodWriter.ifCmp(type, MethodWriter.LT, jump);
            else if (lte) methodWriter.ifCmp(type, MethodWriter.LE, jump);
            else if (gt) methodWriter.ifCmp(type, MethodWriter.GT, jump);
            else if (gte) methodWriter.ifCmp(type, MethodWriter.GE, jump);
            else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }

        } else if (promotedType == def.class) {
            Type booleanType = Type.getType(boolean.class);
            Type descriptor = Type.getMethodType(booleanType, MethodWriter.getType(left.actual), MethodWriter.getType(right.actual));

            if (eq) {
                if (right.isNull) {
                    methodWriter.ifNull(jump);
                } else if (!left.isNull && operation == Operation.EQ) {
                    methodWriter.invokeDefCall("eq", descriptor, DefBootstrap.BINARY_OPERATOR, DefBootstrap.OPERATOR_ALLOWS_NULL);
                    writejump = false;
                } else {
                    methodWriter.ifCmp(type, MethodWriter.EQ, jump);
                }
            } else if (ne) {
                if (right.isNull) {
                    methodWriter.ifNonNull(jump);
                } else if (!left.isNull && operation == Operation.NE) {
                    methodWriter.invokeDefCall("eq", descriptor, DefBootstrap.BINARY_OPERATOR, DefBootstrap.OPERATOR_ALLOWS_NULL);
                    methodWriter.ifZCmp(MethodWriter.EQ, jump);
                } else {
                    methodWriter.ifCmp(type, MethodWriter.NE, jump);
                }
            } else if (lt) {
                methodWriter.invokeDefCall("lt", descriptor, DefBootstrap.BINARY_OPERATOR, 0);
                writejump = false;
            } else if (lte) {
                methodWriter.invokeDefCall("lte", descriptor, DefBootstrap.BINARY_OPERATOR, 0);
                writejump = false;
            } else if (gt) {
                methodWriter.invokeDefCall("gt", descriptor, DefBootstrap.BINARY_OPERATOR, 0);
                writejump = false;
            } else if (gte) {
                methodWriter.invokeDefCall("gte", descriptor, DefBootstrap.BINARY_OPERATOR, 0);
                writejump = false;
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        } else {
            if (eq) {
                if (right.isNull) {
                    methodWriter.ifNull(jump);
                } else if (operation == Operation.EQ) {
                    methodWriter.invokeStatic(OBJECTS_TYPE, EQUALS);
                    writejump = false;
                } else {
                    methodWriter.ifCmp(type, MethodWriter.EQ, jump);
                }
            } else if (ne) {
                if (right.isNull) {
                    methodWriter.ifNonNull(jump);
                } else if (operation == Operation.NE) {
                    methodWriter.invokeStatic(OBJECTS_TYPE, EQUALS);
                    methodWriter.ifZCmp(MethodWriter.EQ, jump);
                } else {
                    methodWriter.ifCmp(type, MethodWriter.NE, jump);
                }
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        if (writejump) {
            methodWriter.push(false);
            methodWriter.goTo(end);
            methodWriter.mark(jump);
            methodWriter.push(true);
            methodWriter.mark(end);
        }
    }

    @Override
    public String toString() {
        return singleLineToString(left, operation.symbol, right);
    }
}
