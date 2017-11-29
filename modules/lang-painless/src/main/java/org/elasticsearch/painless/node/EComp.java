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
import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.DefBootstrap;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.Locals;
import org.objectweb.asm.Label;

import java.util.Objects;
import java.util.Set;

import org.elasticsearch.painless.MethodWriter;

import static org.elasticsearch.painless.WriterConstants.OBJECTS_TYPE;
import static org.elasticsearch.painless.WriterConstants.EQUALS;

/**
 * Represents a comparison expression.
 */
public final class EComp extends AExpression {

    private final Operation operation;
    private AExpression left;
    private AExpression right;

    private Type promotedType;

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
    void analyze(Locals locals) {
        if (operation == Operation.EQ) {
            analyzeEq(locals);
        } else if (operation == Operation.EQR) {
            analyzeEqR(locals);
        } else if (operation == Operation.NE) {
            analyzeNE(locals);
        } else if (operation == Operation.NER) {
            analyzeNER(locals);
        } else if (operation == Operation.GTE) {
            analyzeGTE(locals);
        } else if (operation == Operation.GT) {
            analyzeGT(locals);
        } else if (operation == Operation.LTE) {
            analyzeLTE(locals);
        } else if (operation == Operation.LT) {
            analyzeLT(locals);
        } else {
            throw createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    private void analyzeEq(Locals variables) {
        left.analyze(variables);
        right.analyze(variables);

        promotedType = variables.getDefinition().caster.promoteEquality(left.actual, right.actual);

        if (promotedType == null) {
            throw createError(new ClassCastException("Cannot apply equals [==] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        if (promotedType.dynamic) {
            left.expected = left.actual;
            right.expected = right.actual;
        } else {
            left.expected = promotedType;
            right.expected = promotedType;
        }

        left = left.cast(variables);
        right = right.cast(variables);

        if (left.isNull && right.isNull) {
            throw createError(new IllegalArgumentException("Extraneous comparison of null constants."));
        }

        if ((left.constant != null || left.isNull) && (right.constant != null || right.isNull)) {
            Class<?> sort = promotedType.clazz;

            if (sort == boolean.class) {
                constant = (boolean)left.constant == (boolean)right.constant;
            } else if (sort == int.class) {
                constant = (int)left.constant == (int)right.constant;
            } else if (sort == long.class) {
                constant = (long)left.constant == (long)right.constant;
            } else if (sort == float.class) {
                constant = (float)left.constant == (float)right.constant;
            } else if (sort == double.class) {
                constant = (double)left.constant == (double)right.constant;
            } else if (!left.isNull) {
                constant = left.constant.equals(right.constant);
            } else if (!right.isNull) {
                constant = right.constant.equals(null);
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        actual = variables.getDefinition().booleanType;
    }

    private void analyzeEqR(Locals variables) {
        left.analyze(variables);
        right.analyze(variables);

        promotedType = variables.getDefinition().caster.promoteEquality(left.actual, right.actual);

        if (promotedType == null) {
            throw createError(new ClassCastException("Cannot apply reference equals [===] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promotedType;
        right.expected = promotedType;

        left = left.cast(variables);
        right = right.cast(variables);

        if (left.isNull && right.isNull) {
            throw createError(new IllegalArgumentException("Extraneous comparison of null constants."));
        }

        if ((left.constant != null || left.isNull) && (right.constant != null || right.isNull)) {
            Class<?> sort = promotedType.clazz;

            if (sort == boolean.class) {
                constant = (boolean)left.constant == (boolean)right.constant;
            } else if (sort == int.class) {
                constant = (int)left.constant == (int)right.constant;
            } else if (sort == long.class) {
                constant = (long)left.constant == (long)right.constant;
            } else if (sort == float.class) {
                constant = (float)left.constant == (float)right.constant;
            } else if (sort == double.class) {
                constant = (double)left.constant == (double)right.constant;
            } else {
                constant = left.constant == right.constant;
            }
        }

        actual = variables.getDefinition().booleanType;
    }

    private void analyzeNE(Locals variables) {
        left.analyze(variables);
        right.analyze(variables);

        promotedType = variables.getDefinition().caster.promoteEquality(left.actual, right.actual);

        if (promotedType == null) {
            throw createError(new ClassCastException("Cannot apply not equals [!=] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        if (promotedType.dynamic) {
            left.expected = left.actual;
            right.expected = right.actual;
        } else {
            left.expected = promotedType;
            right.expected = promotedType;
        }

        left = left.cast(variables);
        right = right.cast(variables);

        if (left.isNull && right.isNull) {
            throw createError(new IllegalArgumentException("Extraneous comparison of null constants."));
        }

        if ((left.constant != null || left.isNull) && (right.constant != null || right.isNull)) {
            Class<?> sort = promotedType.clazz;

            if (sort == boolean.class) {
                constant = (boolean)left.constant != (boolean)right.constant;
            } else if (sort == int.class) {
                constant = (int)left.constant != (int)right.constant;
            } else if (sort == long.class) {
                constant = (long)left.constant != (long)right.constant;
            } else if (sort == float.class) {
                constant = (float)left.constant != (float)right.constant;
            } else if (sort == double.class) {
                constant = (double)left.constant != (double)right.constant;
            } else if (!left.isNull) {
                constant = !left.constant.equals(right.constant);
            } else if (!right.isNull) {
                constant = !right.constant.equals(null);
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        actual = variables.getDefinition().booleanType;
    }

    private void analyzeNER(Locals variables) {
        left.analyze(variables);
        right.analyze(variables);

        promotedType = variables.getDefinition().caster.promoteEquality(left.actual, right.actual);

        if (promotedType == null) {
            throw createError(new ClassCastException("Cannot apply reference not equals [!==] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        left.expected = promotedType;
        right.expected = promotedType;

        left = left.cast(variables);
        right = right.cast(variables);

        if (left.isNull && right.isNull) {
            throw createError(new IllegalArgumentException("Extraneous comparison of null constants."));
        }

        if ((left.constant != null || left.isNull) && (right.constant != null || right.isNull)) {
            Class<?> sort = promotedType.clazz;

            if (sort == boolean.class) {
                constant = (boolean)left.constant != (boolean)right.constant;
            } else if (sort == int.class) {
                constant = (int)left.constant != (int)right.constant;
            } else if (sort == long.class) {
                constant = (long)left.constant != (long)right.constant;
            } else if (sort == float.class) {
                constant = (float)left.constant != (float)right.constant;
            } else if (sort == double.class) {
                constant = (double)left.constant != (double)right.constant;
            } else {
                constant = left.constant != right.constant;
            }
        }

        actual = variables.getDefinition().booleanType;
    }

    private void analyzeGTE(Locals variables) {
        left.analyze(variables);
        right.analyze(variables);

        promotedType = variables.getDefinition().caster.promoteNumeric(left.actual, right.actual, true);

        if (promotedType == null) {
            throw createError(new ClassCastException("Cannot apply greater than or equals [>=] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        if (promotedType.dynamic) {
            left.expected = left.actual;
            right.expected = right.actual;
        } else {
            left.expected = promotedType;
            right.expected = promotedType;
        }

        left = left.cast(variables);
        right = right.cast(variables);

        if (left.constant != null && right.constant != null) {
            Class<?> sort = promotedType.clazz;

            if (sort == int.class) {
                constant = (int)left.constant >= (int)right.constant;
            } else if (sort == long.class) {
                constant = (long)left.constant >= (long)right.constant;
            } else if (sort == float.class) {
                constant = (float)left.constant >= (float)right.constant;
            } else if (sort == double.class) {
                constant = (double)left.constant >= (double)right.constant;
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        actual = variables.getDefinition().booleanType;
    }

    private void analyzeGT(Locals variables) {
        left.analyze(variables);
        right.analyze(variables);

        promotedType = variables.getDefinition().caster.promoteNumeric(left.actual, right.actual, true);

        if (promotedType == null) {
            throw createError(new ClassCastException("Cannot apply greater than [>] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        if (promotedType.dynamic) {
            left.expected = left.actual;
            right.expected = right.actual;
        } else {
            left.expected = promotedType;
            right.expected = promotedType;
        }

        left = left.cast(variables);
        right = right.cast(variables);

        if (left.constant != null && right.constant != null) {
            Class<?> sort = promotedType.clazz;

            if (sort == int.class) {
                constant = (int)left.constant > (int)right.constant;
            } else if (sort == long.class) {
                constant = (long)left.constant > (long)right.constant;
            } else if (sort == float.class) {
                constant = (float)left.constant > (float)right.constant;
            } else if (sort == double.class) {
                constant = (double)left.constant > (double)right.constant;
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        actual = variables.getDefinition().booleanType;
    }

    private void analyzeLTE(Locals variables) {
        left.analyze(variables);
        right.analyze(variables);

        promotedType = variables.getDefinition().caster.promoteNumeric(left.actual, right.actual, true);

        if (promotedType == null) {
            throw createError(new ClassCastException("Cannot apply less than or equals [<=] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        if (promotedType.dynamic) {
            left.expected = left.actual;
            right.expected = right.actual;
        } else {
            left.expected = promotedType;
            right.expected = promotedType;
        }

        left = left.cast(variables);
        right = right.cast(variables);

        if (left.constant != null && right.constant != null) {
            Class<?> sort = promotedType.clazz;

            if (sort == int.class) {
                constant = (int)left.constant <= (int)right.constant;
            } else if (sort == long.class) {
                constant = (long)left.constant <= (long)right.constant;
            } else if (sort == float.class) {
                constant = (float)left.constant <= (float)right.constant;
            } else if (sort == double.class) {
                constant = (double)left.constant <= (double)right.constant;
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        actual = variables.getDefinition().booleanType;
    }

    private void analyzeLT(Locals variables) {
        left.analyze(variables);
        right.analyze(variables);

        promotedType = variables.getDefinition().caster.promoteNumeric(left.actual, right.actual, true);

        if (promotedType == null) {
            throw createError(new ClassCastException("Cannot apply less than [>=] to types " +
                "[" + left.actual.name + "] and [" + right.actual.name + "]."));
        }

        if (promotedType.dynamic) {
            left.expected = left.actual;
            right.expected = right.actual;
        } else {
            left.expected = promotedType;
            right.expected = promotedType;
        }

        left = left.cast(variables);
        right = right.cast(variables);

        if (left.constant != null && right.constant != null) {
            Class<?> sort = promotedType.clazz;

            if (sort == int.class) {
                constant = (int)left.constant < (int)right.constant;
            } else if (sort == long.class) {
                constant = (long)left.constant < (long)right.constant;
            } else if (sort == float.class) {
                constant = (float)left.constant < (float)right.constant;
            } else if (sort == double.class) {
                constant = (double)left.constant < (double)right.constant;
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        actual = variables.getDefinition().booleanType;
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        left.write(writer, globals);

        if (!right.isNull) {
            right.write(writer, globals);
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

        Class<?> sort = promotedType.clazz;

        if (sort == void.class || sort == byte.class || sort == short.class || sort == char.class) {
            throw createError(new IllegalStateException("Illegal tree structure."));
        } else if (sort == boolean.class) {
            if (eq) writer.ifCmp(promotedType.type, MethodWriter.EQ, jump);
            else if (ne) writer.ifCmp(promotedType.type, MethodWriter.NE, jump);
            else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        } else if (sort == int.class || sort == long.class || sort == float.class || sort == double.class) {
            if (eq) writer.ifCmp(promotedType.type, MethodWriter.EQ, jump);
            else if (ne) writer.ifCmp(promotedType.type, MethodWriter.NE, jump);
            else if (lt) writer.ifCmp(promotedType.type, MethodWriter.LT, jump);
            else if (lte) writer.ifCmp(promotedType.type, MethodWriter.LE, jump);
            else if (gt) writer.ifCmp(promotedType.type, MethodWriter.GT, jump);
            else if (gte) writer.ifCmp(promotedType.type, MethodWriter.GE, jump);
            else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }

        } else if (promotedType.dynamic) {
            org.objectweb.asm.Type booleanType = org.objectweb.asm.Type.getType(boolean.class);
            org.objectweb.asm.Type descriptor = org.objectweb.asm.Type.getMethodType(booleanType, left.actual.type, right.actual.type);

            if (eq) {
                if (right.isNull) {
                    writer.ifNull(jump);
                } else if (!left.isNull && operation == Operation.EQ) {
                    writer.invokeDefCall("eq", descriptor, DefBootstrap.BINARY_OPERATOR, DefBootstrap.OPERATOR_ALLOWS_NULL);
                    writejump = false;
                } else {
                    writer.ifCmp(promotedType.type, MethodWriter.EQ, jump);
                }
            } else if (ne) {
                if (right.isNull) {
                    writer.ifNonNull(jump);
                } else if (!left.isNull && operation == Operation.NE) {
                    writer.invokeDefCall("eq", descriptor, DefBootstrap.BINARY_OPERATOR, DefBootstrap.OPERATOR_ALLOWS_NULL);
                    writer.ifZCmp(MethodWriter.EQ, jump);
                } else {
                    writer.ifCmp(promotedType.type, MethodWriter.NE, jump);
                }
            } else if (lt) {
                writer.invokeDefCall("lt", descriptor, DefBootstrap.BINARY_OPERATOR, 0);
                writejump = false;
            } else if (lte) {
                writer.invokeDefCall("lte", descriptor, DefBootstrap.BINARY_OPERATOR, 0);
                writejump = false;
            } else if (gt) {
                writer.invokeDefCall("gt", descriptor, DefBootstrap.BINARY_OPERATOR, 0);
                writejump = false;
            } else if (gte) {
                writer.invokeDefCall("gte", descriptor, DefBootstrap.BINARY_OPERATOR, 0);
                writejump = false;
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        } else {
            if (eq) {
                if (right.isNull) {
                    writer.ifNull(jump);
                } else if (operation == Operation.EQ) {
                    writer.invokeStatic(OBJECTS_TYPE, EQUALS);
                    writejump = false;
                } else {
                    writer.ifCmp(promotedType.type, MethodWriter.EQ, jump);
                }
            } else if (ne) {
                if (right.isNull) {
                    writer.ifNonNull(jump);
                } else if (operation == Operation.NE) {
                    writer.invokeStatic(OBJECTS_TYPE, EQUALS);
                    writer.ifZCmp(MethodWriter.EQ, jump);
                } else {
                    writer.ifCmp(promotedType.type, MethodWriter.NE, jump);
                }
            } else {
                throw createError(new IllegalStateException("Illegal tree structure."));
            }
        }

        if (writejump) {
            writer.push(false);
            writer.goTo(end);
            writer.mark(jump);
            writer.push(true);
            writer.mark(end);
        }
    }

    @Override
    public String toString() {
        return singleLineToString(left, operation.symbol, right);
    }
}
