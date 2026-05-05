/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless.phase;

import org.elasticsearch.core.Strings;
import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.ir.BinaryMathNode;
import org.elasticsearch.painless.ir.BooleanNode;
import org.elasticsearch.painless.ir.CastNode;
import org.elasticsearch.painless.ir.ComparisonNode;
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.InvokeCallMemberNode;
import org.elasticsearch.painless.ir.NullNode;
import org.elasticsearch.painless.ir.StringConcatenationNode;
import org.elasticsearch.painless.ir.UnaryMathNode;
import org.elasticsearch.painless.lookup.PainlessInstanceBinding;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.spi.annotation.CompileTimeOnlyAnnotation;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.function.Consumer;

/**
 * This optimization pass will perform the specified operation on two leafs nodes if they are both
 * constants. The additional overrides for visiting ir nodes in this class are required whenever
 * there is a child node that is an expression. The structure of the tree does not have a way
 * for a child node to introspect into its parent node, so to replace itself the parent node
 * must pass the child node's particular set method as method reference.
 */
public class DefaultConstantFoldingOptimizationPhase extends IRExpressionModifyingVisitor {

    private static IllegalStateException unaryError(String type, String operation, String constant) {
        return new IllegalStateException(
            Strings.format(
                "constant folding error: unexpected type [%s] for unary operation [%s] on constant [%s]",
                type,
                operation,
                constant
            )
        );
    }

    private static IllegalStateException binaryError(String type, String operation, String constant1, String constant2) {
        return error(type, "binary", operation, constant1, constant2);
    }

    private static IllegalStateException booleanError(String type, String operation, String constant1, String constant2) {
        return error(type, "boolean", operation, constant1, constant2);
    }

    private static IllegalStateException comparisonError(String type, String operation, String constant1, String constant2) {
        return error(type, "comparison", operation, constant1, constant2);
    }

    private static IllegalStateException error(String type, String opType, String operation, String constant1, String constant2) {
        return new IllegalStateException(
            Strings.format(
                "constant folding error: unexpected type [%s] for %s operation [%s] on constants [%s] and [%s]",
                type,
                opType,
                operation,
                constant1,
                constant2
            )
        );
    }

    @Override
    public void visitUnaryMath(UnaryMathNode irUnaryMathNode, Consumer<ExpressionNode> scope) {
        irUnaryMathNode.getChildNode().visit(this, irUnaryMathNode::setChildNode);

        if (irUnaryMathNode.getChildNode() instanceof ConstantNode) {
            ConstantNode irConstantNode = (ConstantNode) irUnaryMathNode.getChildNode();
            Object constantValue = irConstantNode.getConstant();
            Operation operation = irUnaryMathNode.getOperation();
            Class<?> type = irUnaryMathNode.getExpressionType();

            if (operation == Operation.SUB) {
                if (type == int.class) {
                    irConstantNode.setConstant(-(int) constantValue);
                } else if (type == long.class) {
                    irConstantNode.setConstant(-(long) constantValue);
                } else if (type == float.class) {
                    irConstantNode.setConstant(-(float) constantValue);
                } else if (type == double.class) {
                    irConstantNode.setConstant(-(double) constantValue);
                } else {
                    throw irUnaryMathNode.getLocation()
                        .createError(
                            unaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irConstantNode.getConstant().toString()
                            )
                        );
                }

                scope.accept(irConstantNode);
            } else if (operation == Operation.BWNOT) {
                if (type == int.class) {
                    irConstantNode.setConstant(~(int) constantValue);
                } else if (type == long.class) {
                    irConstantNode.setConstant(~(long) constantValue);
                } else {
                    throw irUnaryMathNode.getLocation()
                        .createError(
                            unaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irConstantNode.getConstant().toString()
                            )
                        );
                }

                scope.accept(irConstantNode);
            } else if (operation == Operation.NOT) {
                if (type == boolean.class) {
                    irConstantNode.setConstant(((boolean) constantValue) == false);
                } else {
                    throw irUnaryMathNode.getLocation()
                        .createError(
                            unaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irConstantNode.getConstant().toString()
                            )
                        );
                }

                scope.accept(irConstantNode);
            } else if (operation == Operation.ADD) {
                scope.accept(irConstantNode);
            }
        }
    }

    @Override
    public void visitBinaryMath(BinaryMathNode irBinaryMathNode, Consumer<ExpressionNode> scope) {
        irBinaryMathNode.getLeftNode().visit(this, irBinaryMathNode::setLeftNode);
        irBinaryMathNode.getRightNode().visit(this, irBinaryMathNode::setRightNode);

        if (irBinaryMathNode.getLeftNode() instanceof ConstantNode && irBinaryMathNode.getRightNode() instanceof ConstantNode) {
            ConstantNode irLeftConstantNode = (ConstantNode) irBinaryMathNode.getLeftNode();
            ConstantNode irRightConstantNode = (ConstantNode) irBinaryMathNode.getRightNode();
            Object leftConstantValue = irLeftConstantNode.getConstant();
            Object rightConstantValue = irRightConstantNode.getConstant();
            Operation operation = irBinaryMathNode.getOperation();
            Class<?> type = irBinaryMathNode.getExpressionType();

            if (operation == Operation.MUL) {
                if (type == int.class) {
                    irLeftConstantNode.setConstant((int) leftConstantValue * (int) rightConstantValue);
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long) leftConstantValue * (long) rightConstantValue);
                } else if (type == float.class) {
                    irLeftConstantNode.setConstant((float) leftConstantValue * (float) rightConstantValue);
                } else if (type == double.class) {
                    irLeftConstantNode.setConstant((double) leftConstantValue * (double) rightConstantValue);
                } else {
                    throw irBinaryMathNode.getLocation()
                        .createError(
                            binaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getConstant().toString(),
                                irRightConstantNode.getConstant().toString()
                            )
                        );
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.DIV) {
                try {
                    if (type == int.class) {
                        irLeftConstantNode.setConstant((int) leftConstantValue / (int) rightConstantValue);
                    } else if (type == long.class) {
                        irLeftConstantNode.setConstant((long) leftConstantValue / (long) rightConstantValue);
                    } else if (type == float.class) {
                        irLeftConstantNode.setConstant((float) leftConstantValue / (float) rightConstantValue);
                    } else if (type == double.class) {
                        irLeftConstantNode.setConstant((double) leftConstantValue / (double) rightConstantValue);
                    } else {
                        throw irBinaryMathNode.getLocation()
                            .createError(
                                binaryError(
                                    PainlessLookupUtility.typeToCanonicalTypeName(type),
                                    operation.symbol,
                                    irLeftConstantNode.getConstant().toString(),
                                    irRightConstantNode.getConstant().toString()
                                )
                            );
                    }
                } catch (ArithmeticException ae) {
                    throw irBinaryMathNode.getLocation().createError(ae);
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.REM) {
                try {
                    if (type == int.class) {
                        irLeftConstantNode.setConstant((int) leftConstantValue % (int) rightConstantValue);
                    } else if (type == long.class) {
                        irLeftConstantNode.setConstant((long) leftConstantValue % (long) rightConstantValue);
                    } else if (type == float.class) {
                        irLeftConstantNode.setConstant((float) leftConstantValue % (float) rightConstantValue);
                    } else if (type == double.class) {
                        irLeftConstantNode.setConstant((double) leftConstantValue % (double) rightConstantValue);
                    } else {
                        throw irBinaryMathNode.getLocation()
                            .createError(
                                binaryError(
                                    PainlessLookupUtility.typeToCanonicalTypeName(type),
                                    operation.symbol,
                                    irLeftConstantNode.getConstant().toString(),
                                    irRightConstantNode.getConstant().toString()
                                )
                            );
                    }
                } catch (ArithmeticException ae) {
                    throw irBinaryMathNode.getLocation().createError(ae);
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.ADD) {
                if (type == int.class) {
                    irLeftConstantNode.setConstant((int) leftConstantValue + (int) rightConstantValue);
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long) leftConstantValue + (long) rightConstantValue);
                } else if (type == float.class) {
                    irLeftConstantNode.setConstant((float) leftConstantValue + (float) rightConstantValue);
                } else if (type == double.class) {
                    irLeftConstantNode.setConstant((double) leftConstantValue + (double) rightConstantValue);
                } else {
                    throw irBinaryMathNode.getLocation()
                        .createError(
                            binaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getConstant().toString(),
                                irRightConstantNode.getConstant().toString()
                            )
                        );
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.SUB) {
                if (type == int.class) {
                    irLeftConstantNode.setConstant((int) leftConstantValue - (int) rightConstantValue);
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long) leftConstantValue - (long) rightConstantValue);
                } else if (type == float.class) {
                    irLeftConstantNode.setConstant((float) leftConstantValue - (float) rightConstantValue);
                } else if (type == double.class) {
                    irLeftConstantNode.setConstant((double) leftConstantValue - (double) rightConstantValue);
                } else {
                    throw irBinaryMathNode.getLocation()
                        .createError(
                            binaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getConstant().toString(),
                                irRightConstantNode.getConstant().toString()
                            )
                        );
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.LSH) {
                if (type == int.class) {
                    irLeftConstantNode.setConstant((int) leftConstantValue << (int) rightConstantValue);
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long) leftConstantValue << (int) rightConstantValue);
                } else {
                    throw irBinaryMathNode.getLocation()
                        .createError(
                            binaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getConstant().toString(),
                                irRightConstantNode.getConstant().toString()
                            )
                        );
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.RSH) {
                if (type == int.class) {
                    irLeftConstantNode.setConstant((int) leftConstantValue >> (int) rightConstantValue);
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long) leftConstantValue >> (int) rightConstantValue);
                } else {
                    throw irBinaryMathNode.getLocation()
                        .createError(
                            binaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getConstant().toString(),
                                irRightConstantNode.getConstant().toString()
                            )
                        );
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.USH) {
                if (type == int.class) {
                    irLeftConstantNode.setConstant((int) leftConstantValue >>> (int) rightConstantValue);
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long) leftConstantValue >>> (int) rightConstantValue);
                } else {
                    throw irBinaryMathNode.getLocation()
                        .createError(
                            binaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getConstant().toString(),
                                irRightConstantNode.getConstant().toString()
                            )
                        );
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.BWAND) {
                if (type == int.class) {
                    irLeftConstantNode.setConstant((int) leftConstantValue & (int) rightConstantValue);
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long) leftConstantValue & (long) rightConstantValue);
                } else {
                    throw irBinaryMathNode.getLocation()
                        .createError(
                            binaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getConstant().toString(),
                                irRightConstantNode.getConstant().toString()
                            )
                        );
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.XOR) {
                if (type == boolean.class) {
                    irLeftConstantNode.setConstant((boolean) leftConstantValue ^ (boolean) rightConstantValue);
                } else if (type == int.class) {
                    irLeftConstantNode.setConstant((int) leftConstantValue ^ (int) rightConstantValue);
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long) leftConstantValue ^ (long) rightConstantValue);
                } else {
                    throw irBinaryMathNode.getLocation()
                        .createError(
                            binaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getConstant().toString(),
                                irRightConstantNode.getConstant().toString()
                            )
                        );
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.BWOR) {
                if (type == int.class) {
                    irLeftConstantNode.setConstant((int) leftConstantValue | (int) rightConstantValue);
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long) leftConstantValue | (long) rightConstantValue);
                } else {
                    throw irBinaryMathNode.getLocation()
                        .createError(
                            binaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getConstant().toString(),
                                irRightConstantNode.getConstant().toString()
                            )
                        );
                }

                scope.accept(irLeftConstantNode);
            }
        }
    }

    @Override
    public void visitStringConcatenation(StringConcatenationNode irStringConcatenationNode, Consumer<ExpressionNode> scope) {
        irStringConcatenationNode.getArgumentNodes().get(0).visit(this, (e) -> irStringConcatenationNode.getArgumentNodes().set(0, e));

        int i = 0;

        while (i < irStringConcatenationNode.getArgumentNodes().size() - 1) {
            ExpressionNode irLeftNode = irStringConcatenationNode.getArgumentNodes().get(i);
            ExpressionNode irRightNode = irStringConcatenationNode.getArgumentNodes().get(i + 1);

            int j = i;
            irRightNode.visit(this, (e) -> irStringConcatenationNode.getArgumentNodes().set(j + 1, e));

            if (irLeftNode instanceof ConstantNode irLeftConstant && irRightNode instanceof ConstantNode irRightConstant) {
                irLeftConstant.setConstant("" + irLeftConstant.getConstant() + irRightConstant.getConstant());
                irLeftConstant.setExpressionType(String.class);
                irStringConcatenationNode.getArgumentNodes().remove(i + 1);
            } else if (irLeftNode instanceof NullNode && irRightNode instanceof ConstantNode irRightConstant) {
                irRightConstant.setConstant("" + null + irRightConstant.getConstant());
                irRightConstant.setExpressionType(String.class);
                irStringConcatenationNode.getArgumentNodes().remove(i);
            } else if (irLeftNode instanceof ConstantNode irLeftConstant && irRightNode instanceof NullNode) {
                irLeftConstant.setConstant("" + irLeftConstant.getConstant() + null);
                irLeftConstant.setExpressionType(String.class);
                irStringConcatenationNode.getArgumentNodes().remove(i + 1);
            } else if (irLeftNode instanceof NullNode && irRightNode instanceof NullNode) {
                ConstantNode irConstantNode = new ConstantNode(irLeftNode.getLocation());
                irConstantNode.setConstant("" + null + null);
                irConstantNode.setExpressionType(String.class);
                irStringConcatenationNode.getArgumentNodes().set(i, irConstantNode);
                irStringConcatenationNode.getArgumentNodes().remove(i + 1);
            } else {
                i++;
            }
        }

        if (irStringConcatenationNode.getArgumentNodes().size() == 1) {
            ExpressionNode irArgumentNode = irStringConcatenationNode.getArgumentNodes().get(0);

            if (irArgumentNode instanceof ConstantNode) {
                scope.accept(irArgumentNode);
            }
        }
    }

    @Override
    public void visitBoolean(BooleanNode irBooleanNode, Consumer<ExpressionNode> scope) {
        irBooleanNode.getLeftNode().visit(this, irBooleanNode::setLeftNode);
        irBooleanNode.getRightNode().visit(this, irBooleanNode::setRightNode);

        if (irBooleanNode.getLeftNode() instanceof ConstantNode && irBooleanNode.getRightNode() instanceof ConstantNode) {
            ConstantNode irLeftConstantNode = (ConstantNode) irBooleanNode.getLeftNode();
            ConstantNode irRightConstantNode = (ConstantNode) irBooleanNode.getRightNode();
            Operation operation = irBooleanNode.getOperation();
            Class<?> type = irBooleanNode.getExpressionType();

            if (operation == Operation.AND) {
                if (type == boolean.class) {
                    irLeftConstantNode.setConstant(
                        (boolean) irLeftConstantNode.getConstant() && (boolean) irRightConstantNode.getConstant()
                    );
                } else {
                    throw irBooleanNode.getLocation()
                        .createError(
                            binaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getConstant().toString(),
                                irRightConstantNode.getConstant().toString()
                            )
                        );
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.OR) {
                if (type == boolean.class) {
                    irLeftConstantNode.setConstant(
                        (boolean) irLeftConstantNode.getConstant() || (boolean) irRightConstantNode.getConstant()
                    );
                } else {
                    throw irBooleanNode.getLocation()
                        .createError(
                            booleanError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getConstant().toString(),
                                irRightConstantNode.getConstant().toString()
                            )
                        );
                }

                scope.accept(irLeftConstantNode);
            }
        }
    }

    @Override
    public void visitComparison(ComparisonNode irComparisonNode, Consumer<ExpressionNode> scope) {
        irComparisonNode.getLeftNode().visit(this, irComparisonNode::setLeftNode);
        irComparisonNode.getRightNode().visit(this, irComparisonNode::setRightNode);

        if ((irComparisonNode.getLeftNode() instanceof ConstantNode || irComparisonNode.getLeftNode() instanceof NullNode)
            && (irComparisonNode.getRightNode() instanceof ConstantNode || irComparisonNode.getRightNode() instanceof NullNode)) {

            ConstantNode irLeftConstantNode = irComparisonNode.getLeftNode() instanceof NullNode
                ? null
                : (ConstantNode) irComparisonNode.getLeftNode();
            ConstantNode irRightConstantNode = irComparisonNode.getRightNode() instanceof NullNode
                ? null
                : (ConstantNode) irComparisonNode.getRightNode();
            Object leftConstantValue = irLeftConstantNode == null ? null : irLeftConstantNode.getConstant();
            Object rightConstantValue = irRightConstantNode == null ? null : irRightConstantNode.getConstant();
            Operation operation = irComparisonNode.getOperation();
            Class<?> type = irComparisonNode.getComparisonType();

            if (operation == Operation.EQ || operation == Operation.EQR) {
                if (irLeftConstantNode == null && irRightConstantNode == null) {
                    irLeftConstantNode = new ConstantNode(irComparisonNode.getLeftNode().getLocation());
                    irLeftConstantNode.setConstant(true);
                } else if (irLeftConstantNode == null || irRightConstantNode == null) {
                    irLeftConstantNode = new ConstantNode(irComparisonNode.getLeftNode().getLocation());
                    irLeftConstantNode.setConstant(false);
                } else if (type == boolean.class) {
                    irLeftConstantNode.setConstant((boolean) leftConstantValue == (boolean) rightConstantValue);
                } else if (type == int.class) {
                    irLeftConstantNode.setConstant((int) leftConstantValue == (int) rightConstantValue);
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long) leftConstantValue == (long) rightConstantValue);
                } else if (type == float.class) {
                    irLeftConstantNode.setConstant((float) leftConstantValue == (float) rightConstantValue);
                } else if (type == double.class) {
                    irLeftConstantNode.setConstant((double) leftConstantValue == (double) rightConstantValue);
                } else {
                    if (operation == Operation.EQ) {
                        irLeftConstantNode.setConstant(leftConstantValue.equals(rightConstantValue));
                    } else {
                        irLeftConstantNode.setConstant(leftConstantValue == rightConstantValue);
                    }
                }

                irLeftConstantNode.setExpressionType(boolean.class);
                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.NE || operation == Operation.NER) {
                if (irLeftConstantNode == null && irRightConstantNode == null) {
                    irLeftConstantNode = new ConstantNode(irComparisonNode.getLeftNode().getLocation());
                    irLeftConstantNode.setConstant(false);
                } else if (irLeftConstantNode == null || irRightConstantNode == null) {
                    irLeftConstantNode = new ConstantNode(irComparisonNode.getLeftNode().getLocation());
                    irLeftConstantNode.setConstant(true);
                } else if (type == boolean.class) {
                    irLeftConstantNode.setConstant((boolean) leftConstantValue != (boolean) rightConstantValue);
                } else if (type == int.class) {
                    irLeftConstantNode.setConstant((int) leftConstantValue != (int) rightConstantValue);
                } else if (type == long.class) {
                    irLeftConstantNode.setConstant((long) leftConstantValue != (long) rightConstantValue);
                } else if (type == float.class) {
                    irLeftConstantNode.setConstant((float) leftConstantValue != (float) rightConstantValue);
                } else if (type == double.class) {
                    irLeftConstantNode.setConstant((double) leftConstantValue != (double) rightConstantValue);
                } else {
                    if (operation == Operation.NE) {
                        irLeftConstantNode.setConstant(leftConstantValue.equals(rightConstantValue) == false);
                    } else {
                        irLeftConstantNode.setConstant(leftConstantValue != rightConstantValue);
                    }
                }

                irLeftConstantNode.setExpressionType(boolean.class);
                scope.accept(irLeftConstantNode);
            } else if (irLeftConstantNode != null && irRightConstantNode != null) {
                if (operation == Operation.GT) {
                    if (type == int.class) {
                        irLeftConstantNode.setConstant((int) leftConstantValue > (int) rightConstantValue);
                    } else if (type == long.class) {
                        irLeftConstantNode.setConstant((long) leftConstantValue > (long) rightConstantValue);
                    } else if (type == float.class) {
                        irLeftConstantNode.setConstant((float) leftConstantValue > (float) rightConstantValue);
                    } else if (type == double.class) {
                        irLeftConstantNode.setConstant((double) leftConstantValue > (double) rightConstantValue);
                    } else {
                        throw irComparisonNode.getLocation()
                            .createError(
                                comparisonError(
                                    PainlessLookupUtility.typeToCanonicalTypeName(type),
                                    operation.symbol,
                                    irLeftConstantNode.getConstant().toString(),
                                    irRightConstantNode.getConstant().toString()
                                )
                            );
                    }

                    irLeftConstantNode.setExpressionType(boolean.class);
                    scope.accept(irLeftConstantNode);
                } else if (operation == Operation.GTE) {
                    if (type == int.class) {
                        irLeftConstantNode.setConstant((int) leftConstantValue >= (int) rightConstantValue);
                    } else if (type == long.class) {
                        irLeftConstantNode.setConstant((long) leftConstantValue >= (long) rightConstantValue);
                    } else if (type == float.class) {
                        irLeftConstantNode.setConstant((float) leftConstantValue >= (float) rightConstantValue);
                    } else if (type == double.class) {
                        irLeftConstantNode.setConstant((double) leftConstantValue >= (double) rightConstantValue);
                    } else {
                        throw irComparisonNode.getLocation()
                            .createError(
                                comparisonError(
                                    PainlessLookupUtility.typeToCanonicalTypeName(type),
                                    operation.symbol,
                                    irLeftConstantNode.getConstant().toString(),
                                    irRightConstantNode.getConstant().toString()
                                )
                            );
                    }

                    irLeftConstantNode.setExpressionType(boolean.class);
                    scope.accept(irLeftConstantNode);
                } else if (operation == Operation.LT) {
                    if (type == int.class) {
                        irLeftConstantNode.setConstant((int) leftConstantValue < (int) rightConstantValue);
                    } else if (type == long.class) {
                        irLeftConstantNode.setConstant((long) leftConstantValue < (long) rightConstantValue);
                    } else if (type == float.class) {
                        irLeftConstantNode.setConstant((float) leftConstantValue < (float) rightConstantValue);
                    } else if (type == double.class) {
                        irLeftConstantNode.setConstant((double) leftConstantValue < (double) rightConstantValue);
                    } else {
                        throw irComparisonNode.getLocation()
                            .createError(
                                comparisonError(
                                    PainlessLookupUtility.typeToCanonicalTypeName(type),
                                    operation.symbol,
                                    irLeftConstantNode.getConstant().toString(),
                                    irRightConstantNode.getConstant().toString()
                                )
                            );
                    }

                    irLeftConstantNode.setExpressionType(boolean.class);
                    scope.accept(irLeftConstantNode);
                } else if (operation == Operation.LTE) {
                    if (type == int.class) {
                        irLeftConstantNode.setConstant((int) leftConstantValue <= (int) rightConstantValue);
                    } else if (type == long.class) {
                        irLeftConstantNode.setConstant((long) leftConstantValue <= (long) rightConstantValue);
                    } else if (type == float.class) {
                        irLeftConstantNode.setConstant((float) leftConstantValue <= (float) rightConstantValue);
                    } else if (type == double.class) {
                        irLeftConstantNode.setConstant((double) leftConstantValue <= (double) rightConstantValue);
                    } else {
                        throw irComparisonNode.getLocation()
                            .createError(
                                comparisonError(
                                    PainlessLookupUtility.typeToCanonicalTypeName(type),
                                    operation.symbol,
                                    irLeftConstantNode.getConstant().toString(),
                                    irRightConstantNode.getConstant().toString()
                                )
                            );
                    }

                    irLeftConstantNode.setExpressionType(boolean.class);
                    scope.accept(irLeftConstantNode);
                }
            }
        }
    }

    @Override
    public void visitCast(CastNode irCastNode, Consumer<ExpressionNode> scope) {
        irCastNode.getChildNode().visit(this, irCastNode::setChildNode);

        if (irCastNode.getChildNode() instanceof ConstantNode && PainlessLookupUtility.isConstantType(irCastNode.getExpressionType())) {
            ConstantNode irConstantNode = (ConstantNode) irCastNode.getChildNode();
            Object constantValue = irConstantNode.getConstant();
            constantValue = AnalyzerCaster.constCast(irCastNode.getLocation(), constantValue, irCastNode.getCast());
            irConstantNode.setConstant(constantValue);
            irConstantNode.setExpressionType(irCastNode.getExpressionType());
            scope.accept(irConstantNode);
        }
    }

    @Override
    public void visitInvokeCallMember(InvokeCallMemberNode irInvokeCallMemberNode, Consumer<ExpressionNode> scope) {
        for (int i = 0; i < irInvokeCallMemberNode.getArgumentNodes().size(); i++) {
            int j = i;
            irInvokeCallMemberNode.getArgumentNodes().get(i).visit(this, (e) -> irInvokeCallMemberNode.getArgumentNodes().set(j, e));
        }
        PainlessMethod method = irInvokeCallMemberNode.getMethod();
        if (method != null && method.annotations().containsKey(CompileTimeOnlyAnnotation.class)) {
            replaceCallWithConstant(irInvokeCallMemberNode, scope, method.javaMethod(), null);
            return;
        }
        PainlessInstanceBinding instanceBinding = irInvokeCallMemberNode.getInstanceBinding();
        if (instanceBinding != null && instanceBinding.annotations().containsKey(CompileTimeOnlyAnnotation.class)) {
            replaceCallWithConstant(irInvokeCallMemberNode, scope, instanceBinding.javaMethod(), instanceBinding.targetInstance());
            return;
        }
    }

    private static void replaceCallWithConstant(
        InvokeCallMemberNode irInvokeCallMemberNode,
        Consumer<ExpressionNode> scope,
        Method javaMethod,
        Object receiver
    ) {
        Object[] args = new Object[irInvokeCallMemberNode.getArgumentNodes().size()];
        for (int i = 0; i < irInvokeCallMemberNode.getArgumentNodes().size(); i++) {
            ExpressionNode argNode = irInvokeCallMemberNode.getArgumentNodes().get(i);
            if (argNode instanceof ConstantNode == false) {
                // TODO find a better string to output
                throw irInvokeCallMemberNode.getLocation()
                    .createError(
                        new IllegalArgumentException(
                            "all arguments to [" + javaMethod.getName() + "] must be constant but the [" + (i + 1) + "] argument isn't"
                        )
                    );
            }
            args[i] = ((ConstantNode) argNode).getConstant();
        }
        Object result;
        try {
            result = javaMethod.invoke(receiver, args);
        } catch (IllegalAccessException | IllegalArgumentException e) {
            throw irInvokeCallMemberNode.getLocation()
                .createError(new IllegalArgumentException("error invoking [" + irInvokeCallMemberNode + "] at compile time", e));
        } catch (InvocationTargetException e) {
            throw irInvokeCallMemberNode.getLocation()
                .createError(new IllegalArgumentException("error invoking [" + irInvokeCallMemberNode + "] at compile time", e.getCause()));
        }
        ConstantNode replacement = new ConstantNode(irInvokeCallMemberNode.getLocation());
        replacement.setConstant(result);
        replacement.setExpressionType(irInvokeCallMemberNode.getExpressionType());
        scope.accept(replacement);
    }
}
