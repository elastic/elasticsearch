/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.painless.symbol.IRDecorations.IRDCast;
import org.elasticsearch.painless.symbol.IRDecorations.IRDComparisonType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDConstant;
import org.elasticsearch.painless.symbol.IRDecorations.IRDExpressionType;
import org.elasticsearch.painless.symbol.IRDecorations.IRDInstanceBinding;
import org.elasticsearch.painless.symbol.IRDecorations.IRDMethod;
import org.elasticsearch.painless.symbol.IRDecorations.IRDOperation;

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
            ExpressionNode irConstantNode = irUnaryMathNode.getChildNode();
            Object constantValue = irConstantNode.getDecorationValue(IRDConstant.class);
            Operation operation = irUnaryMathNode.getDecorationValue(IRDOperation.class);
            Class<?> type = irUnaryMathNode.getDecorationValue(IRDExpressionType.class);

            if (operation == Operation.SUB) {
                if (type == int.class) {
                    irConstantNode.attachDecoration(new IRDConstant(-(int) constantValue));
                } else if (type == long.class) {
                    irConstantNode.attachDecoration(new IRDConstant(-(long) constantValue));
                } else if (type == float.class) {
                    irConstantNode.attachDecoration(new IRDConstant(-(float) constantValue));
                } else if (type == double.class) {
                    irConstantNode.attachDecoration(new IRDConstant(-(double) constantValue));
                } else {
                    throw irUnaryMathNode.getLocation()
                        .createError(
                            unaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irConstantNode.getDecorationString(IRDConstant.class)
                            )
                        );
                }

                scope.accept(irConstantNode);
            } else if (operation == Operation.BWNOT) {
                if (type == int.class) {
                    irConstantNode.attachDecoration(new IRDConstant(~(int) constantValue));
                } else if (type == long.class) {
                    irConstantNode.attachDecoration(new IRDConstant(~(long) constantValue));
                } else {
                    throw irUnaryMathNode.getLocation()
                        .createError(
                            unaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irConstantNode.getDecorationString(IRDConstant.class)
                            )
                        );
                }

                scope.accept(irConstantNode);
            } else if (operation == Operation.NOT) {
                if (type == boolean.class) {
                    irConstantNode.attachDecoration(new IRDConstant(((boolean) constantValue) == false));
                } else {
                    throw irUnaryMathNode.getLocation()
                        .createError(
                            unaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irConstantNode.getDecorationString(IRDConstant.class)
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
            ExpressionNode irLeftConstantNode = irBinaryMathNode.getLeftNode();
            ExpressionNode irRightConstantNode = irBinaryMathNode.getRightNode();
            Object leftConstantValue = irLeftConstantNode.getDecorationValue(IRDConstant.class);
            Object rightConstantValue = irRightConstantNode.getDecorationValue(IRDConstant.class);
            Operation operation = irBinaryMathNode.getDecorationValue(IRDOperation.class);
            Class<?> type = irBinaryMathNode.getDecorationValue(IRDExpressionType.class);

            if (operation == Operation.MUL) {
                if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int) leftConstantValue * (int) rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long) leftConstantValue * (long) rightConstantValue));
                } else if (type == float.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((float) leftConstantValue * (float) rightConstantValue));
                } else if (type == double.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((double) leftConstantValue * (double) rightConstantValue));
                } else {
                    throw irBinaryMathNode.getLocation()
                        .createError(
                            binaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getDecorationString(IRDConstant.class),
                                irRightConstantNode.getDecorationString(IRDConstant.class)
                            )
                        );
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.DIV) {
                try {
                    if (type == int.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((int) leftConstantValue / (int) rightConstantValue));
                    } else if (type == long.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((long) leftConstantValue / (long) rightConstantValue));
                    } else if (type == float.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((float) leftConstantValue / (float) rightConstantValue));
                    } else if (type == double.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((double) leftConstantValue / (double) rightConstantValue));
                    } else {
                        throw irBinaryMathNode.getLocation()
                            .createError(
                                binaryError(
                                    PainlessLookupUtility.typeToCanonicalTypeName(type),
                                    operation.symbol,
                                    irLeftConstantNode.getDecorationString(IRDConstant.class),
                                    irRightConstantNode.getDecorationString(IRDConstant.class)
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
                        irLeftConstantNode.attachDecoration(new IRDConstant((int) leftConstantValue % (int) rightConstantValue));
                    } else if (type == long.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((long) leftConstantValue % (long) rightConstantValue));
                    } else if (type == float.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((float) leftConstantValue % (float) rightConstantValue));
                    } else if (type == double.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((double) leftConstantValue % (double) rightConstantValue));
                    } else {
                        throw irBinaryMathNode.getLocation()
                            .createError(
                                binaryError(
                                    PainlessLookupUtility.typeToCanonicalTypeName(type),
                                    operation.symbol,
                                    irLeftConstantNode.getDecorationString(IRDConstant.class),
                                    irRightConstantNode.getDecorationString(IRDConstant.class)
                                )
                            );
                    }
                } catch (ArithmeticException ae) {
                    throw irBinaryMathNode.getLocation().createError(ae);
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.ADD) {
                if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int) leftConstantValue + (int) rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long) leftConstantValue + (long) rightConstantValue));
                } else if (type == float.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((float) leftConstantValue + (float) rightConstantValue));
                } else if (type == double.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((double) leftConstantValue + (double) rightConstantValue));
                } else {
                    throw irBinaryMathNode.getLocation()
                        .createError(
                            binaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getDecorationString(IRDConstant.class),
                                irRightConstantNode.getDecorationString(IRDConstant.class)
                            )
                        );
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.SUB) {
                if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int) leftConstantValue - (int) rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long) leftConstantValue - (long) rightConstantValue));
                } else if (type == float.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((float) leftConstantValue - (float) rightConstantValue));
                } else if (type == double.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((double) leftConstantValue - (double) rightConstantValue));
                } else {
                    throw irBinaryMathNode.getLocation()
                        .createError(
                            binaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getDecorationString(IRDConstant.class),
                                irRightConstantNode.getDecorationString(IRDConstant.class)
                            )
                        );
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.LSH) {
                if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int) leftConstantValue << (int) rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long) leftConstantValue << (int) rightConstantValue));
                } else {
                    throw irBinaryMathNode.getLocation()
                        .createError(
                            binaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getDecorationString(IRDConstant.class),
                                irRightConstantNode.getDecorationString(IRDConstant.class)
                            )
                        );
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.RSH) {
                if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int) leftConstantValue >> (int) rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long) leftConstantValue >> (int) rightConstantValue));
                } else {
                    throw irBinaryMathNode.getLocation()
                        .createError(
                            binaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getDecorationString(IRDConstant.class),
                                irRightConstantNode.getDecorationString(IRDConstant.class)
                            )
                        );
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.USH) {
                if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int) leftConstantValue >>> (int) rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long) leftConstantValue >>> (int) rightConstantValue));
                } else {
                    throw irBinaryMathNode.getLocation()
                        .createError(
                            binaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getDecorationString(IRDConstant.class),
                                irRightConstantNode.getDecorationString(IRDConstant.class)
                            )
                        );
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.BWAND) {
                if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int) leftConstantValue & (int) rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long) leftConstantValue & (long) rightConstantValue));
                } else {
                    throw irBinaryMathNode.getLocation()
                        .createError(
                            binaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getDecorationString(IRDConstant.class),
                                irRightConstantNode.getDecorationString(IRDConstant.class)
                            )
                        );
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.XOR) {
                if (type == boolean.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((boolean) leftConstantValue ^ (boolean) rightConstantValue));
                } else if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int) leftConstantValue ^ (int) rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long) leftConstantValue ^ (long) rightConstantValue));
                } else {
                    throw irBinaryMathNode.getLocation()
                        .createError(
                            binaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getDecorationString(IRDConstant.class),
                                irRightConstantNode.getDecorationString(IRDConstant.class)
                            )
                        );
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.BWOR) {
                if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int) leftConstantValue | (int) rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long) leftConstantValue | (long) rightConstantValue));
                } else {
                    throw irBinaryMathNode.getLocation()
                        .createError(
                            binaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getDecorationString(IRDConstant.class),
                                irRightConstantNode.getDecorationString(IRDConstant.class)
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

            if (irLeftNode instanceof ConstantNode && irRightNode instanceof ConstantNode) {
                irLeftNode.attachDecoration(
                    new IRDConstant(
                        "" + irLeftNode.getDecorationValue(IRDConstant.class) + irRightNode.getDecorationValue(IRDConstant.class)
                    )
                );
                irLeftNode.attachDecoration(new IRDExpressionType(String.class));
                irStringConcatenationNode.getArgumentNodes().remove(i + 1);
            } else if (irLeftNode instanceof NullNode && irRightNode instanceof ConstantNode) {
                irRightNode.attachDecoration(new IRDConstant("" + null + irRightNode.getDecorationValue(IRDConstant.class)));
                irRightNode.attachDecoration(new IRDExpressionType(String.class));
                irStringConcatenationNode.getArgumentNodes().remove(i);
            } else if (irLeftNode instanceof ConstantNode && irRightNode instanceof NullNode) {
                irLeftNode.attachDecoration(new IRDConstant("" + irLeftNode.getDecorationValue(IRDConstant.class) + null));
                irLeftNode.attachDecoration(new IRDExpressionType(String.class));
                irStringConcatenationNode.getArgumentNodes().remove(i + 1);
            } else if (irLeftNode instanceof NullNode && irRightNode instanceof NullNode) {
                ConstantNode irConstantNode = new ConstantNode(irLeftNode.getLocation());
                irConstantNode.attachDecoration(new IRDConstant("" + null + null));
                irConstantNode.attachDecoration(new IRDExpressionType(String.class));
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
            ExpressionNode irLeftConstantNode = irBooleanNode.getLeftNode();
            ExpressionNode irRightConstantNode = irBooleanNode.getRightNode();
            Operation operation = irBooleanNode.getDecorationValue(IRDOperation.class);
            Class<?> type = irBooleanNode.getDecorationValue(IRDExpressionType.class);

            if (operation == Operation.AND) {
                if (type == boolean.class) {
                    irLeftConstantNode.attachDecoration(
                        new IRDConstant(
                            (boolean) irLeftConstantNode.getDecorationValue(IRDConstant.class)
                                && (boolean) irRightConstantNode.getDecorationValue(IRDConstant.class)
                        )
                    );
                } else {
                    throw irBooleanNode.getLocation()
                        .createError(
                            binaryError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getDecorationString(IRDConstant.class),
                                irRightConstantNode.getDecorationString(IRDConstant.class)
                            )
                        );
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.OR) {
                if (type == boolean.class) {
                    irLeftConstantNode.attachDecoration(
                        new IRDConstant(
                            (boolean) irLeftConstantNode.getDecorationValue(IRDConstant.class)
                                || (boolean) irRightConstantNode.getDecorationValue(IRDConstant.class)
                        )
                    );
                } else {
                    throw irBooleanNode.getLocation()
                        .createError(
                            booleanError(
                                PainlessLookupUtility.typeToCanonicalTypeName(type),
                                operation.symbol,
                                irLeftConstantNode.getDecorationString(IRDConstant.class),
                                irRightConstantNode.getDecorationString(IRDConstant.class)
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

            ExpressionNode irLeftConstantNode = irComparisonNode.getLeftNode() instanceof NullNode ? null : irComparisonNode.getLeftNode();
            ExpressionNode irRightConstantNode = irComparisonNode.getRightNode() instanceof NullNode
                ? null
                : irComparisonNode.getRightNode();
            Object leftConstantValue = irLeftConstantNode == null ? null : irLeftConstantNode.getDecorationValue(IRDConstant.class);
            Object rightConstantValue = irRightConstantNode == null ? null : irRightConstantNode.getDecorationValue(IRDConstant.class);
            Operation operation = irComparisonNode.getDecorationValue(IRDOperation.class);
            Class<?> type = irComparisonNode.getDecorationValue(IRDComparisonType.class);

            if (operation == Operation.EQ || operation == Operation.EQR) {
                if (irLeftConstantNode == null && irRightConstantNode == null) {
                    irLeftConstantNode = new ConstantNode(irComparisonNode.getLeftNode().getLocation());
                    irLeftConstantNode.attachDecoration(new IRDConstant(true));
                } else if (irLeftConstantNode == null || irRightConstantNode == null) {
                    irLeftConstantNode = new ConstantNode(irComparisonNode.getLeftNode().getLocation());
                    irLeftConstantNode.attachDecoration(new IRDConstant(false));
                } else if (type == boolean.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((boolean) leftConstantValue == (boolean) rightConstantValue));
                } else if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int) leftConstantValue == (int) rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long) leftConstantValue == (long) rightConstantValue));
                } else if (type == float.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((float) leftConstantValue == (float) rightConstantValue));
                } else if (type == double.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((double) leftConstantValue == (double) rightConstantValue));
                } else {
                    if (operation == Operation.EQ) {
                        irLeftConstantNode.attachDecoration(new IRDConstant(leftConstantValue.equals(rightConstantValue)));
                    } else {
                        irLeftConstantNode.attachDecoration(new IRDConstant(leftConstantValue == rightConstantValue));
                    }
                }

                irLeftConstantNode.attachDecoration(new IRDExpressionType(boolean.class));
                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.NE || operation == Operation.NER) {
                if (irLeftConstantNode == null && irRightConstantNode == null) {
                    irLeftConstantNode = new ConstantNode(irComparisonNode.getLeftNode().getLocation());
                    irLeftConstantNode.attachDecoration(new IRDConstant(false));
                } else if (irLeftConstantNode == null || irRightConstantNode == null) {
                    irLeftConstantNode = new ConstantNode(irComparisonNode.getLeftNode().getLocation());
                    irLeftConstantNode.attachDecoration(new IRDConstant(true));
                } else if (type == boolean.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((boolean) leftConstantValue != (boolean) rightConstantValue));
                } else if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int) leftConstantValue != (int) rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long) leftConstantValue != (long) rightConstantValue));
                } else if (type == float.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((float) leftConstantValue != (float) rightConstantValue));
                } else if (type == double.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((double) leftConstantValue != (double) rightConstantValue));
                } else {
                    if (operation == Operation.NE) {
                        irLeftConstantNode.attachDecoration(new IRDConstant(leftConstantValue.equals(rightConstantValue) == false));
                    } else {
                        irLeftConstantNode.attachDecoration(new IRDConstant(leftConstantValue != rightConstantValue));
                    }
                }

                irLeftConstantNode.attachDecoration(new IRDExpressionType(boolean.class));
                scope.accept(irLeftConstantNode);
            } else if (irLeftConstantNode != null && irRightConstantNode != null) {
                if (operation == Operation.GT) {
                    if (type == int.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((int) leftConstantValue > (int) rightConstantValue));
                    } else if (type == long.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((long) leftConstantValue > (long) rightConstantValue));
                    } else if (type == float.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((float) leftConstantValue > (float) rightConstantValue));
                    } else if (type == double.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((double) leftConstantValue > (double) rightConstantValue));
                    } else {
                        throw irComparisonNode.getLocation()
                            .createError(
                                comparisonError(
                                    PainlessLookupUtility.typeToCanonicalTypeName(type),
                                    operation.symbol,
                                    irLeftConstantNode.getDecorationString(IRDConstant.class),
                                    irRightConstantNode.getDecorationString(IRDConstant.class)
                                )
                            );
                    }

                    irLeftConstantNode.attachDecoration(new IRDExpressionType(boolean.class));
                    scope.accept(irLeftConstantNode);
                } else if (operation == Operation.GTE) {
                    if (type == int.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((int) leftConstantValue >= (int) rightConstantValue));
                    } else if (type == long.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((long) leftConstantValue >= (long) rightConstantValue));
                    } else if (type == float.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((float) leftConstantValue >= (float) rightConstantValue));
                    } else if (type == double.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((double) leftConstantValue >= (double) rightConstantValue));
                    } else {
                        throw irComparisonNode.getLocation()
                            .createError(
                                comparisonError(
                                    PainlessLookupUtility.typeToCanonicalTypeName(type),
                                    operation.symbol,
                                    irLeftConstantNode.getDecorationString(IRDConstant.class),
                                    irRightConstantNode.getDecorationString(IRDConstant.class)
                                )
                            );
                    }

                    irLeftConstantNode.attachDecoration(new IRDExpressionType(boolean.class));
                    scope.accept(irLeftConstantNode);
                } else if (operation == Operation.LT) {
                    if (type == int.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((int) leftConstantValue < (int) rightConstantValue));
                    } else if (type == long.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((long) leftConstantValue < (long) rightConstantValue));
                    } else if (type == float.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((float) leftConstantValue < (float) rightConstantValue));
                    } else if (type == double.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((double) leftConstantValue < (double) rightConstantValue));
                    } else {
                        throw irComparisonNode.getLocation()
                            .createError(
                                comparisonError(
                                    PainlessLookupUtility.typeToCanonicalTypeName(type),
                                    operation.symbol,
                                    irLeftConstantNode.getDecorationString(IRDConstant.class),
                                    irRightConstantNode.getDecorationString(IRDConstant.class)
                                )
                            );
                    }

                    irLeftConstantNode.attachDecoration(new IRDExpressionType(boolean.class));
                    scope.accept(irLeftConstantNode);
                } else if (operation == Operation.LTE) {
                    if (type == int.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((int) leftConstantValue <= (int) rightConstantValue));
                    } else if (type == long.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((long) leftConstantValue <= (long) rightConstantValue));
                    } else if (type == float.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((float) leftConstantValue <= (float) rightConstantValue));
                    } else if (type == double.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((double) leftConstantValue <= (double) rightConstantValue));
                    } else {
                        throw irComparisonNode.getLocation()
                            .createError(
                                comparisonError(
                                    PainlessLookupUtility.typeToCanonicalTypeName(type),
                                    operation.symbol,
                                    irLeftConstantNode.getDecorationString(IRDConstant.class),
                                    irRightConstantNode.getDecorationString(IRDConstant.class)
                                )
                            );
                    }

                    irLeftConstantNode.attachDecoration(new IRDExpressionType(boolean.class));
                    scope.accept(irLeftConstantNode);
                }
            }
        }
    }

    @Override
    public void visitCast(CastNode irCastNode, Consumer<ExpressionNode> scope) {
        irCastNode.getChildNode().visit(this, irCastNode::setChildNode);

        if (irCastNode.getChildNode() instanceof ConstantNode
            && PainlessLookupUtility.isConstantType(irCastNode.getDecorationValue(IRDExpressionType.class))) {
            ExpressionNode irConstantNode = irCastNode.getChildNode();
            Object constantValue = irConstantNode.getDecorationValue(IRDConstant.class);
            constantValue = AnalyzerCaster.constCast(irCastNode.getLocation(), constantValue, irCastNode.getDecorationValue(IRDCast.class));
            irConstantNode.attachDecoration(new IRDConstant(constantValue));
            irConstantNode.attachDecoration(irCastNode.getDecoration(IRDExpressionType.class));
            scope.accept(irConstantNode);
        }
    }

    @Override
    public void visitInvokeCallMember(InvokeCallMemberNode irInvokeCallMemberNode, Consumer<ExpressionNode> scope) {
        for (int i = 0; i < irInvokeCallMemberNode.getArgumentNodes().size(); i++) {
            int j = i;
            irInvokeCallMemberNode.getArgumentNodes().get(i).visit(this, (e) -> irInvokeCallMemberNode.getArgumentNodes().set(j, e));
        }
        PainlessMethod method = irInvokeCallMemberNode.getDecorationValue(IRDMethod.class);
        if (method != null && method.annotations().containsKey(CompileTimeOnlyAnnotation.class)) {
            replaceCallWithConstant(irInvokeCallMemberNode, scope, method.javaMethod(), null);
            return;
        }
        PainlessInstanceBinding instanceBinding = irInvokeCallMemberNode.getDecorationValue(IRDInstanceBinding.class);
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
            IRDConstant constantDecoration = argNode.getDecoration(IRDConstant.class);
            if (constantDecoration == null) {
                // TODO find a better string to output
                throw irInvokeCallMemberNode.getLocation()
                    .createError(
                        new IllegalArgumentException(
                            "all arguments to [" + javaMethod.getName() + "] must be constant but the [" + (i + 1) + "] argument isn't"
                        )
                    );
            }
            args[i] = constantDecoration.getValue();
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
        replacement.attachDecoration(new IRDConstant(result));
        replacement.attachDecoration(irInvokeCallMemberNode.getDecoration(IRDExpressionType.class));
        scope.accept(replacement);
    }
}
