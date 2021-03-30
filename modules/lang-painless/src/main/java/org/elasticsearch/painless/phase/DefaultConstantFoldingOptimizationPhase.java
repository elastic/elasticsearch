/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.phase;

import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.ir.BinaryImplNode;
import org.elasticsearch.painless.ir.BinaryMathNode;
import org.elasticsearch.painless.ir.BooleanNode;
import org.elasticsearch.painless.ir.CastNode;
import org.elasticsearch.painless.ir.ComparisonNode;
import org.elasticsearch.painless.ir.ConditionalNode;
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.ir.DeclarationNode;
import org.elasticsearch.painless.ir.DoWhileLoopNode;
import org.elasticsearch.painless.ir.DupNode;
import org.elasticsearch.painless.ir.ElvisNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.FlipArrayIndexNode;
import org.elasticsearch.painless.ir.FlipCollectionIndexNode;
import org.elasticsearch.painless.ir.FlipDefIndexNode;
import org.elasticsearch.painless.ir.ForEachSubArrayNode;
import org.elasticsearch.painless.ir.ForEachSubIterableNode;
import org.elasticsearch.painless.ir.ForLoopNode;
import org.elasticsearch.painless.ir.IfElseNode;
import org.elasticsearch.painless.ir.IfNode;
import org.elasticsearch.painless.ir.InstanceofNode;
import org.elasticsearch.painless.ir.InvokeCallDefNode;
import org.elasticsearch.painless.ir.InvokeCallMemberNode;
import org.elasticsearch.painless.ir.InvokeCallNode;
import org.elasticsearch.painless.ir.ListInitializationNode;
import org.elasticsearch.painless.ir.MapInitializationNode;
import org.elasticsearch.painless.ir.NewArrayNode;
import org.elasticsearch.painless.ir.NewObjectNode;
import org.elasticsearch.painless.ir.NullNode;
import org.elasticsearch.painless.ir.NullSafeSubNode;
import org.elasticsearch.painless.ir.ReturnNode;
import org.elasticsearch.painless.ir.StatementExpressionNode;
import org.elasticsearch.painless.ir.StoreBraceDefNode;
import org.elasticsearch.painless.ir.StoreBraceNode;
import org.elasticsearch.painless.ir.StoreDotDefNode;
import org.elasticsearch.painless.ir.StoreDotNode;
import org.elasticsearch.painless.ir.StoreDotShortcutNode;
import org.elasticsearch.painless.ir.StoreFieldMemberNode;
import org.elasticsearch.painless.ir.StoreListShortcutNode;
import org.elasticsearch.painless.ir.StoreMapShortcutNode;
import org.elasticsearch.painless.ir.StoreVariableNode;
import org.elasticsearch.painless.ir.StringConcatenationNode;
import org.elasticsearch.painless.ir.ThrowNode;
import org.elasticsearch.painless.ir.UnaryMathNode;
import org.elasticsearch.painless.ir.WhileLoopNode;
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
public class DefaultConstantFoldingOptimizationPhase extends IRTreeBaseVisitor<Consumer<ExpressionNode>> {

    @Override
    public void visitIf(IfNode irIfNode, Consumer<ExpressionNode> scope) {
        irIfNode.getConditionNode().visit(this, irIfNode::setConditionNode);
        irIfNode.getBlockNode().visit(this, null);
    }

    @Override
    public void visitIfElse(IfElseNode irIfElseNode, Consumer<ExpressionNode> scope) {
        irIfElseNode.getConditionNode().visit(this, irIfElseNode::setConditionNode);
        irIfElseNode.getBlockNode().visit(this, null);
        irIfElseNode.getElseBlockNode().visit(this, null);
    }

    @Override
    public void visitWhileLoop(WhileLoopNode irWhileLoopNode, Consumer<ExpressionNode> scope) {
        if (irWhileLoopNode.getConditionNode() != null) {
            irWhileLoopNode.getConditionNode().visit(this, irWhileLoopNode::setConditionNode);
        }

        if (irWhileLoopNode.getBlockNode() != null) {
            irWhileLoopNode.getBlockNode().visit(this, null);
        }
    }

    @Override
    public void visitDoWhileLoop(DoWhileLoopNode irDoWhileLoopNode, Consumer<ExpressionNode> scope) {
        irDoWhileLoopNode.getBlockNode().visit(this, null);

        if (irDoWhileLoopNode.getConditionNode() != null) {
            irDoWhileLoopNode.getConditionNode().visit(this, irDoWhileLoopNode::setConditionNode);
        }
    }

    @Override
    public void visitForLoop(ForLoopNode irForLoopNode, Consumer<ExpressionNode> scope) {
        if (irForLoopNode.getInitializerNode() != null) {
            irForLoopNode.getInitializerNode().visit(this, irForLoopNode::setInitialzerNode);
        }

        if (irForLoopNode.getConditionNode() != null) {
            irForLoopNode.getConditionNode().visit(this, irForLoopNode::setConditionNode);
        }

        if (irForLoopNode.getAfterthoughtNode() != null) {
            irForLoopNode.getAfterthoughtNode().visit(this, irForLoopNode::setAfterthoughtNode);
        }

        if (irForLoopNode.getBlockNode() != null) {
            irForLoopNode.getBlockNode().visit(this, null);
        }
    }

    @Override
    public void visitForEachSubArrayLoop(ForEachSubArrayNode irForEachSubArrayNode, Consumer<ExpressionNode> scope) {
        irForEachSubArrayNode.getConditionNode().visit(this, irForEachSubArrayNode::setConditionNode);
        irForEachSubArrayNode.getBlockNode().visit(this, null);
    }

    @Override
    public void visitForEachSubIterableLoop(ForEachSubIterableNode irForEachSubIterableNode, Consumer<ExpressionNode> scope) {
        irForEachSubIterableNode.getConditionNode().visit(this, irForEachSubIterableNode::setConditionNode);
        irForEachSubIterableNode.getBlockNode().visit(this, null);
    }

    @Override
    public void visitDeclaration(DeclarationNode irDeclarationNode, Consumer<ExpressionNode> scope) {
        if (irDeclarationNode.getExpressionNode() != null) {
            irDeclarationNode.getExpressionNode().visit(this, irDeclarationNode::setExpressionNode);
        }
    }

    @Override
    public void visitReturn(ReturnNode irReturnNode, Consumer<ExpressionNode> scope) {
        if (irReturnNode.getExpressionNode() != null) {
            irReturnNode.getExpressionNode().visit(this, irReturnNode::setExpressionNode);
        }
    }

    @Override
    public void visitStatementExpression(StatementExpressionNode irStatementExpressionNode, Consumer<ExpressionNode> scope) {
        irStatementExpressionNode.getExpressionNode().visit(this, irStatementExpressionNode::setExpressionNode);
    }

    @Override
    public void visitThrow(ThrowNode irThrowNode, Consumer<ExpressionNode> scope) {
        irThrowNode.getExpressionNode().visit(this, irThrowNode::setExpressionNode);
    }

    @Override
    public void visitBinaryImpl(BinaryImplNode irBinaryImplNode, Consumer<ExpressionNode> scope) {
        irBinaryImplNode.getLeftNode().visit(this, irBinaryImplNode::setLeftNode);
        irBinaryImplNode.getRightNode().visit(this, irBinaryImplNode::setRightNode);
    }

    @Override
    public void visitUnaryMath(UnaryMathNode irUnaryMathNode, Consumer<ExpressionNode> scope) {
        irUnaryMathNode.getChildNode().visit(this, irUnaryMathNode::setChildNode);

        if (irUnaryMathNode.getChildNode() instanceof ConstantNode) {
            ConstantNode irConstantNode = (ConstantNode)irUnaryMathNode.getChildNode();
            Object constantValue =  irConstantNode.getDecorationValue(IRDConstant.class);
            Operation operation = irUnaryMathNode.getDecorationValue(IRDOperation.class);
            Class<?> type = irUnaryMathNode.getDecorationValue(IRDExpressionType.class);

            if (operation == Operation.SUB) {
                if (type == int.class) {
                    irConstantNode.attachDecoration(new IRDConstant(-(int)constantValue));
                } else if (type == long.class) {
                    irConstantNode.attachDecoration(new IRDConstant(-(long)constantValue));
                } else if (type == float.class) {
                    irConstantNode.attachDecoration(new IRDConstant(-(float)constantValue));
                } else if (type == double.class) {
                    irConstantNode.attachDecoration(new IRDConstant(-(double)constantValue));
                } else {
                    throw irUnaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "unary operation [" + operation.symbol + "] on " +
                            "constant [" + irConstantNode.getDecorationString(IRDConstant.class) + "]"));
                }

                scope.accept(irConstantNode);
            } else if (operation == Operation.BWNOT) {
                if (type == int.class) {
                    irConstantNode.attachDecoration(new IRDConstant(~(int)constantValue));
                } else if (type == long.class) {
                    irConstantNode.attachDecoration(new IRDConstant(~(long)constantValue));
                } else {
                    throw irUnaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "unary operation [" + operation.symbol + "] on " +
                            "constant [" + irConstantNode.getDecorationString(IRDConstant.class) + "]"));
                }

                scope.accept(irConstantNode);
            } else if (operation == Operation.NOT) {
                if (type == boolean.class) {
                    irConstantNode.attachDecoration(new IRDConstant(((boolean) constantValue) == false));
                } else {
                    throw irUnaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "unary operation [" + operation.symbol + "] on " +
                            "constant [" + irConstantNode.getDecorationString(IRDConstant.class) + "]"));
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
            ConstantNode irLeftConstantNode = (ConstantNode)irBinaryMathNode.getLeftNode();
            ConstantNode irRightConstantNode = (ConstantNode)irBinaryMathNode.getRightNode();
            Object leftConstantValue = irLeftConstantNode.getDecorationValue(IRDConstant.class);
            Object rightConstantValue = irRightConstantNode.getDecorationValue(IRDConstant.class);
            Operation operation = irBinaryMathNode.getDecorationValue(IRDOperation.class);
            Class<?> type = irBinaryMathNode.getDecorationValue(IRDExpressionType.class);

            if (operation == Operation.MUL) {
                if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int)leftConstantValue * (int)rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long)leftConstantValue * (long)rightConstantValue));
                } else if (type == float.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((float)leftConstantValue * (float)rightConstantValue));
                } else if (type == double.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((double)leftConstantValue * (double)rightConstantValue));
                } else {
                    throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "binary operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getDecorationString(IRDConstant.class) + "] " +
                            "and [" + irRightConstantNode.getDecorationString(IRDConstant.class) + "]"));
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.DIV) {
                try {
                    if (type == int.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((int)leftConstantValue / (int)rightConstantValue));
                    } else if (type == long.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((long)leftConstantValue / (long)rightConstantValue));
                    } else if (type == float.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((float)leftConstantValue / (float)rightConstantValue));
                    } else if (type == double.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((double)leftConstantValue / (double)rightConstantValue));
                    } else {
                        throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                                "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                                "binary operation [" + operation.symbol + "] on " +
                                "constants [" + irLeftConstantNode.getDecorationString(IRDConstant.class) + "] " +
                                "and [" + irRightConstantNode.getDecorationString(IRDConstant.class) + "]"));
                    }
                } catch (ArithmeticException ae) {
                    throw irBinaryMathNode.getLocation().createError(ae);
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.REM) {
                try {
                    if (type == int.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((int)leftConstantValue % (int)rightConstantValue));
                    } else if (type == long.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((long)leftConstantValue % (long)rightConstantValue));
                    } else if (type == float.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((float)leftConstantValue % (float)rightConstantValue));
                    } else if (type == double.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((double)leftConstantValue % (double)rightConstantValue));
                    } else {
                        throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                                "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                                "binary operation [" + operation.symbol + "] on " +
                                "constants [" + irLeftConstantNode.getDecorationString(IRDConstant.class) + "] " +
                                "and [" + irRightConstantNode.getDecorationString(IRDConstant.class) + "]"));
                    }
                } catch (ArithmeticException ae) {
                    throw irBinaryMathNode.getLocation().createError(ae);
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.ADD) {
                if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int)leftConstantValue + (int)rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long)leftConstantValue + (long)rightConstantValue));
                } else if (type == float.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((float)leftConstantValue + (float)rightConstantValue));
                } else if (type == double.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((double)leftConstantValue + (double)rightConstantValue));
                } else {
                    throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "binary operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getDecorationString(IRDConstant.class) + "] " +
                            "and [" + irRightConstantNode.getDecorationString(IRDConstant.class) + "]"));
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.SUB) {
                if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int)leftConstantValue - (int)rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long)leftConstantValue - (long)rightConstantValue));
                } else if (type == float.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((float)leftConstantValue - (float)rightConstantValue));
                } else if (type == double.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((double)leftConstantValue - (double)rightConstantValue));
                } else {
                    throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "binary operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getDecorationString(IRDConstant.class) + "] " +
                            "and [" + irRightConstantNode.getDecorationString(IRDConstant.class) + "]"));
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.LSH) {
                if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int)leftConstantValue << (int)rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long)leftConstantValue << (int)rightConstantValue));
                } else {
                    throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "binary operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getDecorationString(IRDConstant.class) + "] " +
                            "and [" + irRightConstantNode.getDecorationString(IRDConstant.class) + "]"));
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.RSH) {
                if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int)leftConstantValue >> (int)rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long)leftConstantValue >> (int)rightConstantValue));
                } else {
                    throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "binary operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getDecorationString(IRDConstant.class) + "] " +
                            "and [" + irRightConstantNode.getDecorationString(IRDConstant.class) + "]"));
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.USH) {
                if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int)leftConstantValue >>> (int)rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long)leftConstantValue >>> (int)rightConstantValue));
                } else {
                    throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "binary operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getDecorationString(IRDConstant.class) + "] and " +
                            "[" + irRightConstantNode.getDecorationString(IRDConstant.class) + "]"));
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.BWAND) {
                if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int)leftConstantValue & (int)rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long)leftConstantValue & (long)rightConstantValue));
                } else {
                    throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "binary operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getDecorationString(IRDConstant.class) + "] " +
                            "and [" + irRightConstantNode.getDecorationString(IRDConstant.class) + "]"));
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.XOR) {
                if (type == boolean.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((boolean)leftConstantValue ^ (boolean)rightConstantValue));
                } else if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int)leftConstantValue ^ (int)rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long)leftConstantValue ^ (long)rightConstantValue));
                } else {
                    throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "binary operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getDecorationString(IRDConstant.class) + "] and " +
                            "[" + irRightConstantNode.getDecorationString(IRDConstant.class) + "]"));
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.BWOR) {
                if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int)leftConstantValue | (int)rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long)leftConstantValue | (long)rightConstantValue));
                } else {
                    throw irBinaryMathNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "binary operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getDecorationString(IRDConstant.class) + "] " +
                            "and [" + irRightConstantNode.getDecorationString(IRDConstant.class) + "]"));
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
                ConstantNode irConstantNode = (ConstantNode)irLeftNode;
                irConstantNode.attachDecoration(new IRDConstant(
                        "" + irConstantNode.getDecorationValue(IRDConstant.class) + irRightNode.getDecorationValue(IRDConstant.class)));
                irConstantNode.attachDecoration(new IRDExpressionType(String.class));
                irStringConcatenationNode.getArgumentNodes().remove(i + 1);
            } else if (irLeftNode instanceof NullNode && irRightNode instanceof ConstantNode) {
                ConstantNode irConstantNode = (ConstantNode)irRightNode;
                irConstantNode.attachDecoration(new IRDConstant("" + null + irRightNode.getDecorationValue(IRDConstant.class)));
                irConstantNode.attachDecoration(new IRDExpressionType(String.class));
                irStringConcatenationNode.getArgumentNodes().remove(i);
            } else if (irLeftNode instanceof ConstantNode && irRightNode instanceof NullNode) {
                ConstantNode irConstantNode = (ConstantNode)irLeftNode;
                irConstantNode.attachDecoration(new IRDConstant("" + irLeftNode.getDecorationValue(IRDConstant.class) + null));
                irConstantNode.attachDecoration(new IRDExpressionType(String.class));
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
            ConstantNode irLeftConstantNode = (ConstantNode)irBooleanNode.getLeftNode();
            ConstantNode irRightConstantNode = (ConstantNode)irBooleanNode.getRightNode();
            Operation operation = irBooleanNode.getDecorationValue(IRDOperation.class);
            Class<?> type = irBooleanNode.getDecorationValue(IRDExpressionType.class);

            if (operation == Operation.AND) {
                if (type == boolean.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant(
                            (boolean)irLeftConstantNode.getDecorationValue(IRDConstant.class) &&
                            (boolean)irRightConstantNode.getDecorationValue(IRDConstant.class)));
                } else {
                    throw irBooleanNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "binary operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getDecorationString(IRDConstant.class) + "] " +
                            "and [" + irRightConstantNode.getDecorationString(IRDConstant.class) + "]"));
                }

                scope.accept(irLeftConstantNode);
            } else if (operation == Operation.OR) {
                if (type == boolean.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant(
                            (boolean)irLeftConstantNode.getDecorationValue(IRDConstant.class) ||
                            (boolean)irRightConstantNode.getDecorationValue(IRDConstant.class)));
                } else {
                    throw irBooleanNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                            "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                            "boolean operation [" + operation.symbol + "] on " +
                            "constants [" + irLeftConstantNode.getDecorationString(IRDConstant.class) + "] " +
                            "and [" + irRightConstantNode.getDecorationString(IRDConstant.class) + "]"));
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

            ConstantNode irLeftConstantNode =
                    irComparisonNode.getLeftNode() instanceof NullNode ? null : (ConstantNode)irComparisonNode.getLeftNode();
            ConstantNode irRightConstantNode =
                    irComparisonNode.getRightNode() instanceof NullNode ? null : (ConstantNode)irComparisonNode.getRightNode();
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
                    irLeftConstantNode.attachDecoration(new IRDConstant((boolean)leftConstantValue == (boolean)rightConstantValue));
                } else if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int)leftConstantValue == (int)rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long)leftConstantValue == (long)rightConstantValue));
                } else if (type == float.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((float)leftConstantValue == (float)rightConstantValue));
                } else if (type == double.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((double)leftConstantValue == (double)rightConstantValue));
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
                    irLeftConstantNode.attachDecoration(new IRDConstant((boolean)leftConstantValue != (boolean)rightConstantValue));
                } else if (type == int.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((int)leftConstantValue != (int)rightConstantValue));
                } else if (type == long.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((long)leftConstantValue != (long)rightConstantValue));
                } else if (type == float.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((float)leftConstantValue != (float)rightConstantValue));
                } else if (type == double.class) {
                    irLeftConstantNode.attachDecoration(new IRDConstant((double)leftConstantValue != (double)rightConstantValue));
                } else {
                    if (operation == Operation.NE) {
                        irLeftConstantNode.attachDecoration(new IRDConstant(leftConstantValue.equals(
                                rightConstantValue) == false));
                    } else {
                        irLeftConstantNode.attachDecoration(new IRDConstant(leftConstantValue != rightConstantValue));
                    }
                }

                irLeftConstantNode.attachDecoration(new IRDExpressionType(boolean.class));
                scope.accept(irLeftConstantNode);
            } else if (irLeftConstantNode != null && irRightConstantNode != null) {
                if (operation == Operation.GT) {
                    if (type == int.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((int)leftConstantValue > (int)rightConstantValue));
                    } else if (type == long.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((long)leftConstantValue > (long)rightConstantValue));
                    } else if (type == float.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((float)leftConstantValue > (float)rightConstantValue));
                    } else if (type == double.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((double)leftConstantValue > (double)rightConstantValue));
                    } else {
                        throw irComparisonNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                                "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                                "comparison operation [" + operation.symbol + "] on " +
                                "constants [" + irLeftConstantNode.getDecorationString(IRDConstant.class) + "] " +
                                "and [" + irRightConstantNode.getDecorationString(IRDConstant.class) + "]"));
                    }

                    irLeftConstantNode.attachDecoration(new IRDExpressionType(boolean.class));
                    scope.accept(irLeftConstantNode);
                } else if (operation == Operation.GTE) {
                    if (type == int.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((int)leftConstantValue >= (int)rightConstantValue));
                    } else if (type == long.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((long)leftConstantValue >= (long)rightConstantValue));
                    } else if (type == float.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((float)leftConstantValue >= (float)rightConstantValue));
                    } else if (type == double.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((double)leftConstantValue >= (double)rightConstantValue));
                    } else {
                        throw irComparisonNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                                "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                                "comparison operation [" + operation.symbol + "] on " +
                                "constants [" + irLeftConstantNode.getDecorationString(IRDConstant.class) + "] " +
                                "and [" + irRightConstantNode.getDecorationString(IRDConstant.class) + "]"));
                    }

                    irLeftConstantNode.attachDecoration(new IRDExpressionType(boolean.class));
                    scope.accept(irLeftConstantNode);
                } else if (operation == Operation.LT) {
                    if (type == int.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((int)leftConstantValue < (int)rightConstantValue));
                    } else if (type == long.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((long)leftConstantValue < (long)rightConstantValue));
                    } else if (type == float.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((float)leftConstantValue < (float)rightConstantValue));
                    } else if (type == double.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((double)leftConstantValue < (double)rightConstantValue));
                    } else {
                        throw irComparisonNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                                "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                                "comparison operation [" + operation.symbol + "] on " +
                                "constants [" + irLeftConstantNode.getDecorationString(IRDConstant.class) + "] " +
                                "and [" + irRightConstantNode.getDecorationString(IRDConstant.class) + "]"));
                    }

                    irLeftConstantNode.attachDecoration(new IRDExpressionType(boolean.class));
                    scope.accept(irLeftConstantNode);
                } else if (operation == Operation.LTE) {
                    if (type == int.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((int)leftConstantValue <= (int)rightConstantValue));
                    } else if (type == long.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((long)leftConstantValue <= (long)rightConstantValue));
                    } else if (type == float.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((float)leftConstantValue <= (float)rightConstantValue));
                    } else if (type == double.class) {
                        irLeftConstantNode.attachDecoration(new IRDConstant((double)leftConstantValue <= (double)rightConstantValue));
                    } else {
                        throw irComparisonNode.getLocation().createError(new IllegalStateException("constant folding error: " +
                                "unexpected type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] for " +
                                "comparison operation [" + operation.symbol + "] on " +
                                "constants [" + irLeftConstantNode.getDecorationString(IRDConstant.class) + "] " +
                                "and [" + irRightConstantNode.getDecorationString(IRDConstant.class) + "]"));
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

        if (irCastNode.getChildNode() instanceof ConstantNode &&
                PainlessLookupUtility.isConstantType(irCastNode.getDecorationValue(IRDExpressionType.class))) {
            ConstantNode irConstantNode = (ConstantNode)irCastNode.getChildNode();
            Object constantValue = irConstantNode.getDecorationValue(IRDConstant.class);
            constantValue = AnalyzerCaster.constCast(irCastNode.getLocation(), constantValue, irCastNode.getDecorationValue(IRDCast.class));
            irConstantNode.attachDecoration(new IRDConstant(constantValue));
            irConstantNode.attachDecoration(irCastNode.getDecoration(IRDExpressionType.class));
            scope.accept(irConstantNode);
        }
    }

    @Override
    public void visitInstanceof(InstanceofNode irInstanceofNode, Consumer<ExpressionNode> scope) {
        irInstanceofNode.getChildNode().visit(this, irInstanceofNode::setChildNode);
    }

    @Override
    public void visitConditional(ConditionalNode irConditionalNode, Consumer<ExpressionNode> scope) {
        irConditionalNode.getConditionNode().visit(this, irConditionalNode::setConditionNode);
        irConditionalNode.getLeftNode().visit(this, irConditionalNode::setLeftNode);
        irConditionalNode.getRightNode().visit(this, irConditionalNode::setRightNode);
    }

    @Override
    public void visitElvis(ElvisNode irElvisNode, Consumer<ExpressionNode> scope) {
        irElvisNode.getLeftNode().visit(this, irElvisNode::setLeftNode);
        irElvisNode.getRightNode().visit(this, irElvisNode::setRightNode);
    }

    @Override
    public void visitListInitialization(ListInitializationNode irListInitializationNode, Consumer<ExpressionNode> scope) {
        for (int i = 0; i < irListInitializationNode.getArgumentNodes().size(); i++) {
            int j = i;
            irListInitializationNode.getArgumentNodes().get(i).visit(this, (e) -> irListInitializationNode.getArgumentNodes().set(j, e));
        }
    }

    @Override
    public void visitMapInitialization(MapInitializationNode irMapInitializationNode, Consumer<ExpressionNode> scope) {
        for (int i = 0; i < irMapInitializationNode.getKeyNodes().size(); i++) {
            int j = i;
            irMapInitializationNode.getKeyNode(i).visit(this, (e) -> irMapInitializationNode.getKeyNodes().set(j, e));
        }

        for (int i = 0; i < irMapInitializationNode.getValueNodes().size(); i++) {
            int j = i;
            irMapInitializationNode.getValueNode(i).visit(this, (e) -> irMapInitializationNode.getValueNodes().set(j, e));
        }
    }

    @Override
    public void visitNewArray(NewArrayNode irNewArrayNode, Consumer<ExpressionNode> scope) {
        for (int i = 0; i < irNewArrayNode.getArgumentNodes().size(); i++) {
            int j = i;
            irNewArrayNode.getArgumentNodes().get(i).visit(this, (e) -> irNewArrayNode.getArgumentNodes().set(j, e));
        }
    }

    @Override
    public void visitNewObject(NewObjectNode irNewObjectNode, Consumer<ExpressionNode> scope) {
        for (int i = 0; i < irNewObjectNode.getArgumentNodes().size(); i++) {
            int j = i;
            irNewObjectNode.getArgumentNodes().get(i).visit(this, (e) -> irNewObjectNode.getArgumentNodes().set(j, e));
        }
    }

    @Override
    public void visitNullSafeSub(NullSafeSubNode irNullSafeSubNode, Consumer<ExpressionNode> scope) {
        irNullSafeSubNode.getChildNode().visit(this, irNullSafeSubNode::setChildNode);
    }

    @Override
    public void visitStoreVariable(StoreVariableNode irStoreVariableNode, Consumer<ExpressionNode> scope) {
        irStoreVariableNode.getChildNode().visit(this, irStoreVariableNode::setChildNode);
    }

    @Override
    public void visitStoreDotDef(StoreDotDefNode irStoreDotDefNode, Consumer<ExpressionNode> scope) {
        irStoreDotDefNode.getChildNode().visit(this, irStoreDotDefNode::setChildNode);
    }

    @Override
    public void visitStoreDot(StoreDotNode irStoreDotNode, Consumer<ExpressionNode> scope) {
        irStoreDotNode.getChildNode().visit(this, irStoreDotNode::setChildNode);
    }

    @Override
    public void visitStoreDotShortcut(StoreDotShortcutNode irDotSubShortcutNode, Consumer<ExpressionNode> scope) {
        irDotSubShortcutNode.getChildNode().visit(this, irDotSubShortcutNode::setChildNode);
    }

    @Override
    public void visitStoreListShortcut(StoreListShortcutNode irStoreListShortcutNode, Consumer<ExpressionNode> scope) {
        irStoreListShortcutNode.getChildNode().visit(this, irStoreListShortcutNode::setChildNode);
    }

    @Override
    public void visitStoreMapShortcut(StoreMapShortcutNode irStoreMapShortcutNode, Consumer<ExpressionNode> scope) {
        irStoreMapShortcutNode.getChildNode().visit(this, irStoreMapShortcutNode::setChildNode);
    }

    @Override
    public void visitStoreFieldMember(StoreFieldMemberNode irStoreFieldMemberNode, Consumer<ExpressionNode> scope) {
        irStoreFieldMemberNode.getChildNode().visit(this, irStoreFieldMemberNode::setChildNode);
    }

    @Override
    public void visitStoreBraceDef(StoreBraceDefNode irStoreBraceDefNode, Consumer<ExpressionNode> scope) {
        irStoreBraceDefNode.getChildNode().visit(this, irStoreBraceDefNode::setChildNode);
    }

    @Override
    public void visitStoreBrace(StoreBraceNode irStoreBraceNode, Consumer<ExpressionNode> scope) {
        irStoreBraceNode.getChildNode().visit(this, irStoreBraceNode::setChildNode);
    }

    @Override
    public void visitInvokeCallDef(InvokeCallDefNode irInvokeCallDefNode, Consumer<ExpressionNode> scope) {
        for (int i = 0; i < irInvokeCallDefNode.getArgumentNodes().size(); i++) {
            int j = i;
            irInvokeCallDefNode.getArgumentNodes().get(i).visit(this, (e) -> irInvokeCallDefNode.getArgumentNodes().set(j, e));
        }
    }

    @Override
    public void visitInvokeCall(InvokeCallNode irInvokeCallNode, Consumer<ExpressionNode> scope) {
        for (int i = 0; i < irInvokeCallNode.getArgumentNodes().size(); i++) {
            int j = i;
            irInvokeCallNode.getArgumentNodes().get(i).visit(this, (e) -> irInvokeCallNode.getArgumentNodes().set(j, e));
        }
    }

    @Override
    public void visitInvokeCallMember(InvokeCallMemberNode irInvokeCallMemberNode, Consumer<ExpressionNode> scope) {
        for (int i = 0; i < irInvokeCallMemberNode.getArgumentNodes().size(); i++) {
            int j = i;
            irInvokeCallMemberNode.getArgumentNodes().get(i).visit(this, (e) -> irInvokeCallMemberNode.getArgumentNodes().set(j, e));
        }
        PainlessMethod method = irInvokeCallMemberNode.getDecorationValue(IRDMethod.class);
        if (method != null && method.annotations.containsKey(CompileTimeOnlyAnnotation.class)) {
            replaceCallWithConstant(irInvokeCallMemberNode, scope, method.javaMethod, null);
            return;
        }
        PainlessInstanceBinding instanceBinding = irInvokeCallMemberNode.getDecorationValue(IRDInstanceBinding.class);
        if (instanceBinding != null && instanceBinding.annotations.containsKey(CompileTimeOnlyAnnotation.class)) {
            replaceCallWithConstant(irInvokeCallMemberNode, scope, instanceBinding.javaMethod, instanceBinding.targetInstance);
            return;
        }
    }

    private void replaceCallWithConstant(
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

    @Override
    public void visitFlipArrayIndex(FlipArrayIndexNode irFlipArrayIndexNode, Consumer<ExpressionNode> scope) {
        irFlipArrayIndexNode.getChildNode().visit(this, irFlipArrayIndexNode::setChildNode);
    }

    @Override
    public void visitFlipCollectionIndex(FlipCollectionIndexNode irFlipCollectionIndexNode, Consumer<ExpressionNode> scope) {
        irFlipCollectionIndexNode.getChildNode().visit(this, irFlipCollectionIndexNode::setChildNode);
    }

    @Override
    public void visitFlipDefIndex(FlipDefIndexNode irFlipDefIndexNode, Consumer<ExpressionNode> scope) {
        irFlipDefIndexNode.getChildNode().visit(this, irFlipDefIndexNode::setChildNode);
    }

    @Override
    public void visitDup(DupNode irDupNode, Consumer<ExpressionNode> scope) {
        irDupNode.getChildNode().visit(this, irDupNode::setChildNode);
    }
}
