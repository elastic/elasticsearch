/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless.phase;

import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.ir.AssignmentNode;
import org.elasticsearch.painless.ir.BinaryMathNode;
import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.BooleanNode;
import org.elasticsearch.painless.ir.BraceNode;
import org.elasticsearch.painless.ir.BraceSubDefNode;
import org.elasticsearch.painless.ir.BraceSubNode;
import org.elasticsearch.painless.ir.BreakNode;
import org.elasticsearch.painless.ir.CallNode;
import org.elasticsearch.painless.ir.CallSubDefNode;
import org.elasticsearch.painless.ir.CallSubNode;
import org.elasticsearch.painless.ir.CastNode;
import org.elasticsearch.painless.ir.CatchNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ComparisonNode;
import org.elasticsearch.painless.ir.ConditionNode;
import org.elasticsearch.painless.ir.ConditionalNode;
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.ir.ContinueNode;
import org.elasticsearch.painless.ir.DeclarationBlockNode;
import org.elasticsearch.painless.ir.DeclarationNode;
import org.elasticsearch.painless.ir.DefInterfaceReferenceNode;
import org.elasticsearch.painless.ir.DoWhileLoopNode;
import org.elasticsearch.painless.ir.DotNode;
import org.elasticsearch.painless.ir.DotSubArrayLengthNode;
import org.elasticsearch.painless.ir.DotSubDefNode;
import org.elasticsearch.painless.ir.DotSubNode;
import org.elasticsearch.painless.ir.DotSubShortcutNode;
import org.elasticsearch.painless.ir.ElvisNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.FieldNode;
import org.elasticsearch.painless.ir.ForEachLoopNode;
import org.elasticsearch.painless.ir.ForEachSubArrayNode;
import org.elasticsearch.painless.ir.ForEachSubIterableNode;
import org.elasticsearch.painless.ir.ForLoopNode;
import org.elasticsearch.painless.ir.FunctionNode;
import org.elasticsearch.painless.ir.IRNode;
import org.elasticsearch.painless.ir.IfElseNode;
import org.elasticsearch.painless.ir.IfNode;
import org.elasticsearch.painless.ir.InstanceofNode;
import org.elasticsearch.painless.ir.ListInitializationNode;
import org.elasticsearch.painless.ir.ListSubShortcutNode;
import org.elasticsearch.painless.ir.MapInitializationNode;
import org.elasticsearch.painless.ir.MapSubShortcutNode;
import org.elasticsearch.painless.ir.MemberCallNode;
import org.elasticsearch.painless.ir.MemberFieldLoadNode;
import org.elasticsearch.painless.ir.MemberFieldStoreNode;
import org.elasticsearch.painless.ir.NewArrayNode;
import org.elasticsearch.painless.ir.NewObjectNode;
import org.elasticsearch.painless.ir.NullNode;
import org.elasticsearch.painless.ir.NullSafeSubNode;
import org.elasticsearch.painless.ir.ReferenceNode;
import org.elasticsearch.painless.ir.ReturnNode;
import org.elasticsearch.painless.ir.StatementExpressionNode;
import org.elasticsearch.painless.ir.StatementNode;
import org.elasticsearch.painless.ir.StaticNode;
import org.elasticsearch.painless.ir.ThrowNode;
import org.elasticsearch.painless.ir.TryNode;
import org.elasticsearch.painless.ir.TypedCaptureReferenceNode;
import org.elasticsearch.painless.ir.TypedInterfaceReferenceNode;
import org.elasticsearch.painless.ir.UnaryMathNode;
import org.elasticsearch.painless.ir.VariableNode;
import org.elasticsearch.painless.ir.WhileLoopNode;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessClassBinding;
import org.elasticsearch.painless.lookup.PainlessInstanceBinding;
import org.elasticsearch.painless.lookup.PainlessMethod;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.node.AExpression;
import org.elasticsearch.painless.node.ANode;
import org.elasticsearch.painless.node.AStatement;
import org.elasticsearch.painless.node.EAssignment;
import org.elasticsearch.painless.node.EBinary;
import org.elasticsearch.painless.node.EBooleanComp;
import org.elasticsearch.painless.node.EBooleanConstant;
import org.elasticsearch.painless.node.EBrace;
import org.elasticsearch.painless.node.ECall;
import org.elasticsearch.painless.node.ECallLocal;
import org.elasticsearch.painless.node.EComp;
import org.elasticsearch.painless.node.EConditional;
import org.elasticsearch.painless.node.EDecimal;
import org.elasticsearch.painless.node.EDot;
import org.elasticsearch.painless.node.EElvis;
import org.elasticsearch.painless.node.EExplicit;
import org.elasticsearch.painless.node.EFunctionRef;
import org.elasticsearch.painless.node.EInstanceof;
import org.elasticsearch.painless.node.ELambda;
import org.elasticsearch.painless.node.EListInit;
import org.elasticsearch.painless.node.EMapInit;
import org.elasticsearch.painless.node.ENewArray;
import org.elasticsearch.painless.node.ENewArrayFunctionRef;
import org.elasticsearch.painless.node.ENewObj;
import org.elasticsearch.painless.node.ENull;
import org.elasticsearch.painless.node.ENumeric;
import org.elasticsearch.painless.node.ERegex;
import org.elasticsearch.painless.node.EString;
import org.elasticsearch.painless.node.ESymbol;
import org.elasticsearch.painless.node.EUnary;
import org.elasticsearch.painless.node.SBlock;
import org.elasticsearch.painless.node.SBreak;
import org.elasticsearch.painless.node.SCatch;
import org.elasticsearch.painless.node.SClass;
import org.elasticsearch.painless.node.SContinue;
import org.elasticsearch.painless.node.SDeclBlock;
import org.elasticsearch.painless.node.SDeclaration;
import org.elasticsearch.painless.node.SDo;
import org.elasticsearch.painless.node.SEach;
import org.elasticsearch.painless.node.SExpression;
import org.elasticsearch.painless.node.SFor;
import org.elasticsearch.painless.node.SFunction;
import org.elasticsearch.painless.node.SIf;
import org.elasticsearch.painless.node.SIfElse;
import org.elasticsearch.painless.node.SReturn;
import org.elasticsearch.painless.node.SThrow;
import org.elasticsearch.painless.node.STry;
import org.elasticsearch.painless.node.SWhile;
import org.elasticsearch.painless.symbol.Decorations.AllEscape;
import org.elasticsearch.painless.symbol.Decorations.BinaryType;
import org.elasticsearch.painless.symbol.Decorations.CapturesDecoration;
import org.elasticsearch.painless.symbol.Decorations.ComparisonType;
import org.elasticsearch.painless.symbol.Decorations.CompoundType;
import org.elasticsearch.painless.symbol.Decorations.Concatenate;
import org.elasticsearch.painless.symbol.Decorations.ContinuousLoop;
import org.elasticsearch.painless.symbol.Decorations.DowncastPainlessCast;
import org.elasticsearch.painless.symbol.Decorations.EncodingDecoration;
import org.elasticsearch.painless.symbol.Decorations.Explicit;
import org.elasticsearch.painless.symbol.Decorations.ExpressionPainlessCast;
import org.elasticsearch.painless.symbol.Decorations.GetterPainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.InstanceType;
import org.elasticsearch.painless.symbol.Decorations.IterablePainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.ListShortcut;
import org.elasticsearch.painless.symbol.Decorations.MapShortcut;
import org.elasticsearch.painless.symbol.Decorations.MethodEscape;
import org.elasticsearch.painless.symbol.Decorations.MethodNameDecoration;
import org.elasticsearch.painless.symbol.Decorations.ParameterNames;
import org.elasticsearch.painless.symbol.Decorations.Read;
import org.elasticsearch.painless.symbol.Decorations.ReferenceDecoration;
import org.elasticsearch.painless.symbol.Decorations.ReturnType;
import org.elasticsearch.painless.symbol.Decorations.SemanticVariable;
import org.elasticsearch.painless.symbol.Decorations.SetterPainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.ShiftType;
import org.elasticsearch.painless.symbol.Decorations.Shortcut;
import org.elasticsearch.painless.symbol.Decorations.StandardConstant;
import org.elasticsearch.painless.symbol.Decorations.StandardLocalFunction;
import org.elasticsearch.painless.symbol.Decorations.StandardPainlessClassBinding;
import org.elasticsearch.painless.symbol.Decorations.StandardPainlessConstructor;
import org.elasticsearch.painless.symbol.Decorations.StandardPainlessField;
import org.elasticsearch.painless.symbol.Decorations.StandardPainlessInstanceBinding;
import org.elasticsearch.painless.symbol.Decorations.StandardPainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.StaticType;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.TypeParameters;
import org.elasticsearch.painless.symbol.Decorations.UnaryType;
import org.elasticsearch.painless.symbol.Decorations.UpcastPainlessCast;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.SemanticScope.Variable;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

public class UserTreeToIRTreeVisitor implements UserTreeVisitor<ScriptScope, IRNode> {

    private ClassNode irClassNode;

    protected IRNode visit(ANode userNode, ScriptScope scriptScope) {
        return userNode == null ? null : userNode.visit(this, scriptScope);
    }

    protected ExpressionNode injectCast(AExpression userExpressionNode, ScriptScope scriptScope) {
        ExpressionNode irExpressionNode = (ExpressionNode)visit(userExpressionNode, scriptScope);

        if (irExpressionNode == null) {
            return null;
        }

        ExpressionPainlessCast expressionPainlessCast = scriptScope.getDecoration(userExpressionNode, ExpressionPainlessCast.class);

        if (expressionPainlessCast == null) {
            return irExpressionNode;
        }

        CastNode castNode = new CastNode();
        castNode.setLocation(irExpressionNode.getLocation());
        castNode.setExpressionType(expressionPainlessCast.getExpressionPainlessCast().targetType);
        castNode.setCast(expressionPainlessCast.getExpressionPainlessCast());
        castNode.setChildNode(irExpressionNode);

        return castNode;
    }

    @Override
    public IRNode visitClass(SClass userClassNode, ScriptScope scriptScope) {
        irClassNode = new ClassNode();

        for (SFunction userFunctionNode : userClassNode.getFunctionNodes()) {
            irClassNode.addFunctionNode((FunctionNode)visit(userFunctionNode, scriptScope));
        }

        irClassNode.setLocation(irClassNode.getLocation());
        irClassNode.setScriptScope(scriptScope);

        return irClassNode;
    }

    @Override
    public IRNode visitFunction(SFunction userFunctionNode, ScriptScope scriptScope) {
        String functionName = userFunctionNode.getFunctionName();
        int functionArity = userFunctionNode.getCanonicalTypeNameParameters().size();
        LocalFunction localFunction = scriptScope.getFunctionTable().getFunction(functionName, functionArity);
        Class<?> returnType = localFunction.getReturnType();
        boolean methodEscape = scriptScope.getCondition(userFunctionNode, MethodEscape.class);

        BlockNode irBlockNode = (BlockNode)visit(userFunctionNode.getBlockNode(), scriptScope);

        if (methodEscape == false) {
            ExpressionNode irExpressionNode;

            if (returnType == void.class) {
                irExpressionNode = null;
            } else if (userFunctionNode.isAutoReturnEnabled()) {
                if (returnType.isPrimitive()) {
                    ConstantNode constantNode = new ConstantNode();
                    constantNode.setLocation(userFunctionNode.getLocation());
                    constantNode.setExpressionType(returnType);

                    if (returnType == boolean.class) {
                        constantNode.setConstant(false);
                    } else if (returnType == byte.class
                            || returnType == char.class
                            || returnType == short.class
                            || returnType == int.class) {
                        constantNode.setConstant(0);
                    } else if (returnType == long.class) {
                        constantNode.setConstant(0L);
                    } else if (returnType == float.class) {
                        constantNode.setConstant(0f);
                    } else if (returnType == double.class) {
                        constantNode.setConstant(0d);
                    } else {
                        throw userFunctionNode.createError(new IllegalStateException("illegal tree structure"));
                    }

                    irExpressionNode = constantNode;
                } else {
                    irExpressionNode = new NullNode();
                    irExpressionNode.setLocation(userFunctionNode.getLocation());
                    irExpressionNode.setExpressionType(returnType);
                }
            } else {
                throw userFunctionNode.createError(new IllegalStateException("illegal tree structure"));
            }

            ReturnNode irReturnNode = new ReturnNode();
            irReturnNode.setLocation(userFunctionNode.getLocation());
            irReturnNode.setExpressionNode(irExpressionNode);

            irBlockNode.addStatementNode(irReturnNode);
        }

        FunctionNode irFunctionNode = new FunctionNode();
        irFunctionNode.setBlockNode(irBlockNode);
        irFunctionNode.setLocation(userFunctionNode.getLocation());
        irFunctionNode.setName(userFunctionNode.getFunctionName());
        irFunctionNode.setReturnType(returnType);
        irFunctionNode.getTypeParameters().addAll(localFunction.getTypeParameters());
        irFunctionNode.getParameterNames().addAll(userFunctionNode.getParameterNames());
        irFunctionNode.setStatic(userFunctionNode.isStatic());
        irFunctionNode.setVarArgs(false);
        irFunctionNode.setSynthetic(userFunctionNode.isSynthetic());
        irFunctionNode.setMaxLoopCounter(scriptScope.getCompilerSettings().getMaxLoopCounter());

        return irFunctionNode;
    }

    @Override
    public IRNode visitBlock(SBlock userBlockNode, ScriptScope scriptScope) {
        BlockNode irBlockNode = new BlockNode();

        for (AStatement userStatementNode : userBlockNode.getStatementNodes()) {
            irBlockNode.addStatementNode((StatementNode)visit(userStatementNode, scriptScope));
        }

        irBlockNode.setLocation(userBlockNode.getLocation());
        irBlockNode.setAllEscape(scriptScope.getCondition(userBlockNode, AllEscape.class));

        return irBlockNode;
    }

    @Override
    public IRNode visitIf(SIf userIfNode, ScriptScope scriptScope) {
        IfNode irIfNode = new IfNode();
        irIfNode.setConditionNode(injectCast(userIfNode.getConditionNode(), scriptScope));
        irIfNode.setBlockNode((BlockNode)visit(userIfNode.getIfblockNode(), scriptScope));
        irIfNode.setLocation(userIfNode.getLocation());

        return irIfNode;
    }

    @Override
    public IRNode visitIfElse(SIfElse userIfElseNode, ScriptScope scriptScope) {
        IfElseNode irIfElseNode = new IfElseNode();
        irIfElseNode.setConditionNode(injectCast(userIfElseNode.getConditionNode(), scriptScope));
        irIfElseNode.setBlockNode((BlockNode)visit(userIfElseNode.getIfblockNode(), scriptScope));
        irIfElseNode.setElseBlockNode((BlockNode)visit(userIfElseNode.getElseblockNode(), scriptScope));
        irIfElseNode.setLocation(userIfElseNode.getLocation());

        return irIfElseNode;
    }

    @Override
    public IRNode visitWhile(SWhile userWhileNode, ScriptScope scriptScope) {
        WhileLoopNode irWhileLoopNode = new WhileLoopNode();
        irWhileLoopNode.setConditionNode(injectCast(userWhileNode.getConditionNode(), scriptScope));
        irWhileLoopNode.setBlockNode((BlockNode)visit(userWhileNode.getBlockNode(), scriptScope));
        irWhileLoopNode.setLocation(userWhileNode.getLocation());
        irWhileLoopNode.setContinuous(scriptScope.getCondition(userWhileNode, ContinuousLoop.class));

        return irWhileLoopNode;
    }

    @Override
    public IRNode visitDo(SDo userDoNode, ScriptScope scriptScope) {
        DoWhileLoopNode irDoWhileLoopNode = new DoWhileLoopNode();
        irDoWhileLoopNode.setConditionNode(injectCast(userDoNode.getConditionNode(), scriptScope));
        irDoWhileLoopNode.setBlockNode((BlockNode)visit(userDoNode.getBlockNode(), scriptScope));
        irDoWhileLoopNode.setLocation(userDoNode.getLocation());
        irDoWhileLoopNode.setContinuous(scriptScope.getCondition(userDoNode, ContinuousLoop.class));

        return irDoWhileLoopNode;
    }

    @Override
    public IRNode visitFor(SFor userForNode, ScriptScope scriptScope) {
        ForLoopNode irForLoopNode = new ForLoopNode();
        irForLoopNode.setInitialzerNode(visit(userForNode.getInitializerNode(), scriptScope));
        irForLoopNode.setConditionNode(injectCast(userForNode.getConditionNode(), scriptScope));
        irForLoopNode.setAfterthoughtNode((ExpressionNode)visit(userForNode.getAfterthoughtNode(), scriptScope));
        irForLoopNode.setBlockNode((BlockNode)visit(userForNode.getBlockNode(), scriptScope));
        irForLoopNode.setLocation(userForNode.getLocation());
        irForLoopNode.setContinuous(scriptScope.getCondition(userForNode, ContinuousLoop.class));

        return irForLoopNode;
    }

    @Override
    public IRNode visitEach(SEach userEachNode, ScriptScope scriptScope) {
        Variable variable = scriptScope.getDecoration(userEachNode, SemanticVariable.class).getSemanticVariable();
        PainlessCast painlessCast = scriptScope.hasDecoration(userEachNode, ExpressionPainlessCast.class) ?
                scriptScope.getDecoration(userEachNode, ExpressionPainlessCast.class).getExpressionPainlessCast() : null;
        ExpressionNode irIterableNode = (ExpressionNode)visit(userEachNode.getIterableNode(), scriptScope);
        Class<?> iterableValueType = scriptScope.getDecoration(userEachNode.getIterableNode(), ValueType.class).getValueType();
        BlockNode irBlockNode = (BlockNode)visit(userEachNode.getBlockNode(), scriptScope);

        ConditionNode irConditionNode;

        if (iterableValueType.isArray()) {
            ForEachSubArrayNode irForEachSubArrayNode = new ForEachSubArrayNode();
            irForEachSubArrayNode.setConditionNode(irIterableNode);
            irForEachSubArrayNode.setBlockNode(irBlockNode);
            irForEachSubArrayNode.setLocation(userEachNode.getLocation());
            irForEachSubArrayNode.setVariableType(variable.getType());
            irForEachSubArrayNode.setVariableName(variable.getName());
            irForEachSubArrayNode.setCast(painlessCast);
            irForEachSubArrayNode.setArrayType(iterableValueType);
            irForEachSubArrayNode.setArrayName("#array" + userEachNode.getLocation().getOffset());
            irForEachSubArrayNode.setIndexType(int.class);
            irForEachSubArrayNode.setIndexName("#index" + userEachNode.getLocation().getOffset());
            irForEachSubArrayNode.setIndexedType(iterableValueType.getComponentType());
            irForEachSubArrayNode.setContinuous(false);
            irConditionNode = irForEachSubArrayNode;
        } else if (iterableValueType == def.class || Iterable.class.isAssignableFrom(iterableValueType)) {
            ForEachSubIterableNode irForEachSubIterableNode = new ForEachSubIterableNode();
            irForEachSubIterableNode.setConditionNode(irIterableNode);
            irForEachSubIterableNode.setBlockNode(irBlockNode);
            irForEachSubIterableNode.setLocation(userEachNode.getLocation());
            irForEachSubIterableNode.setVariableType(variable.getType());
            irForEachSubIterableNode.setVariableName(variable.getName());
            irForEachSubIterableNode.setCast(painlessCast);
            irForEachSubIterableNode.setIteratorType(Iterator.class);
            irForEachSubIterableNode.setIteratorName("#itr" + userEachNode.getLocation().getOffset());
            irForEachSubIterableNode.setMethod(iterableValueType == def.class ? null :
                    scriptScope.getDecoration(userEachNode, IterablePainlessMethod.class).getIterablePainlessMethod());
            irForEachSubIterableNode.setContinuous(false);
            irConditionNode = irForEachSubIterableNode;
        } else {
            throw userEachNode.createError(new IllegalStateException("illegal tree structure"));
        }

        ForEachLoopNode irForEachLoopNode = new ForEachLoopNode();
        irForEachLoopNode.setConditionNode(irConditionNode);
        irForEachLoopNode.setLocation(userEachNode.getLocation());

        return irForEachLoopNode;
    }

    @Override
    public IRNode visitDeclBlock(SDeclBlock userDeclBlockNode, ScriptScope scriptScope) {
        DeclarationBlockNode irDeclarationBlockNode = new DeclarationBlockNode();

        for (SDeclaration userDeclarationNode : userDeclBlockNode.getDeclarationNodes()) {
            irDeclarationBlockNode.addDeclarationNode((DeclarationNode)visit(userDeclarationNode, scriptScope));
        }

        irDeclarationBlockNode.setLocation(userDeclBlockNode.getLocation());

        return irDeclarationBlockNode;
    }

    @Override
    public IRNode visitDeclaration(SDeclaration userDeclarationNode, ScriptScope scriptScope) {
        Variable variable = scriptScope.getDecoration(userDeclarationNode, SemanticVariable.class).getSemanticVariable();

        DeclarationNode declarationNode = new DeclarationNode();
        declarationNode.setExpressionNode(injectCast(userDeclarationNode.getValueNode(), scriptScope));
        declarationNode.setLocation(userDeclarationNode.getLocation());
        declarationNode.setDeclarationType(variable.getType());
        declarationNode.setName(variable.getName());

        return declarationNode;
    }

    @Override
    public IRNode visitReturn(SReturn userReturnNode, ScriptScope scriptScope) {
        ReturnNode irReturnNode = new ReturnNode();
        irReturnNode.setExpressionNode(injectCast(userReturnNode.getExpressionNode(), scriptScope));
        irReturnNode.setLocation(userReturnNode.getLocation());

        return irReturnNode;
    }

    @Override
    public IRNode visitExpression(SExpression userExpressionNode, ScriptScope scriptScope) {
        StatementNode irStatementNode;
        ExpressionNode irExpressionNode = injectCast(userExpressionNode.getExpressionNode(), scriptScope);

        if (scriptScope.getCondition(userExpressionNode, MethodEscape.class)) {
            ReturnNode returnNode = new ReturnNode();
            returnNode.setExpressionNode(irExpressionNode);
            returnNode.setLocation(userExpressionNode.getLocation());
            irStatementNode = returnNode;
        } else {
            StatementExpressionNode statementExpressionNode = new StatementExpressionNode();
            statementExpressionNode.setExpressionNode(irExpressionNode);
            statementExpressionNode.setLocation(userExpressionNode.getLocation());
            irStatementNode = statementExpressionNode;
        }

        return irStatementNode;
    }

    @Override
    public IRNode visitTry(STry userTryNode, ScriptScope scriptScope) {
        TryNode irTryNode = new TryNode();

        for (SCatch userCatchNode : userTryNode.getCatchNodes()) {
            irTryNode.addCatchNode((CatchNode)visit(userCatchNode, scriptScope));
        }

        irTryNode.setBlockNode((BlockNode)visit(userTryNode.getBlockNode(), scriptScope));
        irTryNode.setLocation(userTryNode.getLocation());

        return irTryNode;
    }

    @Override
    public IRNode visitCatch(SCatch userCatchNode, ScriptScope scriptScope) {
        Variable variable = scriptScope.getDecoration(userCatchNode, SemanticVariable.class).getSemanticVariable();

        CatchNode irCatchNode = new CatchNode();
        irCatchNode.setExceptionType(variable.getType());
        irCatchNode.setSymbol(variable.getName());
        irCatchNode.setBlockNode((BlockNode)visit(userCatchNode.getBlockNode(), scriptScope));
        irCatchNode.setLocation(userCatchNode.getLocation());

        return irCatchNode;
    }

    @Override
    public IRNode visitThrow(SThrow userThrowNode, ScriptScope scriptScope) {
        ThrowNode irThrowNode = new ThrowNode();
        irThrowNode.setExpressionNode(injectCast(userThrowNode.getExpressionNode(), scriptScope));
        irThrowNode.setLocation(userThrowNode.getLocation());

        return irThrowNode;
    }

    @Override
    public IRNode visitContinue(SContinue userContinueNode, ScriptScope scriptScope) {
        ContinueNode irContinueNode = new ContinueNode();
        irContinueNode.setLocation(userContinueNode.getLocation());

        return irContinueNode;
    }

    @Override
    public IRNode visitBreak(SBreak userBreakNode, ScriptScope scriptScope) {
        BreakNode irBreakNode = new BreakNode();
        irBreakNode.setLocation(userBreakNode.getLocation());

        return irBreakNode;
    }

    @Override
    public IRNode visitAssignment(EAssignment userAssignmentNode, ScriptScope scriptScope) {
        Class<?> compoundType = scriptScope.hasDecoration(userAssignmentNode, CompoundType.class) ?
                scriptScope.getDecoration(userAssignmentNode, CompoundType.class).getCompoundType() : null;
        PainlessCast upcast = scriptScope.hasDecoration(userAssignmentNode, UpcastPainlessCast.class) ?
                scriptScope.getDecoration(userAssignmentNode, UpcastPainlessCast.class).getUpcastPainlessCast() : null;
        PainlessCast downcast = scriptScope.hasDecoration(userAssignmentNode, DowncastPainlessCast.class) ?
                scriptScope.getDecoration(userAssignmentNode, DowncastPainlessCast.class).getDowncastPainlessCast() : null;

        AssignmentNode irAssignmentNode = new AssignmentNode();
        irAssignmentNode.setLeftNode((ExpressionNode)visit(userAssignmentNode.getLeftNode(), scriptScope));
        irAssignmentNode.setRightNode(injectCast(userAssignmentNode.getRightNode(), scriptScope));
        irAssignmentNode.setLocation(userAssignmentNode.getLocation());
        irAssignmentNode.setExpressionType(scriptScope.getDecoration(userAssignmentNode, ValueType.class).getValueType());
        irAssignmentNode.setCompoundType(compoundType);
        irAssignmentNode.setPost(userAssignmentNode.postIfRead());
        irAssignmentNode.setOperation(userAssignmentNode.getOperation());
        irAssignmentNode.setRead(scriptScope.getCondition(userAssignmentNode, Read.class));
        irAssignmentNode.setCat(scriptScope.getCondition(userAssignmentNode, Concatenate.class));
        irAssignmentNode.setThere(upcast);
        irAssignmentNode.setBack(downcast);

        return irAssignmentNode;
    }

    @Override
    public IRNode visitUnary(EUnary userUnaryNode, ScriptScope scriptScope) {
        Class<?> unaryType = scriptScope.hasDecoration(userUnaryNode, UnaryType.class) ?
                scriptScope.getDecoration(userUnaryNode, UnaryType.class).getUnaryType() : null;

        IRNode irNode;

        if ((userUnaryNode.getOperation() == Operation.ADD || userUnaryNode.getOperation() == Operation.SUB) && unaryType == null) {
            irNode = visit(userUnaryNode.getChildNode(), scriptScope);
        } else {
            UnaryMathNode irUnaryMathNode = new UnaryMathNode();
            irUnaryMathNode.setLocation(userUnaryNode.getLocation());
            irUnaryMathNode.setExpressionType(scriptScope.getDecoration(userUnaryNode, ValueType.class).getValueType());
            irUnaryMathNode.setUnaryType(unaryType);
            irUnaryMathNode.setOperation(userUnaryNode.getOperation());
            irUnaryMathNode.setOriginallyExplicit(scriptScope.getCondition(userUnaryNode, Explicit.class));
            irUnaryMathNode.setChildNode(injectCast(userUnaryNode.getChildNode(), scriptScope));
            irNode = irUnaryMathNode;
        }

        return irNode;
    }

    @Override
    public IRNode visitBinary(EBinary userBinaryNode, ScriptScope scriptScope) {
        Class<?> shiftType = scriptScope.hasDecoration(userBinaryNode, ShiftType.class) ?
                scriptScope.getDecoration(userBinaryNode, ShiftType.class).getShiftType() : null;

        BinaryMathNode irBinaryMathNode = new BinaryMathNode();
        irBinaryMathNode.setLocation(userBinaryNode.getLocation());
        irBinaryMathNode.setExpressionType(scriptScope.getDecoration(userBinaryNode, ValueType.class).getValueType());
        irBinaryMathNode.setBinaryType(scriptScope.getDecoration(userBinaryNode, BinaryType.class).getBinaryType());
        irBinaryMathNode.setShiftType(shiftType);
        irBinaryMathNode.setOperation(userBinaryNode.getOperation());
        irBinaryMathNode.setCat(scriptScope.getCondition(userBinaryNode, Concatenate.class));
        irBinaryMathNode.setOriginallyExplicit(scriptScope.getCondition(userBinaryNode, Explicit.class));
        irBinaryMathNode.setLeftNode(injectCast(userBinaryNode.getLeftNode(), scriptScope));
        irBinaryMathNode.setRightNode(injectCast(userBinaryNode.getRightNode(), scriptScope));

        return irBinaryMathNode;
    }

    @Override
    public IRNode visitBool(EBooleanComp userBoolNode, ScriptScope scriptScope) {
        BooleanNode irBooleanNode = new BooleanNode();
        irBooleanNode.setLocation(userBoolNode.getLocation());
        irBooleanNode.setExpressionType(scriptScope.getDecoration(userBoolNode, ValueType.class).getValueType());
        irBooleanNode.setOperation(userBoolNode.getOperation());
        irBooleanNode.setLeftNode(injectCast(userBoolNode.getLeftNode(), scriptScope));
        irBooleanNode.setRightNode(injectCast(userBoolNode.getRightNode(), scriptScope));

        return irBooleanNode;
    }

    @Override
    public IRNode visitComp(EComp userCompNode, ScriptScope scriptScope) {
        ComparisonNode irComparisonNode = new ComparisonNode();
        irComparisonNode.setLocation(userCompNode.getLocation());
        irComparisonNode.setExpressionType(scriptScope.getDecoration(userCompNode, ValueType.class).getValueType());
        irComparisonNode.setComparisonType(scriptScope.getDecoration(userCompNode, ComparisonType.class).getComparisonType());
        irComparisonNode.setOperation(userCompNode.getOperation());
        irComparisonNode.setLeftNode(injectCast(userCompNode.getLeftNode(), scriptScope));
        irComparisonNode.setRightNode(injectCast(userCompNode.getRightNode(), scriptScope));

        return irComparisonNode;
    }

    @Override
    public IRNode visitExplicit(EExplicit userExplicitNode, ScriptScope scriptScope) {
        return injectCast(userExplicitNode.getChildNode(), scriptScope);
    }

    @Override
    public IRNode visitInstanceof(EInstanceof userInstanceofNode, ScriptScope scriptScope) {
        InstanceofNode irInstanceofNode = new InstanceofNode();
        irInstanceofNode.setLocation(userInstanceofNode.getLocation());
        irInstanceofNode.setExpressionType(scriptScope.getDecoration(userInstanceofNode, ValueType.class).getValueType());
        irInstanceofNode.setInstanceType(scriptScope.getDecoration(userInstanceofNode, InstanceType.class).getInstanceType());
        irInstanceofNode.setChildNode((ExpressionNode)visit(userInstanceofNode.getExpressionNode(), scriptScope));

        return irInstanceofNode;
    }

    @Override
    public IRNode visitConditional(EConditional userConditionalNode, ScriptScope scriptScope) {
        ConditionalNode irConditionalNode = new ConditionalNode();
        irConditionalNode.setLocation(userConditionalNode.getLocation());
        irConditionalNode.setExpressionType(scriptScope.getDecoration(userConditionalNode, ValueType.class).getValueType());
        irConditionalNode.setConditionNode(injectCast(userConditionalNode.getConditionNode(), scriptScope));
        irConditionalNode.setLeftNode(injectCast(userConditionalNode.getTrueNode(), scriptScope));
        irConditionalNode.setRightNode(injectCast(userConditionalNode.getFalseNode(), scriptScope));

        return irConditionalNode;
    }

    @Override
    public IRNode visitElvis(EElvis userElvisNode, ScriptScope scriptScope) {
        ElvisNode irElvisNode = new ElvisNode();
        irElvisNode.setLocation(userElvisNode.getLocation());
        irElvisNode.setExpressionType(scriptScope.getDecoration(userElvisNode, ValueType.class).getValueType());
        irElvisNode.setLeftNode(injectCast(userElvisNode.getLeftNode(), scriptScope));
        irElvisNode.setRightNode(injectCast(userElvisNode.getRightNode(), scriptScope));

        return irElvisNode;
    }

    @Override
    public IRNode visitListInit(EListInit userListInitNode, ScriptScope scriptScope) {
        ListInitializationNode irListInitializationNode = new ListInitializationNode();

        irListInitializationNode.setLocation(userListInitNode.getLocation());
        irListInitializationNode.setExpressionType(scriptScope.getDecoration(userListInitNode, ValueType.class).getValueType());
        irListInitializationNode.setConstructor(
                scriptScope.getDecoration(userListInitNode, StandardPainlessConstructor.class).getStandardPainlessConstructor());
        irListInitializationNode.setMethod(
                scriptScope.getDecoration(userListInitNode, StandardPainlessMethod.class).getStandardPainlessMethod());

        for (AExpression userValueNode : userListInitNode.getValueNodes()) {
            irListInitializationNode.addArgumentNode(injectCast(userValueNode, scriptScope));
        }

        return irListInitializationNode;
    }

    @Override
    public IRNode visitMapInit(EMapInit userMapInitNode, ScriptScope scriptScope) {
        MapInitializationNode irMapInitializationNode = new MapInitializationNode();

        irMapInitializationNode.setLocation(userMapInitNode.getLocation());
        irMapInitializationNode.setExpressionType(scriptScope.getDecoration(userMapInitNode, ValueType.class).getValueType());
        irMapInitializationNode.setConstructor(
                scriptScope.getDecoration(userMapInitNode, StandardPainlessConstructor.class).getStandardPainlessConstructor());
        irMapInitializationNode.setMethod(
                scriptScope.getDecoration(userMapInitNode, StandardPainlessMethod.class).getStandardPainlessMethod());


        for (int i = 0; i < userMapInitNode.getKeyNodes().size(); ++i) {
            irMapInitializationNode.addArgumentNode(
                    injectCast(userMapInitNode.getKeyNodes().get(i), scriptScope),
                    injectCast(userMapInitNode.getValueNodes().get(i), scriptScope));
        }

        return irMapInitializationNode;
    }

    @Override
    public IRNode visitNewArray(ENewArray userNewArrayNode, ScriptScope scriptScope) {
        NewArrayNode irNewArrayNode = new NewArrayNode();

        irNewArrayNode.setLocation(userNewArrayNode.getLocation());
        irNewArrayNode.setExpressionType(scriptScope.getDecoration(userNewArrayNode, ValueType.class).getValueType());
        irNewArrayNode.setInitialize(userNewArrayNode.isInitializer());

        for (AExpression userArgumentNode : userNewArrayNode.getValueNodes()) {
            irNewArrayNode.addArgumentNode(injectCast(userArgumentNode, scriptScope));
        }

        return irNewArrayNode;
    }

    @Override
    public IRNode visitNewObj(ENewObj userNewObjectNode, ScriptScope scriptScope) {
        NewObjectNode irNewObjectNode = new NewObjectNode();

        irNewObjectNode.setLocation(userNewObjectNode.getLocation());
        irNewObjectNode.setExpressionType(scriptScope.getDecoration(userNewObjectNode, ValueType.class).getValueType());
        irNewObjectNode.setRead(scriptScope.getCondition(userNewObjectNode, Read.class));
        irNewObjectNode.setConstructor(
                scriptScope.getDecoration(userNewObjectNode, StandardPainlessConstructor.class).getStandardPainlessConstructor());

        for (AExpression userArgumentNode : userNewObjectNode.getArgumentNodes()) {
            irNewObjectNode.addArgumentNode(injectCast(userArgumentNode, scriptScope));
        }

        return irNewObjectNode;
    }

    @Override
    public IRNode visitCallLocal(ECallLocal callLocalNode, ScriptScope scriptScope) {
        MemberCallNode irMemberCallNode = new MemberCallNode();

        if (scriptScope.hasDecoration(callLocalNode, StandardLocalFunction.class)) {
            irMemberCallNode.setLocalFunction(scriptScope.getDecoration(callLocalNode, StandardLocalFunction.class).getLocalFunction());
        } else if (scriptScope.hasDecoration(callLocalNode, StandardPainlessMethod.class)) {
            irMemberCallNode.setImportedMethod(
                    scriptScope.getDecoration(callLocalNode, StandardPainlessMethod.class).getStandardPainlessMethod());
        } else if (scriptScope.hasDecoration(callLocalNode, StandardPainlessClassBinding.class)) {
            PainlessClassBinding painlessClassBinding =
                    scriptScope.getDecoration(callLocalNode, StandardPainlessClassBinding.class).getPainlessClassBinding();
            String bindingName = scriptScope.getNextSyntheticName("class_binding");

            FieldNode irFieldNode = new FieldNode();
            irFieldNode.setLocation(callLocalNode.getLocation());
            irFieldNode.setModifiers(Modifier.PRIVATE);
            irFieldNode.setFieldType(painlessClassBinding.javaConstructor.getDeclaringClass());
            irFieldNode.setName(bindingName);
            irClassNode.addFieldNode(irFieldNode);

            irMemberCallNode.setClassBinding(painlessClassBinding);
            irMemberCallNode.setClassBindingOffset(
                    (int)scriptScope.getDecoration(callLocalNode, StandardConstant.class).getStandardConstant());
            irMemberCallNode.setBindingName(bindingName);
        } else if (scriptScope.hasDecoration(callLocalNode, StandardPainlessInstanceBinding.class)) {
            PainlessInstanceBinding painlessInstanceBinding =
                    scriptScope.getDecoration(callLocalNode, StandardPainlessInstanceBinding.class).getPainlessInstanceBinding();
            String bindingName = scriptScope.getNextSyntheticName("instance_binding");

            FieldNode irFieldNode = new FieldNode();
            irFieldNode.setLocation(callLocalNode.getLocation());
            irFieldNode.setModifiers(Modifier.PUBLIC | Modifier.STATIC);
            irFieldNode.setFieldType(painlessInstanceBinding.targetInstance.getClass());
            irFieldNode.setName(bindingName);
            irClassNode.addFieldNode(irFieldNode);

            irMemberCallNode.setInstanceBinding(painlessInstanceBinding);
            irMemberCallNode.setBindingName(bindingName);

            scriptScope.addStaticConstant(bindingName, painlessInstanceBinding.targetInstance);
        } else {
            throw callLocalNode.createError(new IllegalStateException("illegal tree structure"));
        }

        for (AExpression userArgumentNode : callLocalNode.getArgumentNodes()) {
            irMemberCallNode.addArgumentNode(injectCast(userArgumentNode, scriptScope));
        }

        irMemberCallNode.setLocation(callLocalNode.getLocation());
        irMemberCallNode.setExpressionType(scriptScope.getDecoration(callLocalNode, ValueType.class).getValueType());

        return irMemberCallNode;
    }

    @Override
    public IRNode visitBoolean(EBooleanConstant userBooleanNode, ScriptScope scriptScope) {
        ConstantNode irConstantNode = new ConstantNode();
        irConstantNode.setLocation(userBooleanNode.getLocation());
        irConstantNode.setExpressionType(scriptScope.getDecoration(userBooleanNode, ValueType.class).getValueType());
        irConstantNode.setConstant(scriptScope.getDecoration(userBooleanNode, StandardConstant.class).getStandardConstant());

        return irConstantNode;
    }

    @Override
    public IRNode visitNumeric(ENumeric userNumericNode, ScriptScope scriptScope) {
        ConstantNode irConstantNode = new ConstantNode();
        irConstantNode.setLocation(userNumericNode.getLocation());
        irConstantNode.setExpressionType(scriptScope.getDecoration(userNumericNode, ValueType.class).getValueType());
        irConstantNode.setConstant(scriptScope.getDecoration(userNumericNode, StandardConstant.class).getStandardConstant());

        return irConstantNode;
    }

    @Override
    public IRNode visitDecimal(EDecimal userDecimalNode, ScriptScope scriptScope) {
        ConstantNode irConstantNode = new ConstantNode();
        irConstantNode.setLocation(userDecimalNode.getLocation());
        irConstantNode.setExpressionType(scriptScope.getDecoration(userDecimalNode, ValueType.class).getValueType());
        irConstantNode.setConstant(scriptScope.getDecoration(userDecimalNode, StandardConstant.class).getStandardConstant());

        return irConstantNode;
    }

    @Override
    public IRNode visitString(EString userStringNode, ScriptScope scriptScope) {
        ConstantNode irConstantNode = new ConstantNode();
        irConstantNode.setLocation(userStringNode.getLocation());
        irConstantNode.setExpressionType(scriptScope.getDecoration(userStringNode, ValueType.class).getValueType());
        irConstantNode.setConstant(scriptScope.getDecoration(userStringNode, StandardConstant.class).getStandardConstant());

        return irConstantNode;
    }

    @Override
    public IRNode visitNull(ENull userNullNode, ScriptScope scriptScope) {
        NullNode irNullNode = new NullNode();
        irNullNode.setLocation(userNullNode.getLocation());
        irNullNode.setExpressionType(scriptScope.getDecoration(userNullNode, ValueType.class).getValueType());

        return irNullNode;
    }

    @Override
    public IRNode visitRegex(ERegex userRegexNode, ScriptScope scriptScope) {
        String memberFieldName = scriptScope.getNextSyntheticName("regex");

        FieldNode irFieldNode = new FieldNode();
        irFieldNode.setLocation(userRegexNode.getLocation());
        irFieldNode.setModifiers(Modifier.FINAL | Modifier.STATIC | Modifier.PRIVATE);
        irFieldNode.setFieldType(Pattern.class);
        irFieldNode.setName(memberFieldName);

        irClassNode.addFieldNode(irFieldNode);

        try {
            StatementExpressionNode irStatementExpressionNode = new StatementExpressionNode();
            irStatementExpressionNode.setLocation(userRegexNode.getLocation());

            BlockNode blockNode = irClassNode.getClinitBlockNode();
            blockNode.addStatementNode(irStatementExpressionNode);

            MemberFieldStoreNode irMemberFieldStoreNode = new MemberFieldStoreNode();
            irMemberFieldStoreNode.setLocation(userRegexNode.getLocation());
            irMemberFieldStoreNode.setExpressionType(void.class);
            irMemberFieldStoreNode.setFieldType(Pattern.class);
            irMemberFieldStoreNode.setName(memberFieldName);
            irMemberFieldStoreNode.setStatic(true);

            irStatementExpressionNode.setExpressionNode(irMemberFieldStoreNode);

            CallNode irCallNode = new CallNode();
            irCallNode.setLocation(userRegexNode.getLocation());
            irCallNode.setExpressionType(Pattern.class);

            irMemberFieldStoreNode.setChildNode(irCallNode);

            StaticNode irStaticNode = new StaticNode();
            irStaticNode.setLocation(userRegexNode.getLocation());
            irStaticNode.setExpressionType(Pattern.class);

            irCallNode.setLeftNode(irStaticNode);

            CallSubNode callSubNode = new CallSubNode();
            callSubNode.setLocation(userRegexNode.getLocation());
            callSubNode.setExpressionType(Pattern.class);
            callSubNode.setBox(Pattern.class);
            callSubNode.setMethod(new PainlessMethod(
                            Pattern.class.getMethod("compile", String.class, int.class),
                            Pattern.class,
                            Pattern.class,
                            Arrays.asList(String.class, int.class),
                            null,
                            null,
                            null
                    )
            );

            irCallNode.setRightNode(callSubNode);

            ConstantNode irConstantNode = new ConstantNode();
            irConstantNode.setLocation(userRegexNode.getLocation());
            irConstantNode.setExpressionType(String.class);
            irConstantNode.setConstant(userRegexNode.getPattern());

            callSubNode.addArgumentNode(irConstantNode);

            irConstantNode = new ConstantNode();
            irConstantNode.setLocation(userRegexNode.getLocation());
            irConstantNode.setExpressionType(int.class);
            irConstantNode.setConstant(scriptScope.getDecoration(userRegexNode, StandardConstant.class).getStandardConstant());

            callSubNode.addArgumentNode(irConstantNode);
        } catch (Exception exception) {
            throw userRegexNode.createError(new IllegalStateException("illegal tree structure"));
        }

        MemberFieldLoadNode irMemberFieldLoadNode = new MemberFieldLoadNode();
        irMemberFieldLoadNode.setLocation(userRegexNode.getLocation());
        irMemberFieldLoadNode.setExpressionType(Pattern.class);
        irMemberFieldLoadNode.setName(memberFieldName);
        irMemberFieldLoadNode.setStatic(true);

        return irMemberFieldLoadNode;
    }

    @Override
    public IRNode visitLambda(ELambda userLambdaNode, ScriptScope scriptScope) {
        ReferenceNode irReferenceNode;

        if (scriptScope.hasDecoration(userLambdaNode, TargetType.class)) {
            TypedInterfaceReferenceNode typedInterfaceReferenceNode = new TypedInterfaceReferenceNode();
            typedInterfaceReferenceNode.setReference(scriptScope.getDecoration(userLambdaNode, ReferenceDecoration.class).getReference());
            irReferenceNode = typedInterfaceReferenceNode;
        } else {
            DefInterfaceReferenceNode defInterfaceReferenceNode = new DefInterfaceReferenceNode();
            defInterfaceReferenceNode.setDefReferenceEncoding(
                    scriptScope.getDecoration(userLambdaNode, EncodingDecoration.class).getEncoding());
            irReferenceNode = defInterfaceReferenceNode;
        }

        FunctionNode irFunctionNode = new FunctionNode();
        irFunctionNode.setBlockNode((BlockNode)visit(userLambdaNode.getBlockNode(), scriptScope));
        irFunctionNode.setLocation(userLambdaNode.getLocation());
        irFunctionNode.setName(scriptScope.getDecoration(userLambdaNode, MethodNameDecoration.class).getMethodName());
        irFunctionNode.setReturnType(scriptScope.getDecoration(userLambdaNode, ReturnType.class).getReturnType());
        irFunctionNode.getTypeParameters().addAll(scriptScope.getDecoration(userLambdaNode, TypeParameters.class).getTypeParameters());
        irFunctionNode.getParameterNames().addAll(scriptScope.getDecoration(userLambdaNode, ParameterNames.class).getParameterNames());
        irFunctionNode.setStatic(true);
        irFunctionNode.setVarArgs(false);
        irFunctionNode.setSynthetic(true);
        irFunctionNode.setMaxLoopCounter(scriptScope.getCompilerSettings().getMaxLoopCounter());
        irClassNode.addFunctionNode(irFunctionNode);

        irReferenceNode.setLocation(userLambdaNode.getLocation());
        irReferenceNode.setExpressionType(scriptScope.getDecoration(userLambdaNode, ValueType.class).getValueType());

        List<Variable> captures = scriptScope.getDecoration(userLambdaNode, CapturesDecoration.class).getCaptures();

        for (Variable capture : captures) {
            irReferenceNode.addCapture(capture.getName());
        }

        return irReferenceNode;
    }

    @Override
    public IRNode visitFunctionRef(EFunctionRef userFunctionRefNode, ScriptScope scriptScope) {
        ReferenceNode irReferenceNode;

        TargetType targetType = scriptScope.getDecoration(userFunctionRefNode, TargetType.class);
        CapturesDecoration capturesDecoration = scriptScope.getDecoration(userFunctionRefNode, CapturesDecoration.class);

        if (targetType == null) {
            DefInterfaceReferenceNode defInterfaceReferenceNode = new DefInterfaceReferenceNode();
            defInterfaceReferenceNode.setDefReferenceEncoding(
                    scriptScope.getDecoration(userFunctionRefNode, EncodingDecoration.class).getEncoding());
            irReferenceNode = defInterfaceReferenceNode;
        } else if (capturesDecoration != null && capturesDecoration.getCaptures().get(0).getType() == def.class) {
            TypedCaptureReferenceNode typedCaptureReferenceNode = new TypedCaptureReferenceNode();
            typedCaptureReferenceNode.setMethodName(userFunctionRefNode.getCall());
            irReferenceNode = typedCaptureReferenceNode;
        } else {
            TypedInterfaceReferenceNode typedInterfaceReferenceNode = new TypedInterfaceReferenceNode();
            typedInterfaceReferenceNode.setReference(
                    scriptScope.getDecoration(userFunctionRefNode, ReferenceDecoration.class).getReference());
            irReferenceNode = typedInterfaceReferenceNode;
        }

        irReferenceNode.setLocation(userFunctionRefNode.getLocation());
        irReferenceNode.setExpressionType(scriptScope.getDecoration(userFunctionRefNode, ValueType.class).getValueType());

        if (capturesDecoration != null) {
            irReferenceNode.addCapture(capturesDecoration.getCaptures().get(0).getName());
        }

        return irReferenceNode;
    }

    @Override
    public IRNode visitNewArrayFunctionRef(ENewArrayFunctionRef userNewArrayFunctionRefNode, ScriptScope scriptScope) {
        ReferenceNode irReferenceNode;

        if (scriptScope.hasDecoration(userNewArrayFunctionRefNode, TargetType.class)) {
            TypedInterfaceReferenceNode typedInterfaceReferenceNode = new TypedInterfaceReferenceNode();
            typedInterfaceReferenceNode.setReference(
                    scriptScope.getDecoration(userNewArrayFunctionRefNode, ReferenceDecoration.class).getReference());
            irReferenceNode = typedInterfaceReferenceNode;
        } else {
            DefInterfaceReferenceNode defInterfaceReferenceNode = new DefInterfaceReferenceNode();
            defInterfaceReferenceNode.setDefReferenceEncoding(
                    scriptScope.getDecoration(userNewArrayFunctionRefNode, EncodingDecoration.class).getEncoding());
            irReferenceNode = defInterfaceReferenceNode;
        }

        Class<?> returnType = scriptScope.getDecoration(userNewArrayFunctionRefNode, ReturnType.class).getReturnType();

        VariableNode irVariableNode = new VariableNode();
        irVariableNode.setLocation(userNewArrayFunctionRefNode.getLocation());
        irVariableNode.setExpressionType(int.class);
        irVariableNode.setName("size");

        NewArrayNode irNewArrayNode = new NewArrayNode();
        irNewArrayNode.setLocation(userNewArrayFunctionRefNode.getLocation());
        irNewArrayNode.setExpressionType(returnType);
        irNewArrayNode.setInitialize(false);

        irNewArrayNode.addArgumentNode(irVariableNode);

        ReturnNode irReturnNode = new ReturnNode();
        irReturnNode.setLocation(userNewArrayFunctionRefNode.getLocation());
        irReturnNode.setExpressionNode(irNewArrayNode);

        BlockNode irBlockNode = new BlockNode();
        irBlockNode.setAllEscape(true);
        irBlockNode.setStatementCount(1);
        irBlockNode.addStatementNode(irReturnNode);

        FunctionNode irFunctionNode = new FunctionNode();
        irFunctionNode.setMaxLoopCounter(0);
        irFunctionNode.setName(scriptScope.getDecoration(userNewArrayFunctionRefNode, MethodNameDecoration.class).getMethodName());
        irFunctionNode.setReturnType(returnType);
        irFunctionNode.addTypeParameter(int.class);
        irFunctionNode.addParameterName("size");
        irFunctionNode.setStatic(true);
        irFunctionNode.setVarArgs(false);
        irFunctionNode.setSynthetic(true);
        irFunctionNode.setBlockNode(irBlockNode);

        irClassNode.addFunctionNode(irFunctionNode);

        irReferenceNode.setLocation(userNewArrayFunctionRefNode.getLocation());
        irReferenceNode.setExpressionType(scriptScope.getDecoration(userNewArrayFunctionRefNode, ValueType.class).getValueType());

        return irReferenceNode;
    }

    @Override
    public IRNode visitSymbol(ESymbol userSymbolNode, ScriptScope scriptScope) {
        ExpressionNode irExpressionNode;

        if (scriptScope.hasDecoration(userSymbolNode, StaticType.class)) {
            Class<?> staticType = scriptScope.getDecoration(userSymbolNode, StaticType.class).getStaticType();
            StaticNode staticNode = new StaticNode();
            staticNode.setLocation(userSymbolNode.getLocation());
            staticNode.setExpressionType(staticType);
            irExpressionNode = staticNode;
        } else if (scriptScope.hasDecoration(userSymbolNode, ValueType.class)) {
            VariableNode variableNode = new VariableNode();
            variableNode.setLocation(userSymbolNode.getLocation());
            variableNode.setExpressionType(scriptScope.getDecoration(userSymbolNode, ValueType.class).getValueType());
            variableNode.setName(userSymbolNode.getSymbol());
            irExpressionNode = variableNode;
        } else {
            throw userSymbolNode.createError(new IllegalStateException("illegal tree structure"));
        }

        return irExpressionNode;
    }

    @Override
    public IRNode visitDot(EDot userDotNode, ScriptScope scriptScope) {
        ExpressionNode irExpressionNode;

        if (scriptScope.hasDecoration(userDotNode, StaticType.class)) {
            Class<?> staticType = scriptScope.getDecoration(userDotNode, StaticType.class).getStaticType();
            StaticNode staticNode = new StaticNode();
            staticNode.setLocation(userDotNode.getLocation());
            staticNode.setExpressionType(staticType);
            irExpressionNode = staticNode;
        } else {
            ValueType prefixValueType = scriptScope.getDecoration(userDotNode.getPrefixNode(), ValueType.class);

            if (prefixValueType != null && prefixValueType.getValueType().isArray()) {
                DotSubArrayLengthNode dotSubArrayLengthNode = new DotSubArrayLengthNode();
                dotSubArrayLengthNode.setLocation(userDotNode.getLocation());
                dotSubArrayLengthNode.setExpressionType(int.class);
                irExpressionNode = dotSubArrayLengthNode;
            } else if (prefixValueType != null && prefixValueType.getValueType() == def.class) {
                DotSubDefNode dotSubDefNode = new DotSubDefNode();
                dotSubDefNode.setLocation(userDotNode.getLocation());
                dotSubDefNode.setExpressionType(scriptScope.getDecoration(userDotNode, ValueType.class).getValueType());
                dotSubDefNode.setValue(userDotNode.getIndex());
                irExpressionNode = dotSubDefNode;
            } else if (scriptScope.hasDecoration(userDotNode, StandardPainlessField.class)) {
                DotSubNode dotSubNode = new DotSubNode();
                dotSubNode.setLocation(userDotNode.getLocation());
                dotSubNode.setExpressionType(scriptScope.getDecoration(userDotNode, ValueType.class).getValueType());
                dotSubNode.setField(scriptScope.getDecoration(userDotNode, StandardPainlessField.class).getStandardPainlessField());
                irExpressionNode = dotSubNode;
            } else if (scriptScope.getCondition(userDotNode, Shortcut.class)) {
                DotSubShortcutNode dotSubShortcutNode = new DotSubShortcutNode();
                dotSubShortcutNode.setLocation(userDotNode.getLocation());
                dotSubShortcutNode.setExpressionType(scriptScope.getDecoration(userDotNode, ValueType.class).getValueType());

                if (scriptScope.hasDecoration(userDotNode, GetterPainlessMethod.class)) {
                    dotSubShortcutNode.setGetter(
                            scriptScope.getDecoration(userDotNode, GetterPainlessMethod.class).getGetterPainlessMethod());
                }

                if (scriptScope.hasDecoration(userDotNode, SetterPainlessMethod.class)) {
                    dotSubShortcutNode.setSetter(
                            scriptScope.getDecoration(userDotNode, SetterPainlessMethod.class).getSetterPainlessMethod());
                }

                irExpressionNode = dotSubShortcutNode;
            } else if (scriptScope.getCondition(userDotNode, MapShortcut.class)) {
                ConstantNode constantNode = new ConstantNode();
                constantNode.setLocation(userDotNode.getLocation());
                constantNode.setExpressionType(String.class);
                constantNode.setConstant(userDotNode.getIndex());

                MapSubShortcutNode mapSubShortcutNode = new MapSubShortcutNode();
                mapSubShortcutNode.setChildNode(constantNode);
                mapSubShortcutNode.setLocation(userDotNode.getLocation());
                mapSubShortcutNode.setExpressionType(scriptScope.getDecoration(userDotNode, ValueType.class).getValueType());

                if (scriptScope.hasDecoration(userDotNode, GetterPainlessMethod.class)) {
                    mapSubShortcutNode.setGetter(
                            scriptScope.getDecoration(userDotNode, GetterPainlessMethod.class).getGetterPainlessMethod());
                }

                if (scriptScope.hasDecoration(userDotNode, SetterPainlessMethod.class)) {
                    mapSubShortcutNode.setSetter(
                            scriptScope.getDecoration(userDotNode, SetterPainlessMethod.class).getSetterPainlessMethod());
                }

                irExpressionNode = mapSubShortcutNode;
            } else if (scriptScope.getCondition(userDotNode, ListShortcut.class)) {
                ConstantNode constantNode = new ConstantNode();
                constantNode.setLocation(userDotNode.getLocation());
                constantNode.setExpressionType(int.class);
                constantNode.setConstant(scriptScope.getDecoration(userDotNode, StandardConstant.class).getStandardConstant());

                ListSubShortcutNode listSubShortcutNode = new ListSubShortcutNode();
                listSubShortcutNode.setChildNode(constantNode);
                listSubShortcutNode.setLocation(userDotNode.getLocation());
                listSubShortcutNode.setExpressionType(scriptScope.getDecoration(userDotNode, ValueType.class).getValueType());

                if (scriptScope.hasDecoration(userDotNode, GetterPainlessMethod.class)) {
                    listSubShortcutNode.setGetter(
                            scriptScope.getDecoration(userDotNode, GetterPainlessMethod.class).getGetterPainlessMethod());
                }

                if (scriptScope.hasDecoration(userDotNode, SetterPainlessMethod.class)) {
                    listSubShortcutNode.setSetter(
                            scriptScope.getDecoration(userDotNode, SetterPainlessMethod.class).getSetterPainlessMethod());
                }

                irExpressionNode = listSubShortcutNode;
            } else {
                throw userDotNode.createError(new IllegalStateException("illegal tree structure"));
            }

            if (userDotNode.isNullSafe()) {
                NullSafeSubNode nullSafeSubNode = new NullSafeSubNode();
                nullSafeSubNode.setChildNode(irExpressionNode);
                nullSafeSubNode.setLocation(irExpressionNode.getLocation());
                nullSafeSubNode.setExpressionType(irExpressionNode.getExpressionType());
                irExpressionNode = nullSafeSubNode;
            }

            DotNode irDotNode = new DotNode();
            irDotNode.setLeftNode((ExpressionNode)visit(userDotNode.getPrefixNode(), scriptScope));
            irDotNode.setRightNode(irExpressionNode);
            irDotNode.setLocation(irExpressionNode.getLocation());
            irDotNode.setExpressionType(irExpressionNode.getExpressionType());
            irExpressionNode = irDotNode;
        }

        return irExpressionNode;
    }

    @Override
    public IRNode visitBrace(EBrace userBraceNode, ScriptScope scriptScope) {
        ExpressionNode irExpressionNode;

        Class<?> prefixValueType = scriptScope.getDecoration(userBraceNode.getPrefixNode(), ValueType.class).getValueType();

        if (prefixValueType.isArray()) {
            BraceSubNode braceSubNode = new BraceSubNode();
            braceSubNode.setChildNode(injectCast(userBraceNode.getIndexNode(), scriptScope));
            braceSubNode.setLocation(userBraceNode.getLocation());
            braceSubNode.setExpressionType(scriptScope.getDecoration(userBraceNode, ValueType.class).getValueType());
            irExpressionNode = braceSubNode;
        } else if (prefixValueType == def.class) {
            BraceSubDefNode braceSubDefNode = new BraceSubDefNode();
            braceSubDefNode.setChildNode((ExpressionNode)visit(userBraceNode.getIndexNode(), scriptScope));
            braceSubDefNode.setLocation(userBraceNode.getLocation());
            braceSubDefNode.setExpressionType(scriptScope.getDecoration(userBraceNode, ValueType.class).getValueType());
            irExpressionNode = braceSubDefNode;
        } else if (scriptScope.getCondition(userBraceNode, MapShortcut.class)) {
            MapSubShortcutNode mapSubShortcutNode = new MapSubShortcutNode();
            mapSubShortcutNode.setChildNode(injectCast(userBraceNode.getIndexNode(), scriptScope));
            mapSubShortcutNode.setLocation(userBraceNode.getLocation());
            mapSubShortcutNode.setExpressionType(scriptScope.getDecoration(userBraceNode, ValueType.class).getValueType());

            if (scriptScope.hasDecoration(userBraceNode, GetterPainlessMethod.class)) {
                mapSubShortcutNode.setGetter(
                        scriptScope.getDecoration(userBraceNode, GetterPainlessMethod.class).getGetterPainlessMethod());
            }

            if (scriptScope.hasDecoration(userBraceNode, SetterPainlessMethod.class)) {
                mapSubShortcutNode.setSetter(
                        scriptScope.getDecoration(userBraceNode, SetterPainlessMethod.class).getSetterPainlessMethod());
            }

            irExpressionNode = mapSubShortcutNode;
        } else if (scriptScope.getCondition(userBraceNode, ListShortcut.class)) {
            ListSubShortcutNode listSubShortcutNode = new ListSubShortcutNode();
            listSubShortcutNode.setChildNode(injectCast(userBraceNode.getIndexNode(), scriptScope));
            listSubShortcutNode.setLocation(userBraceNode.getLocation());
            listSubShortcutNode.setExpressionType(scriptScope.getDecoration(userBraceNode, ValueType.class).getValueType());

            if (scriptScope.hasDecoration(userBraceNode, GetterPainlessMethod.class)) {
                listSubShortcutNode.setGetter(
                        scriptScope.getDecoration(userBraceNode, GetterPainlessMethod.class).getGetterPainlessMethod());
            }

            if (scriptScope.hasDecoration(userBraceNode, SetterPainlessMethod.class)) {
                listSubShortcutNode.setSetter(
                        scriptScope.getDecoration(userBraceNode, SetterPainlessMethod.class).getSetterPainlessMethod());
            }

            irExpressionNode = listSubShortcutNode;
        } else {
            throw userBraceNode.createError(new IllegalStateException("illegal tree structure"));
        }

        BraceNode braceNode = new BraceNode();
        braceNode.setLeftNode((ExpressionNode)visit(userBraceNode.getPrefixNode(), scriptScope));
        braceNode.setRightNode(irExpressionNode);
        braceNode.setLocation(irExpressionNode.getLocation());
        braceNode.setExpressionType(irExpressionNode.getExpressionType());

        return braceNode;
    }

    @Override
    public IRNode visitCall(ECall userCallNode, ScriptScope scriptScope) {
        ExpressionNode irExpressionNode;

        ValueType prefixValueType = scriptScope.getDecoration(userCallNode.getPrefixNode(), ValueType.class);

        if (prefixValueType != null && prefixValueType.getValueType() == def.class) {
            CallSubDefNode irCallSubDefNode = new CallSubDefNode();

            for (AExpression userArgumentNode : userCallNode.getArgumentNodes()) {
                irCallSubDefNode.addArgumentNode((ExpressionNode)visit(userArgumentNode, scriptScope));
            }

            irCallSubDefNode.setLocation(userCallNode.getLocation());
            irCallSubDefNode.setExpressionType(scriptScope.getDecoration(userCallNode, ValueType.class).getValueType());
            irCallSubDefNode.setName(userCallNode.getMethodName());
            irExpressionNode = irCallSubDefNode;
        } else {
            Class<?> boxType;

            if (prefixValueType != null) {
                boxType = prefixValueType.getValueType();
            } else {
                boxType = scriptScope.getDecoration(userCallNode.getPrefixNode(), StaticType.class).getStaticType();
            }

            CallSubNode callSubNode = new CallSubNode();

            for (AExpression userArgumentNode : userCallNode.getArgumentNodes()) {
                callSubNode.addArgumentNode(injectCast(userArgumentNode, scriptScope));
            }

            callSubNode.setLocation(userCallNode.getLocation());
            callSubNode.setExpressionType(scriptScope.getDecoration(userCallNode, ValueType.class).getValueType());;
            callSubNode.setMethod(scriptScope.getDecoration(userCallNode, StandardPainlessMethod.class).getStandardPainlessMethod());
            callSubNode.setBox(boxType);
            irExpressionNode = callSubNode;
        }

        if (userCallNode.isNullSafe()) {
            NullSafeSubNode nullSafeSubNode = new NullSafeSubNode();
            nullSafeSubNode.setChildNode(irExpressionNode);
            nullSafeSubNode.setLocation(irExpressionNode.getLocation());
            nullSafeSubNode.setExpressionType(irExpressionNode.getExpressionType());
            irExpressionNode = nullSafeSubNode;
        }

        CallNode callNode = new CallNode();
        callNode.setLeftNode((ExpressionNode)visit(userCallNode.getPrefixNode(), scriptScope));
        callNode.setRightNode(irExpressionNode);
        callNode.setLocation(irExpressionNode.getLocation());
        callNode.setExpressionType(irExpressionNode.getExpressionType());

        return callNode;
    }
}
