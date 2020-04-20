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

import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.BreakNode;
import org.elasticsearch.painless.ir.CastNode;
import org.elasticsearch.painless.ir.CatchNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ConditionNode;
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.ir.ContinueNode;
import org.elasticsearch.painless.ir.DeclarationBlockNode;
import org.elasticsearch.painless.ir.DeclarationNode;
import org.elasticsearch.painless.ir.DoWhileLoopNode;
import org.elasticsearch.painless.ir.ExpressionNode;
import org.elasticsearch.painless.ir.ForEachLoopNode;
import org.elasticsearch.painless.ir.ForEachSubArrayNode;
import org.elasticsearch.painless.ir.ForEachSubIterableNode;
import org.elasticsearch.painless.ir.ForLoopNode;
import org.elasticsearch.painless.ir.FunctionNode;
import org.elasticsearch.painless.ir.IRNode;
import org.elasticsearch.painless.ir.IfElseNode;
import org.elasticsearch.painless.ir.IfNode;
import org.elasticsearch.painless.ir.NullNode;
import org.elasticsearch.painless.ir.ReturnNode;
import org.elasticsearch.painless.ir.StatementExpressionNode;
import org.elasticsearch.painless.ir.StatementNode;
import org.elasticsearch.painless.ir.ThrowNode;
import org.elasticsearch.painless.ir.TryNode;
import org.elasticsearch.painless.ir.WhileLoopNode;
import org.elasticsearch.painless.lookup.def;
import org.elasticsearch.painless.node.AExpression;
import org.elasticsearch.painless.node.ANode;
import org.elasticsearch.painless.node.AStatement;
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
import org.elasticsearch.painless.symbol.Decorations.ContinuousLoop;
import org.elasticsearch.painless.symbol.Decorations.ExpressionPainlessCast;
import org.elasticsearch.painless.symbol.Decorations.IterablePainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.MethodEscape;
import org.elasticsearch.painless.symbol.Decorations.SemanticVariable;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.SemanticScope.Variable;

import java.util.Iterator;

public class IRTreeBuilder implements UserTreeVisitor<ScriptScope, IRNode> {

    private static final Location INTERNAL_LOCATION = new Location("<internal>", 0);

    private ClassNode irClassNode;

    protected IRNode visit(ANode userNode, ScriptScope scriptScope) {
        return userNode == null ? null : userNode.visit(this, scriptScope);
    }

    protected ExpressionNode injectCast(AExpression userExpressionNode, ScriptScope scriptScope) {
        ExpressionNode irExpressionNode = (ExpressionNode)visit(userExpressionNode, scriptScope);

        if (irExpressionNode == null) {
            return irExpressionNode;
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
    public ClassNode visitClass(SClass userClassNode, ScriptScope scriptScope) {
        irClassNode = new ClassNode();

        for (SFunction userFunctionNode : userClassNode.getFunctionNodes()) {
            irClassNode.addFunctionNode((FunctionNode)visit(userFunctionNode, scriptScope));
        }

        irClassNode.setLocation(irClassNode.getLocation());
        irClassNode.setScriptScope(scriptScope);

        return irClassNode;
    }

    @Override
    public FunctionNode visitFunction(SFunction userFunctionNode, ScriptScope scriptScope) {
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
                    constantNode.setLocation(INTERNAL_LOCATION);
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
                    irExpressionNode.setLocation(INTERNAL_LOCATION);
                    irExpressionNode.setExpressionType(returnType);
                }
            } else {
                throw userFunctionNode.createError(new IllegalStateException("illegal tree structure"));
            }

            ReturnNode irReturnNode = new ReturnNode();
            irReturnNode.setLocation(INTERNAL_LOCATION);
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
    public BlockNode visitBlock(SBlock userBlockNode, ScriptScope scriptScope) {
        BlockNode irBlockNode = new BlockNode();

        for (AStatement userStatementNode : userBlockNode.getStatementNodes()) {
            irBlockNode.addStatementNode((StatementNode)visit(userStatementNode, scriptScope));
        }

        irBlockNode.setLocation(userBlockNode.getLocation());
        irBlockNode.setAllEscape(scriptScope.getCondition(userBlockNode, AllEscape.class));

        return irBlockNode;
    }

    @Override
    public IfNode visitIf(SIf userIfNode, ScriptScope scriptScope) {
        IfNode irIfNode = new IfNode();
        irIfNode.setConditionNode(injectCast(userIfNode.getConditionNode(), scriptScope));
        irIfNode.setBlockNode((BlockNode)visit(userIfNode.getIfblockNode(), scriptScope));
        irIfNode.setLocation(irIfNode.getLocation());

        return irIfNode;
    }

    @Override
    public IfElseNode visitIfElse(SIfElse userIfNode, ScriptScope scriptScope) {
        IfElseNode irIfElseNode = new IfElseNode();
        irIfElseNode.setConditionNode(injectCast(userIfNode.getConditionNode(), scriptScope));
        irIfElseNode.setBlockNode((BlockNode)visit(userIfNode.getIfblockNode(), scriptScope));
        irIfElseNode.setElseBlockNode((BlockNode)visit(userIfNode.getElseblockNode(), scriptScope));
        irIfElseNode.setLocation(irIfElseNode.getLocation());

        return irIfElseNode;
    }

    @Override
    public WhileLoopNode visitWhile(SWhile userWhileNode, ScriptScope scriptScope) {
        WhileLoopNode irWhileLoopNode = new WhileLoopNode();
        irWhileLoopNode.setConditionNode(injectCast(userWhileNode.getConditionNode(), scriptScope));
        irWhileLoopNode.setBlockNode((BlockNode)visit(userWhileNode.getBlockNode(), scriptScope));
        irWhileLoopNode.setLocation(irWhileLoopNode.getLocation());
        irWhileLoopNode.setContinuous(scriptScope.getCondition(userWhileNode, ContinuousLoop.class));

        return irWhileLoopNode;
    }

    @Override
    public DoWhileLoopNode visitDo(SDo userDoNode, ScriptScope scriptScope) {
        DoWhileLoopNode irDoWhileLoopNode = new DoWhileLoopNode();
        irDoWhileLoopNode.setConditionNode(injectCast(userDoNode.getConditionNode(), scriptScope));
        irDoWhileLoopNode.setBlockNode((BlockNode)visit(userDoNode.getBlockNode(), scriptScope));
        irDoWhileLoopNode.setLocation(userDoNode.getLocation());
        irDoWhileLoopNode.setContinuous(scriptScope.getCondition(userDoNode, ContinuousLoop.class));

        return irDoWhileLoopNode;
    }

    @Override
    public ForLoopNode visitFor(SFor userForNode, ScriptScope scriptScope) {
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
    public ForEachLoopNode visitEach(SEach userEachNode, ScriptScope scriptScope) {
        Variable variable = scriptScope.getDecoration(userEachNode, SemanticVariable.class).getSemanticVariable();
        ExpressionNode irIterableNode = (ExpressionNode)visit(userEachNode.getIterableNode(), scriptScope);
        Class<?> iterableValueType = scriptScope.getDecoration(userEachNode, ValueType.class).getValueType();
        BlockNode irBlockNode = (BlockNode)visit(userEachNode, scriptScope);

        ConditionNode irConditionNode;

        if (iterableValueType.isArray()) {
            ForEachSubArrayNode irForEachSubArrayNode = new ForEachSubArrayNode();
            irForEachSubArrayNode.setConditionNode(irIterableNode);
            irForEachSubArrayNode.setBlockNode(irBlockNode);
            irForEachSubArrayNode.setLocation(userEachNode.getLocation());
            irForEachSubArrayNode.setVariableType(variable.getType());
            irForEachSubArrayNode.setVariableName(variable.getName());
            irForEachSubArrayNode.setCast(
                    scriptScope.getDecoration(userEachNode, ExpressionPainlessCast.class).getExpressionPainlessCast());
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
            irForEachSubIterableNode.setCast(
                    scriptScope.getDecoration(userEachNode, ExpressionPainlessCast.class).getExpressionPainlessCast());
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
    public DeclarationBlockNode visitDeclBlock(SDeclBlock userDeclBlockNode, ScriptScope scriptScope) {
        DeclarationBlockNode irDeclarationBlockNode = new DeclarationBlockNode();

        for (SDeclaration userDeclarationNode : userDeclBlockNode.getDeclarationNodes()) {
            irDeclarationBlockNode.addDeclarationNode((DeclarationNode)visit(userDeclarationNode, scriptScope));
        }

        irDeclarationBlockNode.setLocation(userDeclBlockNode.getLocation());

        return irDeclarationBlockNode;
    }

    @Override
    public DeclarationNode visitDeclaration(SDeclaration userDeclarationNode, ScriptScope scriptScope) {
        Variable variable = scriptScope.getDecoration(userDeclarationNode, SemanticVariable.class).getSemanticVariable();

        DeclarationNode declarationNode = new DeclarationNode();
        declarationNode.setExpressionNode(injectCast(userDeclarationNode.getValueNode(), scriptScope));
        declarationNode.setLocation(userDeclarationNode.getLocation());
        declarationNode.setDeclarationType(variable.getType());
        declarationNode.setName(variable.getName());

        return declarationNode;
    }

    @Override
    public ReturnNode visitReturn(SReturn userReturnNode, ScriptScope scriptScope) {
        ReturnNode irReturnNode = new ReturnNode();
        irReturnNode.setExpressionNode(injectCast(userReturnNode.getExpressionNode(), scriptScope));
        irReturnNode.setLocation(userReturnNode.getLocation());

        return irReturnNode;
    }

    @Override
    public StatementNode visitExpression(SExpression userExpressionNode, ScriptScope scriptScope) {
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
    public TryNode visitTry(STry userTryNode, ScriptScope scriptScope) {
        TryNode irTryNode = new TryNode();

        for (SCatch userCatchNode : userTryNode.getCatchNodes()) {
            irTryNode.addCatchNode((CatchNode)visit(userCatchNode, scriptScope));
        }

        irTryNode.setBlockNode((BlockNode)visit(userTryNode.getBlockNode(), scriptScope));
        irTryNode.setLocation(userTryNode.getLocation());

        return irTryNode;
    }

    @Override
    public CatchNode visitCatch(SCatch userCatchNode, ScriptScope scriptScope) {
        Variable variable = scriptScope.getDecoration(userCatchNode, SemanticVariable.class).getSemanticVariable();

        CatchNode irCatchNode = new CatchNode();
        irCatchNode.setExceptionType(variable.getType());
        irCatchNode.setSymbol(variable.getName());
        irCatchNode.setBlockNode((BlockNode)visit(userCatchNode.getBlockNode(), scriptScope));
        irCatchNode.setLocation(userCatchNode.getLocation());

        return irCatchNode;
    }

    @Override
    public ThrowNode visitThrow(SThrow userThrowNode, ScriptScope scriptScope) {
        ThrowNode irThrowNode = new ThrowNode();
        irThrowNode.setExpressionNode(injectCast(userThrowNode.getExpressionNode(), scriptScope));
        irThrowNode.setLocation(userThrowNode.getLocation());

        return irThrowNode;
    }

    @Override
    public ContinueNode visitContinue(SContinue userContinueNode, ScriptScope scriptScope) {
        ContinueNode irContinueNode = new ContinueNode();
        irContinueNode.setLocation(userContinueNode.getLocation());

        return irContinueNode;
    }

    @Override
    public BreakNode visitBreak(SBreak userBreakNode, ScriptScope scriptScope) {
        BreakNode irBreakNode = new BreakNode();
        irBreakNode.setLocation(userBreakNode.getLocation());

        return irBreakNode;
    }
}
