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

import org.elasticsearch.painless.AnalyzerCaster;
import org.elasticsearch.painless.FunctionRef;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.lookup.PainlessClassBinding;
import org.elasticsearch.painless.lookup.PainlessConstructor;
import org.elasticsearch.painless.lookup.PainlessField;
import org.elasticsearch.painless.lookup.PainlessInstanceBinding;
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
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
import org.elasticsearch.painless.spi.annotation.NonDeterministicAnnotation;
import org.elasticsearch.painless.symbol.Decorations;
import org.elasticsearch.painless.symbol.Decorations.AllEscape;
import org.elasticsearch.painless.symbol.Decorations.AnyBreak;
import org.elasticsearch.painless.symbol.Decorations.AnyContinue;
import org.elasticsearch.painless.symbol.Decorations.BeginLoop;
import org.elasticsearch.painless.symbol.Decorations.BinaryType;
import org.elasticsearch.painless.symbol.Decorations.CapturesDecoration;
import org.elasticsearch.painless.symbol.Decorations.ComparisonType;
import org.elasticsearch.painless.symbol.Decorations.CompoundType;
import org.elasticsearch.painless.symbol.Decorations.Concatenate;
import org.elasticsearch.painless.symbol.Decorations.ContinuousLoop;
import org.elasticsearch.painless.symbol.Decorations.DefOptimized;
import org.elasticsearch.painless.symbol.Decorations.DowncastPainlessCast;
import org.elasticsearch.painless.symbol.Decorations.EncodingDecoration;
import org.elasticsearch.painless.symbol.Decorations.Explicit;
import org.elasticsearch.painless.symbol.Decorations.ExpressionPainlessCast;
import org.elasticsearch.painless.symbol.Decorations.GetterPainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.InLoop;
import org.elasticsearch.painless.symbol.Decorations.InstanceType;
import org.elasticsearch.painless.symbol.Decorations.Internal;
import org.elasticsearch.painless.symbol.Decorations.IterablePainlessMethod;
import org.elasticsearch.painless.symbol.Decorations.LastLoop;
import org.elasticsearch.painless.symbol.Decorations.LastSource;
import org.elasticsearch.painless.symbol.Decorations.ListShortcut;
import org.elasticsearch.painless.symbol.Decorations.LoopEscape;
import org.elasticsearch.painless.symbol.Decorations.MapShortcut;
import org.elasticsearch.painless.symbol.Decorations.MethodEscape;
import org.elasticsearch.painless.symbol.Decorations.MethodNameDecoration;
import org.elasticsearch.painless.symbol.Decorations.Negate;
import org.elasticsearch.painless.symbol.Decorations.ParameterNames;
import org.elasticsearch.painless.symbol.Decorations.PartialCanonicalTypeName;
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
import org.elasticsearch.painless.symbol.Decorations.Write;
import org.elasticsearch.painless.symbol.FunctionTable;
import org.elasticsearch.painless.symbol.FunctionTable.LocalFunction;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.SemanticScope;
import org.elasticsearch.painless.symbol.SemanticScope.FunctionScope;
import org.elasticsearch.painless.symbol.SemanticScope.LambdaScope;
import org.elasticsearch.painless.symbol.SemanticScope.Variable;

import java.lang.reflect.Modifier;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import static org.elasticsearch.painless.lookup.PainlessLookupUtility.typeToCanonicalTypeName;
import static org.elasticsearch.painless.symbol.SemanticScope.newFunctionScope;

/**
 * Semantically validates a user tree visiting all user tree nodes to check for
 * valid control flow, valid types, valid variable resolution, valid method resolution,
 * valid field resolution, and other specialized validation.
 */
public class DefaultSemanticAnalysisPhase extends UserTreeBaseVisitor<SemanticScope> {

    /**
     * Decorates a user expression node with a PainlessCast.
     */
    public void decorateWithCast(AExpression userExpressionNode, SemanticScope semanticScope) {
        Location location = userExpressionNode.getLocation();
        Class<?> valueType = semanticScope.getDecoration(userExpressionNode, ValueType.class).getValueType();
        Class<?> targetType = semanticScope.getDecoration(userExpressionNode, TargetType.class).getTargetType();
        boolean isExplicitCast = semanticScope.getCondition(userExpressionNode, Explicit.class);
        boolean isInternalCast = semanticScope.getCondition(userExpressionNode, Internal.class);

        PainlessCast painlessCast = AnalyzerCaster.getLegalCast(location, valueType, targetType, isExplicitCast, isInternalCast);

        if (painlessCast != null) {
            semanticScope.putDecoration(userExpressionNode, new ExpressionPainlessCast(painlessCast));
        }
    }

    /**
     * Shortcut to visit a user tree node with a null check.
     */
    public void visit(ANode userNode, SemanticScope semanticScope) {
        if (userNode != null) {
            userNode.visit(this, semanticScope);
        }
    }

    /**
     * Shortcut to visit a user expression node with additional checks common to most expression nodes. These
     * additional checks include looking for an escaped partial canonical type, an unexpected static type, and an
     * unexpected value type.
     */
    public void checkedVisit(AExpression userExpressionNode, SemanticScope semanticScope) {
        if (userExpressionNode != null) {
            userExpressionNode.visit(this, semanticScope);

            if (semanticScope.hasDecoration(userExpressionNode, PartialCanonicalTypeName.class)) {
                throw userExpressionNode.createError(new IllegalArgumentException("cannot resolve symbol [" +
                        semanticScope.getDecoration(userExpressionNode, PartialCanonicalTypeName.class).getPartialCanonicalTypeName() +
                        "]"));
            }

            if (semanticScope.hasDecoration(userExpressionNode, StaticType.class)) {
                throw userExpressionNode.createError(new IllegalArgumentException("value required: instead found unexpected type " +
                        "[" + semanticScope.getDecoration(userExpressionNode, StaticType.class).getStaticCanonicalTypeName() + "]"));
            }

            if (semanticScope.hasDecoration(userExpressionNode, ValueType.class) == false) {
                throw userExpressionNode.createError(new IllegalStateException("value required: instead found no value"));
            }
        }
    }

    /**
     * Visits a class.
     */
    public void visitClass(SClass userClassNode, ScriptScope scriptScope) {
        for (SFunction userFunctionNode : userClassNode.getFunctionNodes()) {
            visitFunction(userFunctionNode, scriptScope);
        }
    }

    /**
     * Visits a function and defines variables for each parameter.
     * Checks: control flow, type validation
     */
    public void visitFunction(SFunction userFunctionNode, ScriptScope scriptScope) {
        String functionName = userFunctionNode.getFunctionName();
        LocalFunction localFunction =
                scriptScope.getFunctionTable().getFunction(functionName, userFunctionNode.getCanonicalTypeNameParameters().size());
        Class<?> returnType = localFunction.getReturnType();
        List<Class<?>> typeParameters = localFunction.getTypeParameters();
        FunctionScope functionScope = newFunctionScope(scriptScope, localFunction.getReturnType());

        for (int index = 0; index < localFunction.getTypeParameters().size(); ++index) {
            Class<?> typeParameter = localFunction.getTypeParameters().get(index);
            String parameterName = userFunctionNode.getParameterNames().get(index);
            functionScope.defineVariable(userFunctionNode.getLocation(), typeParameter, parameterName, false);
        }

        SBlock userBlockNode = userFunctionNode.getBlockNode();

        if (userBlockNode.getStatementNodes().isEmpty()) {
            throw userFunctionNode.createError(new IllegalArgumentException("invalid function definition: " +
                    "found no statements for function " +
                    "[" + functionName + "] with [" + typeParameters.size() + "] parameters"));
        }

        functionScope.setCondition(userBlockNode, LastSource.class);
        visit(userBlockNode, functionScope.newLocalScope());
        boolean methodEscape = functionScope.getCondition(userBlockNode, MethodEscape.class);
        boolean isAutoReturnEnabled = userFunctionNode.isAutoReturnEnabled();

        if (methodEscape == false && isAutoReturnEnabled == false && returnType != void.class) {
            throw userFunctionNode.createError(new IllegalArgumentException("invalid function definition: " +
                    "not all paths provide a return value for function " +
                    "[" + functionName + "] with [" + typeParameters.size() + "] parameters"));
        }

        if (methodEscape) {
            functionScope.setCondition(userFunctionNode, MethodEscape.class);
        }
    }

    /**
     * Visits a block and which contains one-to-many statements.
     * Checks: control flow
     */
    @Override
    public void visitBlock(SBlock userBlockNode, SemanticScope semanticScope) {
        List<AStatement> userStatementNodes = userBlockNode.getStatementNodes();

        if (userStatementNodes.isEmpty()) {
            throw userBlockNode.createError(new IllegalArgumentException("invalid block: found no statements"));
        }

        AStatement lastUserStatement = userStatementNodes.get(userStatementNodes.size() - 1);

        boolean lastSource = semanticScope.getCondition(userBlockNode, LastSource.class);
        boolean beginLoop = semanticScope.getCondition(userBlockNode, BeginLoop.class);
        boolean inLoop = semanticScope.getCondition(userBlockNode, InLoop.class);
        boolean lastLoop = semanticScope.getCondition(userBlockNode, LastLoop.class);

        boolean allEscape;
        boolean anyContinue = false;
        boolean anyBreak = false;

        for (AStatement userStatementNode : userStatementNodes) {
            if (inLoop) {
                semanticScope.setCondition(userStatementNode, InLoop.class);
            }

            if (userStatementNode == lastUserStatement) {
                if (beginLoop || lastLoop) {
                    semanticScope.setCondition(userStatementNode, LastLoop.class);
                }

                if (lastSource) {
                    semanticScope.setCondition(userStatementNode, LastSource.class);
                }
            }

            visit(userStatementNode, semanticScope);
            allEscape = semanticScope.getCondition(userStatementNode, AllEscape.class);

            if (userStatementNode == lastUserStatement) {
                semanticScope.replicateCondition(userStatementNode, userBlockNode, MethodEscape.class);
                semanticScope.replicateCondition(userStatementNode, userBlockNode, LoopEscape.class);

                if (allEscape) {
                    semanticScope.setCondition(userStatementNode, AllEscape.class);
                }
            } else {
                if (allEscape) {
                    throw userBlockNode.createError(new IllegalArgumentException("invalid block: unreachable statement"));
                }
            }

            anyContinue |= semanticScope.getCondition(userStatementNode, AnyContinue.class);
            anyBreak |= semanticScope.getCondition(userStatementNode, AnyBreak.class);
        }

        if (anyContinue) {
            semanticScope.setCondition(userBlockNode, AnyContinue.class);
        }

        if (anyBreak) {
            semanticScope.setCondition(userBlockNode, AnyBreak.class);
        }
    }

    /**
     * Visits an if statement with error checking for an extraneous if.
     * Checks: control flow
     */
    @Override
    public void visitIf(SIf userIfNode, SemanticScope semanticScope) {
        AExpression userConditionNode = userIfNode.getConditionNode();
        semanticScope.setCondition(userConditionNode, Read.class);
        semanticScope.putDecoration(userConditionNode, new TargetType(boolean.class));
        checkedVisit(userConditionNode, semanticScope);
        decorateWithCast(userConditionNode, semanticScope);

        SBlock userIfBlockNode = userIfNode.getIfBlockNode();

        if (userConditionNode instanceof EBooleanConstant || userIfBlockNode == null) {
            throw userIfNode.createError(new IllegalArgumentException("extraneous if block"));
        }

        semanticScope.replicateCondition(userIfNode, userIfBlockNode, LastSource.class);
        semanticScope.replicateCondition(userIfNode, userIfBlockNode, InLoop.class);
        semanticScope.replicateCondition(userIfNode, userIfBlockNode, LastLoop.class);
        visit(userIfBlockNode, semanticScope.newLocalScope());
        semanticScope.replicateCondition(userIfBlockNode, userIfNode, AnyContinue.class);
        semanticScope.replicateCondition(userIfBlockNode, userIfNode, AnyBreak.class);
    }

    /**
     * Visits an if/else statement with error checking for an extraneous if/else.
     * Checks: control flow
     */
    @Override
    public void visitIfElse(SIfElse userIfElseNode, SemanticScope semanticScope) {
        AExpression userConditionNode = userIfElseNode.getConditionNode();
        semanticScope.setCondition(userConditionNode, Read.class);
        semanticScope.putDecoration(userConditionNode, new TargetType(boolean.class));
        checkedVisit(userConditionNode, semanticScope);
        decorateWithCast(userConditionNode, semanticScope);

        SBlock userIfBlockNode = userIfElseNode.getIfBlockNode();

        if (userConditionNode instanceof EBooleanConstant || userIfBlockNode == null) {
            throw userIfElseNode.createError(new IllegalArgumentException("extraneous if block"));
        }

        semanticScope.replicateCondition(userIfElseNode, userIfBlockNode, LastSource.class);
        semanticScope.replicateCondition(userIfElseNode, userIfBlockNode, InLoop.class);
        semanticScope.replicateCondition(userIfElseNode, userIfBlockNode, LastLoop.class);
        visit(userIfBlockNode, semanticScope.newLocalScope());

        SBlock userElseBlockNode = userIfElseNode.getElseBlockNode();

        if (userElseBlockNode == null) {
            throw userIfElseNode.createError(new IllegalArgumentException("extraneous else block."));
        }

        semanticScope.replicateCondition(userIfElseNode, userElseBlockNode, LastSource.class);
        semanticScope.replicateCondition(userIfElseNode, userElseBlockNode, InLoop.class);
        semanticScope.replicateCondition(userIfElseNode, userElseBlockNode, LastLoop.class);
        visit(userElseBlockNode, semanticScope.newLocalScope());

        if (semanticScope.getCondition(userIfBlockNode, MethodEscape.class) &&
            semanticScope.getCondition(userElseBlockNode, MethodEscape.class)) {
            semanticScope.setCondition(userIfElseNode, MethodEscape.class);
        }

        if (semanticScope.getCondition(userIfBlockNode, LoopEscape.class) &&
            semanticScope.getCondition(userElseBlockNode, LoopEscape.class)) {
            semanticScope.setCondition(userIfElseNode, LoopEscape.class);
        }

        if (semanticScope.getCondition(userIfBlockNode, AllEscape.class) &&
            semanticScope.getCondition(userElseBlockNode, AllEscape.class)) {
            semanticScope.setCondition(userIfElseNode, AllEscape.class);
        }

        if (semanticScope.getCondition(userIfBlockNode, AnyContinue.class) ||
            semanticScope.getCondition(userElseBlockNode, AnyContinue.class)) {
            semanticScope.setCondition(userIfElseNode, AnyContinue.class);
        }

        if (    semanticScope.getCondition(userIfBlockNode, AnyBreak.class) ||
                semanticScope.getCondition(userElseBlockNode, AnyBreak.class)) {
            semanticScope.setCondition(userIfElseNode, AnyBreak.class);
        }
    }

    /**
     * Visits a while statement with error checking for an extraneous loop.
     * Checks: control flow
     */
    @Override
    public void visitWhile(SWhile userWhileNode, SemanticScope semanticScope) {
        semanticScope = semanticScope.newLocalScope();

        AExpression userConditionNode = userWhileNode.getConditionNode();
        semanticScope.setCondition(userConditionNode, Read.class);
        semanticScope.putDecoration(userConditionNode, new TargetType(boolean.class));
        checkedVisit(userConditionNode, semanticScope);
        decorateWithCast(userConditionNode, semanticScope);

        SBlock userBlockNode = userWhileNode.getBlockNode();
        boolean continuous = false;

        if (userConditionNode instanceof EBooleanConstant) {
            continuous = ((EBooleanConstant)userConditionNode).getBool();

            if (continuous == false) {
                throw userWhileNode.createError(new IllegalArgumentException("extraneous while loop"));
            } else {
                semanticScope.setCondition(userWhileNode, ContinuousLoop.class);
            }

            if (userBlockNode == null) {
                throw userWhileNode.createError(new IllegalArgumentException("no paths escape from while loop"));
            }
        }

        if (userBlockNode != null) {
            semanticScope.setCondition(userBlockNode, BeginLoop.class);
            semanticScope.setCondition(userBlockNode, InLoop.class);
            visit(userBlockNode, semanticScope);

            if (semanticScope.getCondition(userBlockNode, LoopEscape.class) &&
                    semanticScope.getCondition(userBlockNode, AnyContinue.class) == false) {
                throw userWhileNode.createError(new IllegalArgumentException("extraneous while loop"));
            }

            if (continuous && semanticScope.getCondition(userBlockNode, AnyBreak.class) == false) {
                semanticScope.setCondition(userWhileNode, MethodEscape.class);
                semanticScope.setCondition(userWhileNode, AllEscape.class);
            }
        }
    }

    /**
     * Visits a do-while statement with error checking for an extraneous loop.
     * Checks: control flow
     */
    @Override
    public void visitDo(SDo userDoNode, SemanticScope semanticScope) {
        semanticScope = semanticScope.newLocalScope();

        SBlock userBlockNode = userDoNode.getBlockNode();

        if (userBlockNode == null) {
            throw userDoNode.createError(new IllegalArgumentException("extraneous do-while loop"));
        }

        semanticScope.setCondition(userBlockNode, BeginLoop.class);
        semanticScope.setCondition(userBlockNode, InLoop.class);
        visit(userBlockNode, semanticScope);

        if (semanticScope.getCondition(userBlockNode, LoopEscape.class) &&
                semanticScope.getCondition(userBlockNode, AnyContinue.class) == false) {
            throw userDoNode.createError(new IllegalArgumentException("extraneous do-while loop"));
        }

        AExpression userConditionNode = userDoNode.getConditionNode();

        semanticScope.setCondition(userConditionNode, Read.class);
        semanticScope.putDecoration(userConditionNode, new TargetType(boolean.class));
        checkedVisit(userConditionNode, semanticScope);
        decorateWithCast(userConditionNode, semanticScope);

        boolean continuous;

        if (userConditionNode instanceof EBooleanConstant) {
            continuous = ((EBooleanConstant)userConditionNode).getBool();

            if (continuous == false) {
                throw userDoNode.createError(new IllegalArgumentException("extraneous do-while loop"));
            } else {
                semanticScope.setCondition(userDoNode, ContinuousLoop.class);
            }

            if (semanticScope.getCondition(userBlockNode, AnyBreak.class) == false) {
                semanticScope.setCondition(userDoNode, MethodEscape.class);
                semanticScope.setCondition(userDoNode, AllEscape.class);
            }
        }
    }

    /**
     * Visits a for statement with error checking for an extraneous loop.
     * Checks: control flow
     */
    @Override
    public void visitFor(SFor userForNode, SemanticScope semanticScope) {
        semanticScope = semanticScope.newLocalScope();

        ANode userInitializerNode = userForNode.getInitializerNode();

        if (userInitializerNode != null) {
            if (userInitializerNode instanceof SDeclBlock) {
                visit(userInitializerNode, semanticScope);
            } else if (userInitializerNode instanceof AExpression) {
                checkedVisit((AExpression)userInitializerNode, semanticScope);
            } else {
                throw userForNode.createError(new IllegalStateException("illegal tree structure"));
            }
        }

        AExpression userConditionNode = userForNode.getConditionNode();
        SBlock userBlockNode = userForNode.getBlockNode();
        boolean continuous = false;

        if (userConditionNode != null) {
            semanticScope.setCondition(userConditionNode, Read.class);
            semanticScope.putDecoration(userConditionNode, new TargetType(boolean.class));
            checkedVisit(userConditionNode, semanticScope);
            decorateWithCast(userConditionNode, semanticScope);

            if (userConditionNode instanceof EBooleanConstant) {
                continuous = ((EBooleanConstant)userConditionNode).getBool();

                if (continuous == false) {
                    throw userForNode.createError(new IllegalArgumentException("extraneous for loop"));
                }

                if (userBlockNode == null) {
                    throw userForNode.createError(new IllegalArgumentException("no paths escape from for loop"));
                }
            }
        } else {
            continuous = true;
        }

        AExpression userAfterthoughtNode = userForNode.getAfterthoughtNode();

        if (userAfterthoughtNode != null) {
            checkedVisit(userAfterthoughtNode, semanticScope);
        }

        if (userBlockNode != null) {
            semanticScope.setCondition(userBlockNode, BeginLoop.class);
            semanticScope.setCondition(userBlockNode, InLoop.class);
            visit(userBlockNode, semanticScope);

            if (semanticScope.getCondition(userBlockNode, LoopEscape.class) &&
                    semanticScope.getCondition(userBlockNode, AnyContinue.class) == false) {
                throw userForNode.createError(new IllegalArgumentException("extraneous for loop"));
            }

            if (continuous && semanticScope.getCondition(userBlockNode, AnyBreak.class) == false) {
                semanticScope.setCondition(userForNode, MethodEscape.class);
                semanticScope.setCondition(userForNode, AllEscape.class);
            }
        }
    }

    /**
     * Visits a for-each statement which and adds an internal variable for a generated iterator.
     * Checks: control flow
     */
    @Override
    public void visitEach(SEach userEachNode, SemanticScope semanticScope) {
        AExpression userIterableNode = userEachNode.getIterableNode();
        semanticScope.setCondition(userIterableNode, Read.class);
        checkedVisit(userIterableNode, semanticScope);

        String canonicalTypeName = userEachNode.getCanonicalTypeName();
        Class<?> type = semanticScope.getScriptScope().getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

        if (type == null) {
            throw userEachNode.createError(new IllegalArgumentException(
                    "invalid foreach loop: type [" + canonicalTypeName + "] not found"));
        }

        semanticScope = semanticScope.newLocalScope();

        Location location = userEachNode.getLocation();
        String symbol = userEachNode.getSymbol();
        Variable variable = semanticScope.defineVariable(location, type, symbol, true);
        semanticScope.putDecoration(userEachNode, new SemanticVariable(variable));

        SBlock userBlockNode = userEachNode.getBlockNode();

        if (userBlockNode == null) {
            throw userEachNode.createError(new IllegalArgumentException("extraneous foreach loop"));
        }

        semanticScope.setCondition(userBlockNode, BeginLoop.class);
        semanticScope.setCondition(userBlockNode, InLoop.class);
        visit(userBlockNode, semanticScope);

        if (semanticScope.getCondition(userBlockNode, LoopEscape.class) &&
                semanticScope.getCondition(userBlockNode, AnyContinue.class) == false) {
            throw userEachNode.createError(new IllegalArgumentException("extraneous foreach loop"));
        }

        Class<?> iterableValueType = semanticScope.getDecoration(userIterableNode, ValueType.class).getValueType();

        if (iterableValueType.isArray()) {
            PainlessCast painlessCast =
                    AnalyzerCaster.getLegalCast(location, iterableValueType.getComponentType(), variable.getType(), true, true);

            if (painlessCast != null) {
                semanticScope.putDecoration(userEachNode, new ExpressionPainlessCast(painlessCast));
            }
        } else if (iterableValueType == def.class || Iterable.class.isAssignableFrom(iterableValueType)) {
            if (iterableValueType != def.class) {
                PainlessMethod method = semanticScope.getScriptScope().getPainlessLookup().
                        lookupPainlessMethod(iterableValueType, false, "iterator", 0);

                if (method == null) {
                    throw userEachNode.createError(new IllegalArgumentException("invalid foreach loop: " +
                            "method [" + typeToCanonicalTypeName(iterableValueType) + ", iterator/0] not found"));
                }

                semanticScope.putDecoration(userEachNode, new IterablePainlessMethod(method));
            }

            PainlessCast painlessCast = AnalyzerCaster.getLegalCast(location, def.class, type, true, true);

            if (painlessCast != null) {
                semanticScope.putDecoration(userEachNode, new ExpressionPainlessCast(painlessCast));
            }
        } else {
            throw userEachNode.createError(new IllegalArgumentException("invalid foreach loop: " +
                    "cannot iterate over type [" + PainlessLookupUtility.typeToCanonicalTypeName(iterableValueType) + "]."));
        }
    }

    /**
     * Visits a declaration block which contains one-to-many declarations.
     */
    @Override
    public void visitDeclBlock(SDeclBlock userDeclBlockNode, SemanticScope semanticScope) {
        for (SDeclaration userDeclarationNode : userDeclBlockNode.getDeclarationNodes()) {
            visit(userDeclarationNode, semanticScope);
        }
    }

    /**
     * Visits a declaration and defines a variable with a type and optionally a value.
     * Checks: type validation
     */
    @Override
    public void visitDeclaration(SDeclaration userDeclarationNode, SemanticScope semanticScope) {
        ScriptScope scriptScope = semanticScope.getScriptScope();
        String symbol = userDeclarationNode.getSymbol();

        if (scriptScope.getPainlessLookup().isValidCanonicalClassName(symbol)) {
            throw userDeclarationNode.createError(new IllegalArgumentException(
                    "invalid declaration: type [" + symbol + "] cannot be a name"));
        }

        String canonicalTypeName = userDeclarationNode.getCanonicalTypeName();
        Class<?> type = scriptScope.getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

        if (type == null) {
            throw userDeclarationNode.createError(new IllegalArgumentException(
                    "invalid declaration: cannot resolve type [" + canonicalTypeName + "]"));
        }

        AExpression userValueNode = userDeclarationNode.getValueNode();

        if (userValueNode != null) {
            semanticScope.setCondition(userValueNode, Read.class);
            semanticScope.putDecoration(userValueNode, new TargetType(type));
            checkedVisit(userValueNode, semanticScope);
            decorateWithCast(userValueNode, semanticScope);
        }

        Location location = userDeclarationNode.getLocation();
        Variable variable = semanticScope.defineVariable(location, type, symbol, false);
        semanticScope.putDecoration(userDeclarationNode, new SemanticVariable(variable));
    }

    /**
     * Visits a return statement and casts the value to the return type if possible.
     * Checks: type validation
     */
    @Override
    public void visitReturn(SReturn userReturnNode, SemanticScope semanticScope) {
        AExpression userValueNode = userReturnNode.getValueNode();

        if (userValueNode == null) {
            if (semanticScope.getReturnType() != void.class) {
                throw userReturnNode.createError(new ClassCastException("cannot cast from " +
                        "[" + semanticScope.getReturnCanonicalTypeName() + "] to " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(void.class) + "]"));
            }
        } else {
            semanticScope.setCondition(userValueNode, Read.class);
            semanticScope.putDecoration(userValueNode, new TargetType(semanticScope.getReturnType()));
            semanticScope.setCondition(userValueNode, Internal.class);
            checkedVisit(userValueNode, semanticScope);
            decorateWithCast(userValueNode, semanticScope);
        }

        semanticScope.setCondition(userReturnNode, MethodEscape.class);
        semanticScope.setCondition(userReturnNode, LoopEscape.class);
        semanticScope.setCondition(userReturnNode, AllEscape.class);
    }

    /**
     * Visits an expression that is also considered a statement.
     * Checks: control flow, type validation
     */
    @Override
    public void visitExpression(SExpression userExpressionNode, SemanticScope semanticScope) {
        Class<?> rtnType = semanticScope.getReturnType();
        boolean isVoid = rtnType == void.class;
        boolean lastSource = semanticScope.getCondition(userExpressionNode, LastSource.class);
        AExpression userStatementNode = userExpressionNode.getStatementNode();

        if (lastSource && isVoid == false) {
            semanticScope.setCondition(userStatementNode, Read.class);
        }

        checkedVisit(userStatementNode, semanticScope);
        Class<?> expressionValueType = semanticScope.getDecoration(userStatementNode, ValueType.class).getValueType();
        boolean rtn = lastSource && isVoid == false && expressionValueType != void.class;

        if (rtn) {
            semanticScope.putDecoration(userStatementNode, new TargetType(rtnType));
            semanticScope.setCondition(userStatementNode, Internal.class);
            decorateWithCast(userStatementNode, semanticScope);

            semanticScope.setCondition(userExpressionNode, MethodEscape.class);
            semanticScope.setCondition(userExpressionNode, LoopEscape.class);
            semanticScope.setCondition(userExpressionNode, AllEscape.class);
        }
    }

    /**
     * Visits a try statement.
     * Checks: control flow
     */
    @Override
    public void visitTry(STry userTryNode, SemanticScope semanticScope) {
        SBlock userBlockNode = userTryNode.getBlockNode();

        if (userBlockNode == null) {
            throw userTryNode.createError(new IllegalArgumentException("extraneous try statement"));
        }

        semanticScope.replicateCondition(userTryNode, userBlockNode, LastSource.class);
        semanticScope.replicateCondition(userTryNode, userBlockNode, InLoop.class);
        semanticScope.replicateCondition(userTryNode, userBlockNode, LastLoop.class);
        visit(userBlockNode, semanticScope.newLocalScope());

        boolean methodEscape = semanticScope.getCondition(userBlockNode, MethodEscape.class);
        boolean loopEscape = semanticScope.getCondition(userBlockNode, LoopEscape.class);
        boolean allEscape = semanticScope.getCondition(userBlockNode, AllEscape.class);
        boolean anyContinue = semanticScope.getCondition(userBlockNode, AnyContinue.class);
        boolean anyBreak = semanticScope.getCondition(userBlockNode, AnyBreak.class);

        for (SCatch userCatchNode : userTryNode.getCatchNodes()) {
            semanticScope.replicateCondition(userTryNode, userCatchNode, LastSource.class);
            semanticScope.replicateCondition(userTryNode, userCatchNode, InLoop.class);
            semanticScope.replicateCondition(userTryNode, userCatchNode, LastLoop.class);
            visit(userCatchNode, semanticScope.newLocalScope());

            methodEscape &= semanticScope.getCondition(userCatchNode, MethodEscape.class);
            loopEscape &= semanticScope.getCondition(userCatchNode, LoopEscape.class);
            allEscape &= semanticScope.getCondition(userCatchNode, AllEscape.class);
            anyContinue |= semanticScope.getCondition(userCatchNode, AnyContinue.class);
            anyBreak |= semanticScope.getCondition(userCatchNode, AnyBreak.class);
        }

        if (methodEscape) {
            semanticScope.setCondition(userTryNode, MethodEscape.class);
        }

        if (loopEscape) {
            semanticScope.setCondition(userTryNode, LoopEscape.class);
        }

        if (allEscape) {
            semanticScope.setCondition(userTryNode, AllEscape.class);
        }

        if (anyContinue) {
            semanticScope.setCondition(userTryNode, AnyContinue.class);
        }

        if (anyBreak) {
            semanticScope.setCondition(userTryNode, AnyBreak.class);
        }
    }

    /**
     * Visits a catch statement and defines a variable for the caught exception.
     * Checks: control flow, type validation
     */
    @Override
    public void visitCatch(SCatch userCatchNode, SemanticScope semanticScope) {
        ScriptScope scriptScope = semanticScope.getScriptScope();
        String symbol = userCatchNode.getSymbol();

        if (scriptScope.getPainlessLookup().isValidCanonicalClassName(symbol)) {
            throw userCatchNode.createError(new IllegalArgumentException(
                    "invalid catch declaration: type [" + symbol + "] cannot be a name"));
        }

        String canonicalTypeName = userCatchNode.getCanonicalTypeName();
        Class<?> type = scriptScope.getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

        if (type == null) {
            throw userCatchNode.createError(new IllegalArgumentException(
                    "invalid catch declaration: cannot resolve type [" + canonicalTypeName + "]"));
        }

        Location location = userCatchNode.getLocation();
        Variable variable = semanticScope.defineVariable(location, type, symbol, false);
        semanticScope.putDecoration(userCatchNode, new SemanticVariable(variable));
        Class<?> baseException = userCatchNode.getBaseException();

        if (userCatchNode.getBaseException().isAssignableFrom(type) == false) {
            throw userCatchNode.createError(new ClassCastException(
                    "cannot cast from [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "] " +
                            "to [" + PainlessLookupUtility.typeToCanonicalTypeName(baseException) + "]"));
        }

        SBlock userBlockNode = userCatchNode.getBlockNode();

        if (userBlockNode != null) {
            semanticScope.replicateCondition(userCatchNode, userBlockNode, LastSource.class);
            semanticScope.replicateCondition(userCatchNode, userBlockNode, InLoop.class);
            semanticScope.replicateCondition(userCatchNode, userBlockNode, LastLoop.class);
            visit(userBlockNode, semanticScope);

            semanticScope.replicateCondition(userBlockNode, userCatchNode, MethodEscape.class);
            semanticScope.replicateCondition(userBlockNode, userCatchNode, LoopEscape.class);
            semanticScope.replicateCondition(userBlockNode, userCatchNode, AllEscape.class);
            semanticScope.replicateCondition(userBlockNode, userCatchNode, AnyContinue.class);
            semanticScope.replicateCondition(userBlockNode, userCatchNode, AnyBreak.class);
        }
    }

    /**
     * Visits a throw statement.
     * Checks: type validation
     */
    @Override
    public void visitThrow(SThrow userThrowNode, SemanticScope semanticScope) {
        AExpression userExpressionNode = userThrowNode.getExpressionNode();

        semanticScope.setCondition(userExpressionNode, Read.class);
        semanticScope.putDecoration(userExpressionNode, new TargetType(Exception.class));
        checkedVisit(userExpressionNode, semanticScope);
        decorateWithCast(userExpressionNode, semanticScope);

        semanticScope.setCondition(userThrowNode, MethodEscape.class);
        semanticScope.setCondition(userThrowNode, LoopEscape.class);
        semanticScope.setCondition(userThrowNode, AllEscape.class);
    }

    /**
     * Visits a continue statement.
     * Checks: control flow
     */
    @Override
    public void visitContinue(SContinue userContinueNode, SemanticScope semanticScope) {
        if (semanticScope.getCondition(userContinueNode, InLoop.class) == false) {
            throw userContinueNode.createError(new IllegalArgumentException("invalid continue statement: not inside loop"));
        }

        if (semanticScope.getCondition(userContinueNode, LastLoop.class)) {
            throw userContinueNode.createError(new IllegalArgumentException("extraneous continue statement"));
        }

        semanticScope.setCondition(userContinueNode, AllEscape.class);
        semanticScope.setCondition(userContinueNode, AnyContinue.class);
    }

    /**
     * Visits a break statement.
     * Checks: control flow
     */
    @Override
    public void visitBreak(SBreak userBreakNode, SemanticScope semanticScope) {
        if (semanticScope.getCondition(userBreakNode, InLoop.class) == false) {
            throw userBreakNode.createError(new IllegalArgumentException("invalid break statement: not inside loop"));
        }

        semanticScope.setCondition(userBreakNode, AllEscape.class);
        semanticScope.setCondition(userBreakNode, LoopEscape.class);
        semanticScope.setCondition(userBreakNode, AnyBreak.class);
    }

    /**
     * Visits an assignment expression which handles both assignment and compound assignment.
     * Checks: type validation
     */
    @Override
    public void visitAssignment(EAssignment userAssignmentNode, SemanticScope semanticScope) {
        AExpression userLeftNode = userAssignmentNode.getLeftNode();
        semanticScope.replicateCondition(userAssignmentNode, userLeftNode, Read.class);
        semanticScope.setCondition(userLeftNode, Write.class);
        checkedVisit(userLeftNode, semanticScope);
        Class<?> leftValueType = semanticScope.getDecoration(userLeftNode, Decorations.ValueType.class).getValueType();

        AExpression userRightNode = userAssignmentNode.getRightNode();
        semanticScope.setCondition(userRightNode, Read.class);

        Operation operation = userAssignmentNode.getOperation();

        if (operation != null) {
            checkedVisit(userRightNode, semanticScope);
            Class<?> rightValueType = semanticScope.getDecoration(userRightNode, ValueType.class).getValueType();

            Class<?> compoundType;
            Class<?> shiftType = null;
            boolean isShift = false;

            if (operation == Operation.MUL) {
                compoundType = AnalyzerCaster.promoteNumeric(leftValueType, rightValueType, true);
            } else if (operation == Operation.DIV) {
                compoundType = AnalyzerCaster.promoteNumeric(leftValueType, rightValueType, true);
            } else if (operation == Operation.REM) {
                compoundType = AnalyzerCaster.promoteNumeric(leftValueType, rightValueType, true);
            } else if (operation == Operation.ADD) {
                compoundType = AnalyzerCaster.promoteAdd(leftValueType, rightValueType);
            } else if (operation == Operation.SUB) {
                compoundType = AnalyzerCaster.promoteNumeric(leftValueType, rightValueType, true);
            } else if (operation == Operation.LSH) {
                compoundType = AnalyzerCaster.promoteNumeric(leftValueType, false);
                shiftType = AnalyzerCaster.promoteNumeric(rightValueType, false);
                isShift = true;
            } else if (operation == Operation.RSH) {
                compoundType = AnalyzerCaster.promoteNumeric(leftValueType, false);
                shiftType = AnalyzerCaster.promoteNumeric(rightValueType, false);
                isShift = true;
            } else if (operation == Operation.USH) {
                compoundType = AnalyzerCaster.promoteNumeric(leftValueType, false);
                shiftType = AnalyzerCaster.promoteNumeric(rightValueType, false);
                isShift = true;
            } else if (operation == Operation.BWAND) {
                compoundType = AnalyzerCaster.promoteXor(leftValueType, rightValueType);
            } else if (operation == Operation.XOR) {
                compoundType = AnalyzerCaster.promoteXor(leftValueType, rightValueType);
            } else if (operation == Operation.BWOR) {
                compoundType = AnalyzerCaster.promoteXor(leftValueType, rightValueType);
            } else {
                throw userAssignmentNode.createError(new IllegalStateException("illegal tree structure"));
            }

            if (compoundType == null || (isShift && shiftType == null)) {
                throw userAssignmentNode.createError(new ClassCastException("invalid compound assignment: " +
                        "cannot apply [" + operation.symbol + "=] to types [" + leftValueType + "] and [" + rightValueType + "]"));
            }

            boolean cat = operation == Operation.ADD && compoundType == String.class;

            if (cat && userRightNode instanceof EBinary &&
                    ((EBinary)userRightNode).getOperation() == Operation.ADD && rightValueType == String.class) {
                semanticScope.setCondition(userRightNode, Concatenate.class);
            }

            if (isShift) {
                if (compoundType == def.class) {
                    // shifts are promoted independently, but for the def type, we need object.
                    semanticScope.putDecoration(userRightNode, new TargetType(def.class));
                } else if (shiftType == long.class) {
                    semanticScope.putDecoration(userRightNode, new TargetType(int.class));
                    semanticScope.setCondition(userRightNode, Explicit.class);
                } else {
                    semanticScope.putDecoration(userRightNode, new TargetType(shiftType));
                }
            } else {
                semanticScope.putDecoration(userRightNode, new TargetType(compoundType));
            }

            decorateWithCast(userRightNode, semanticScope);

            Location location = userAssignmentNode.getLocation();
            PainlessCast upcast = AnalyzerCaster.getLegalCast(location, leftValueType, compoundType, false, false);
            PainlessCast downcast = AnalyzerCaster.getLegalCast(location, compoundType, leftValueType, true, false);

            semanticScope.putDecoration(userAssignmentNode, new CompoundType(compoundType));

            if (cat) {
                semanticScope.setCondition(userAssignmentNode, Concatenate.class);
            }

            if (upcast != null) {
                semanticScope.putDecoration(userAssignmentNode, new UpcastPainlessCast(upcast));
            }

            if (downcast != null) {
                semanticScope.putDecoration(userAssignmentNode, new DowncastPainlessCast(downcast));
            }
            // if the lhs node is a def optimized node we update the actual type to remove the need for a cast
        } else if (semanticScope.getCondition(userLeftNode, DefOptimized.class)) {
            checkedVisit(userRightNode, semanticScope);
            Class<?> rightValueType = semanticScope.getDecoration(userRightNode, ValueType.class).getValueType();

            if (rightValueType == void.class) {
                throw userAssignmentNode.createError(new IllegalArgumentException(
                        "invalid assignment: cannot assign type [" + PainlessLookupUtility.typeToCanonicalTypeName(void.class) + "]"));
            }

            semanticScope.putDecoration(userLeftNode, new ValueType(rightValueType));
            leftValueType = rightValueType;
            // Otherwise, we must adapt the rhs type to the lhs type with a cast.
        } else {
            semanticScope.putDecoration(userRightNode, new TargetType(leftValueType));
            checkedVisit(userRightNode, semanticScope);
            decorateWithCast(userRightNode, semanticScope);
        }

        semanticScope.putDecoration(userAssignmentNode,
                new ValueType(semanticScope.getCondition(userAssignmentNode, Read.class) ? leftValueType : void.class));
    }

    /**
     * Visits a unary expression which special-cases a negative operator when the child
     * is a constant expression to handle the maximum negative values appropriately.
     * Checks: type validation
     */
    @Override
    public void visitUnary(EUnary userUnaryNode, SemanticScope semanticScope) {
        Operation operation = userUnaryNode.getOperation();

        if (semanticScope.getCondition(userUnaryNode, Write.class)) {
            throw userUnaryNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        if (semanticScope.getCondition(userUnaryNode, Read.class) == false) {
            throw userUnaryNode.createError(new IllegalArgumentException(
                    "not a statement: result not used from " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        AExpression userChildNode = userUnaryNode.getChildNode();
        Class<?> valueType;
        Class<?> unaryType = null;

        if (operation == Operation.SUB && (userChildNode instanceof ENumeric || userChildNode instanceof EDecimal)) {
            semanticScope.setCondition(userChildNode, Read.class);
            semanticScope.copyDecoration(userUnaryNode, userChildNode, TargetType.class);
            semanticScope.replicateCondition(userUnaryNode, userChildNode, Explicit.class);
            semanticScope.replicateCondition(userUnaryNode, userChildNode, Internal.class);
            semanticScope.setCondition(userChildNode, Negate.class);
            checkedVisit(userChildNode, semanticScope);

            if (semanticScope.hasDecoration(userUnaryNode, TargetType.class)) {
                decorateWithCast(userChildNode, semanticScope);
            }

            valueType = semanticScope.getDecoration(userChildNode, ValueType.class).getValueType();
        } else {
            if (operation == Operation.NOT) {
                semanticScope.setCondition(userChildNode, Read.class);
                semanticScope.putDecoration(userChildNode, new TargetType(boolean.class));
                checkedVisit(userChildNode, semanticScope);
                decorateWithCast(userChildNode, semanticScope);

                valueType = boolean.class;
            } else if (operation == Operation.BWNOT || operation == Operation.ADD || operation == Operation.SUB) {
                semanticScope.setCondition(userChildNode, Read.class);
                checkedVisit(userChildNode, semanticScope);
                Class<?> childValueType = semanticScope.getDecoration(userChildNode, ValueType.class).getValueType();

                unaryType = AnalyzerCaster.promoteNumeric(childValueType, operation != Operation.BWNOT);

                if (unaryType == null) {
                    throw userUnaryNode.createError(new ClassCastException("cannot apply the " + operation.name + " operator " +
                            "[" + operation.symbol + "] to the type " +
                            "[" + PainlessLookupUtility.typeToCanonicalTypeName(childValueType) + "]"));
                }

                semanticScope.putDecoration(userChildNode, new TargetType(unaryType));
                decorateWithCast(userChildNode, semanticScope);

                TargetType targetType = semanticScope.getDecoration(userUnaryNode, TargetType.class);

                if (unaryType == def.class && targetType != null) {
                    valueType = targetType.getTargetType();
                } else {
                    valueType = unaryType;
                }
            } else {
                throw userUnaryNode.createError(new IllegalStateException("unexpected unary operation [" + operation.name + "]"));
            }
        }

        semanticScope.putDecoration(userUnaryNode, new ValueType(valueType));

        if (unaryType != null) {
            semanticScope.putDecoration(userUnaryNode, new UnaryType(unaryType));
        }
    }

    /**
     * Visits a binary expression which covers all the mathematical operators.
     * Checks: type validation
     */
    @Override
    public void visitBinary(EBinary userBinaryNode, SemanticScope semanticScope) {
        Operation operation = userBinaryNode.getOperation();

        if (semanticScope.getCondition(userBinaryNode, Write.class)) {
            throw userBinaryNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        if (semanticScope.getCondition(userBinaryNode, Read.class) == false) {
            throw userBinaryNode.createError(new IllegalArgumentException(
                    "not a statement: result not used from " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        AExpression userLeftNode = userBinaryNode.getLeftNode();
        semanticScope.setCondition(userLeftNode, Read.class);
        checkedVisit(userLeftNode, semanticScope);
        Class<?> leftValueType = semanticScope.getDecoration(userLeftNode, ValueType.class).getValueType();

        AExpression userRightNode = userBinaryNode.getRightNode();
        semanticScope.setCondition(userRightNode, Read.class);
        checkedVisit(userRightNode, semanticScope);
        Class<?> rightValueType = semanticScope.getDecoration(userRightNode, ValueType.class).getValueType();

        Class<?> valueType;
        Class<?> binaryType;
        Class<?> shiftType = null;

        if (operation == Operation.FIND || operation == Operation.MATCH) {
            semanticScope.putDecoration(userLeftNode, new TargetType(String.class));
            semanticScope.putDecoration(userRightNode, new TargetType(Pattern.class));
            decorateWithCast(userLeftNode, semanticScope);
            decorateWithCast(userRightNode, semanticScope);
            binaryType = boolean.class;
            valueType = boolean.class;
        } else {
            if (operation == Operation.MUL || operation == Operation.DIV || operation == Operation.REM) {
                binaryType = AnalyzerCaster.promoteNumeric(leftValueType, rightValueType, true);
            } else if (operation == Operation.ADD) {
                binaryType = AnalyzerCaster.promoteAdd(leftValueType, rightValueType);
            } else if (operation == Operation.SUB) {
                binaryType = AnalyzerCaster.promoteNumeric(leftValueType, rightValueType, true);
            } else if (operation == Operation.LSH || operation == Operation.RSH || operation == Operation.USH) {
                binaryType = AnalyzerCaster.promoteNumeric(leftValueType, false);
                shiftType = AnalyzerCaster.promoteNumeric(rightValueType, false);

                if (shiftType == null) {
                    binaryType = null;
                }
            } else if (operation == Operation.BWOR || operation == Operation.BWAND) {
                binaryType = AnalyzerCaster.promoteNumeric(leftValueType, rightValueType, false);
            } else if (operation == Operation.XOR) {
                binaryType = AnalyzerCaster.promoteXor(leftValueType, rightValueType);
            } else {
                throw userBinaryNode.createError(new IllegalStateException("unexpected binary operation [" + operation.name + "]"));
            }

            if (binaryType == null) {
                throw userBinaryNode.createError(new ClassCastException("cannot apply the " + operation.name + " operator " +
                        "[" + operation.symbol + "] to the types " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(leftValueType) + "] and " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(rightValueType) + "]"));
            }

            valueType = binaryType;

            if (operation == Operation.ADD && binaryType == String.class) {
                if (userLeftNode instanceof EBinary &&
                        ((EBinary)userLeftNode).getOperation() == Operation.ADD && leftValueType == String.class) {
                    semanticScope.setCondition(userLeftNode, Concatenate.class);
                }

                if (userRightNode instanceof EBinary &&
                        ((EBinary)userRightNode).getOperation() == Operation.ADD && rightValueType == String.class) {
                    semanticScope.setCondition(userRightNode, Concatenate.class);
                }
            } else if (binaryType == def.class || shiftType == def.class) {
                TargetType targetType = semanticScope.getDecoration(userBinaryNode, TargetType.class);

                if (targetType != null) {
                    valueType = targetType.getTargetType();
                }
            } else {
                semanticScope.putDecoration(userLeftNode, new TargetType(binaryType));

                if (operation == Operation.LSH || operation == Operation.RSH || operation == Operation.USH) {
                    if (shiftType == long.class) {
                        semanticScope.putDecoration(userRightNode, new TargetType(int.class));
                        semanticScope.setCondition(userRightNode, Explicit.class);
                    } else {
                        semanticScope.putDecoration(userRightNode, new TargetType(shiftType));
                    }
                } else {
                    semanticScope.putDecoration(userRightNode, new TargetType(binaryType));
                }

                decorateWithCast(userLeftNode, semanticScope);
                decorateWithCast(userRightNode, semanticScope);
            }
        }

        semanticScope.putDecoration(userBinaryNode, new ValueType(valueType));
        semanticScope.putDecoration(userBinaryNode, new BinaryType(binaryType));

        if (shiftType != null) {
            semanticScope.putDecoration(userBinaryNode, new ShiftType(shiftType));
        }
    }

    /**
     * Visits a boolean comp expression which covers boolean comparision operators.
     * Checks: type validation
     */
    @Override
    public void visitBooleanComp(EBooleanComp userBooleanCompNode, SemanticScope semanticScope) {
        Operation operation = userBooleanCompNode.getOperation();

        if (semanticScope.getCondition(userBooleanCompNode, Write.class)) {
            throw userBooleanCompNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        if (semanticScope.getCondition(userBooleanCompNode, Read.class) == false) {
            throw userBooleanCompNode.createError(new IllegalArgumentException(
                    "not a statement: result not used from " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        AExpression userLeftNode = userBooleanCompNode.getLeftNode();
        semanticScope.setCondition(userLeftNode, Read.class);
        semanticScope.putDecoration(userLeftNode, new TargetType(boolean.class));
        checkedVisit(userLeftNode, semanticScope);
        decorateWithCast(userLeftNode, semanticScope);

        AExpression userRightNode = userBooleanCompNode.getRightNode();
        semanticScope.setCondition(userRightNode, Read.class);
        semanticScope.putDecoration(userRightNode, new TargetType(boolean.class));
        checkedVisit(userRightNode, semanticScope);
        decorateWithCast(userRightNode, semanticScope);

        semanticScope.putDecoration(userBooleanCompNode, new ValueType(boolean.class));
    }

    /**
     * Visits a comp expression which covers mathematical comparision operators.
     * Checks: type validation
     */
    @Override
    public void visitComp(EComp userCompNode, SemanticScope semanticScope) {
        Operation operation = userCompNode.getOperation();

        if (semanticScope.getCondition(userCompNode, Write.class)) {
            throw userCompNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        if (semanticScope.getCondition(userCompNode, Read.class) == false) {
            throw userCompNode.createError(new IllegalArgumentException(
                    "not a statement: result not used from " + operation.name + " operation " + "[" + operation.symbol + "]"));
        }

        AExpression userLeftNode = userCompNode.getLeftNode();
        semanticScope.setCondition(userLeftNode, Read.class);
        checkedVisit(userLeftNode, semanticScope);
        Class<?> leftValueType = semanticScope.getDecoration(userLeftNode, ValueType.class).getValueType();

        AExpression userRightNode = userCompNode.getRightNode();
        semanticScope.setCondition(userRightNode, Read.class);
        checkedVisit(userRightNode, semanticScope);
        Class<?> rightValueType = semanticScope.getDecoration(userRightNode, ValueType.class).getValueType();

        Class<?> promotedType;

        if (operation == Operation.EQ || operation == Operation.EQR || operation == Operation.NE || operation == Operation.NER) {
            promotedType = AnalyzerCaster.promoteEquality(leftValueType, rightValueType);
        } else if (operation == Operation.GT || operation == Operation.GTE || operation == Operation.LT || operation == Operation.LTE) {
            promotedType = AnalyzerCaster.promoteNumeric(leftValueType, rightValueType, true);
        } else {
            throw userCompNode.createError(new IllegalStateException("unexpected binary operation [" + operation.name + "]"));
        }

        if (promotedType == null) {
            throw userCompNode.createError(new ClassCastException("cannot apply the " + operation.name + " operator " +
                    "[" + operation.symbol + "] to the types " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(leftValueType) + "] and " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(rightValueType) + "]"));
        }

        if ((operation == Operation.EQ || operation == Operation.EQR || operation == Operation.NE || operation == Operation.NER)
                && userLeftNode instanceof ENull && userRightNode instanceof ENull) {
            throw userCompNode.createError(new IllegalArgumentException("extraneous comparison of [null] constants"));
        }

        if (operation == Operation.EQR || operation == Operation.NER || promotedType != def.class) {
            semanticScope.putDecoration(userLeftNode, new TargetType(promotedType));
            semanticScope.putDecoration(userRightNode, new TargetType(promotedType));
            decorateWithCast(userLeftNode, semanticScope);
            decorateWithCast(userRightNode, semanticScope);
        }

        semanticScope.putDecoration(userCompNode, new ValueType(boolean.class));
        semanticScope.putDecoration(userCompNode, new ComparisonType(promotedType));
    }

    /**
     * Visits an explicit expression which handles an explicit cast.
     * Checks: type validation
     */
    @Override
    public void visitExplicit(EExplicit userExplicitNode, SemanticScope semanticScope) {
        String canonicalTypeName = userExplicitNode.getCanonicalTypeName();

        if (semanticScope.getCondition(userExplicitNode, Write.class)) {
            throw userExplicitNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to an explicit cast with target type [" + canonicalTypeName + "]"));
        }

        if (semanticScope.getCondition(userExplicitNode, Read.class) == false) {
            throw userExplicitNode.createError(new IllegalArgumentException(
                    "not a statement: result not used from explicit cast with target type [" + canonicalTypeName + "]"));
        }

        Class<?> valueType = semanticScope.getScriptScope().getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

        if (valueType == null) {
            throw userExplicitNode.createError(new IllegalArgumentException("cannot resolve type [" + canonicalTypeName + "]"));
        }

        AExpression userChildNode = userExplicitNode.getChildNode();
        semanticScope.setCondition(userChildNode, Read.class);
        semanticScope.putDecoration(userChildNode, new TargetType(valueType));
        semanticScope.setCondition(userChildNode, Explicit.class);
        checkedVisit(userChildNode, semanticScope);
        decorateWithCast(userChildNode, semanticScope);

        semanticScope.putDecoration(userExplicitNode, new ValueType(valueType));
    }

    /**
     * Visits an instanceof expression which handles both primitive and non-primitive types.
     * Checks: type validation
     */
    @Override
    public void visitInstanceof(EInstanceof userInstanceofNode, SemanticScope semanticScope) {
        String canonicalTypeName = userInstanceofNode.getCanonicalTypeName();

        if (semanticScope.getCondition(userInstanceofNode, Write.class)) {
            throw userInstanceofNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to instanceof with target type [" + canonicalTypeName + "]"));
        }

        if (semanticScope.getCondition(userInstanceofNode, Read.class) == false) {
            throw userInstanceofNode.createError(new IllegalArgumentException(
                    "not a statement: result not used from instanceof with target type [" + canonicalTypeName + "]"));
        }

        Class<?> instanceType = semanticScope.getScriptScope().getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

        if (instanceType == null) {
            throw userInstanceofNode.createError(new IllegalArgumentException("Not a type [" + canonicalTypeName + "]."));
        }

        AExpression userExpressionNode = userInstanceofNode.getExpressionNode();
        semanticScope.setCondition(userExpressionNode, Read.class);
        checkedVisit(userExpressionNode, semanticScope);

        semanticScope.putDecoration(userInstanceofNode, new ValueType(boolean.class));
        semanticScope.putDecoration(userInstanceofNode, new InstanceType(instanceType));
    }

    /**
     * Visits a conditional expression.
     * Checks: type validation
     */
    @Override
    public void visitConditional(EConditional userConditionalNode, SemanticScope semanticScope) {
        if (semanticScope.getCondition(userConditionalNode, Write.class)) {
            throw userConditionalNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to conditional operation [?:]"));
        }

        if (semanticScope.getCondition(userConditionalNode, Read.class) == false) {
            throw userConditionalNode.createError(new IllegalArgumentException(
                    "not a statement: result not used from conditional operation [?:]"));
        }

        AExpression userConditionNode = userConditionalNode.getConditionNode();
        semanticScope.setCondition(userConditionNode, Read.class);
        semanticScope.putDecoration(userConditionNode, new TargetType(boolean.class));
        checkedVisit(userConditionNode, semanticScope);
        decorateWithCast(userConditionNode, semanticScope);

        AExpression userTrueNode = userConditionalNode.getTrueNode();
        semanticScope.setCondition(userTrueNode, Read.class);
        semanticScope.copyDecoration(userConditionalNode, userTrueNode, TargetType.class);
        semanticScope.replicateCondition(userConditionalNode, userTrueNode, Explicit.class);
        semanticScope.replicateCondition(userConditionalNode, userTrueNode, Internal.class);
        checkedVisit(userTrueNode, semanticScope);
        Class<?> leftValueType = semanticScope.getDecoration(userTrueNode, ValueType.class).getValueType();

        AExpression userFalseNode = userConditionalNode.getFalseNode();
        semanticScope.setCondition(userFalseNode, Read.class);
        semanticScope.copyDecoration(userConditionalNode, userFalseNode, TargetType.class);
        semanticScope.replicateCondition(userConditionalNode, userFalseNode, Explicit.class);
        semanticScope.replicateCondition(userConditionalNode, userFalseNode, Internal.class);
        checkedVisit(userFalseNode, semanticScope);
        Class<?> rightValueType = semanticScope.getDecoration(userFalseNode, ValueType.class).getValueType();

        TargetType targetType = semanticScope.getDecoration(userConditionalNode, TargetType.class);
        Class<?> valueType;

        if (targetType == null) {
            Class<?> promote = AnalyzerCaster.promoteConditional(leftValueType, rightValueType);

            if (promote == null) {
                throw userConditionalNode.createError(new ClassCastException("cannot apply the conditional operator [?:] to the types " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(leftValueType) + "] and " +
                        "[" + PainlessLookupUtility.typeToCanonicalTypeName(rightValueType) + "]"));
            }

            semanticScope.putDecoration(userTrueNode, new TargetType(promote));
            semanticScope.putDecoration(userFalseNode, new TargetType(promote));
            valueType = promote;
        } else {
            valueType = targetType.getTargetType();
        }

        decorateWithCast(userTrueNode, semanticScope);
        decorateWithCast(userFalseNode, semanticScope);

        semanticScope.putDecoration(userConditionalNode, new ValueType(valueType));
    }

    /**
     * Visits a elvis expression which is a shortcut for a null check on a conditional expression.
     * Checks: type validation
     */
    @Override
    public void visitElvis(EElvis userElvisNode, SemanticScope semanticScope) {
        if (semanticScope.getCondition(userElvisNode, Write.class)) {
            throw userElvisNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to elvis operation [?:]"));
        }

        if (semanticScope.getCondition(userElvisNode, Read.class) == false) {
            throw userElvisNode.createError(new IllegalArgumentException("not a statement: result not used from elvis operation [?:]"));
        }

        TargetType targetType = semanticScope.getDecoration(userElvisNode, TargetType.class);

        if (targetType != null && targetType.getTargetType().isPrimitive()) {
            throw userElvisNode.createError(new IllegalArgumentException("Elvis operator cannot return primitives"));
        }

        AExpression userLeftNode = userElvisNode.getLeftNode();
        semanticScope.setCondition(userLeftNode, Read.class);
        semanticScope.copyDecoration(userElvisNode, userLeftNode, TargetType.class);
        semanticScope.replicateCondition(userElvisNode, userLeftNode, Explicit.class);
        semanticScope.replicateCondition(userElvisNode, userLeftNode, Internal.class);
        checkedVisit(userLeftNode, semanticScope);
        Class<?> leftValueType = semanticScope.getDecoration(userLeftNode, ValueType.class).getValueType();

        AExpression userRightNode = userElvisNode.getRightNode();
        semanticScope.setCondition(userRightNode, Read.class);
        semanticScope.copyDecoration(userElvisNode, userRightNode, TargetType.class);
        semanticScope.replicateCondition(userElvisNode, userRightNode, Explicit.class);
        semanticScope.replicateCondition(userElvisNode, userRightNode, Internal.class);
        checkedVisit(userRightNode, semanticScope);
        Class<?> rightValueType = semanticScope.getDecoration(userRightNode, ValueType.class).getValueType();

        if (userLeftNode instanceof ENull) {
            throw userElvisNode.createError(new IllegalArgumentException("Extraneous elvis operator. LHS is null."));
        }
        if (    userLeftNode instanceof EBooleanConstant ||
                userLeftNode instanceof ENumeric         ||
                userLeftNode instanceof EDecimal         ||
                userLeftNode instanceof EString
        ) {
            throw userElvisNode.createError(new IllegalArgumentException("Extraneous elvis operator. LHS is a constant."));
        }
        if (leftValueType.isPrimitive()) {
            throw userElvisNode.createError(new IllegalArgumentException("Extraneous elvis operator. LHS is a primitive."));
        }
        if (userRightNode instanceof ENull) {
            throw userElvisNode.createError(new IllegalArgumentException("Extraneous elvis operator. RHS is null."));
        }

        Class<?> valueType;

        if (targetType == null) {
            Class<?> promote = AnalyzerCaster.promoteConditional(leftValueType, rightValueType);

            semanticScope.putDecoration(userLeftNode, new TargetType(promote));
            semanticScope.putDecoration(userRightNode, new TargetType(promote));
            valueType = promote;
        } else {
            valueType = targetType.getTargetType();
        }

        decorateWithCast(userLeftNode, semanticScope);
        decorateWithCast(userRightNode, semanticScope);

        semanticScope.putDecoration(userElvisNode, new ValueType(valueType));
    }

    /**
     * Visits a list init expression which is a shortcut for initializing a list with pre-defined values.
     * Checks: type validation
     */
    @Override
    public void visitListInit(EListInit userListInitNode, SemanticScope semanticScope) {
        if (semanticScope.getCondition(userListInitNode, Write.class)) {
            throw userListInitNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to list initializer"));
        }

        if (semanticScope.getCondition(userListInitNode, Read.class) == false) {
            throw userListInitNode.createError(new IllegalArgumentException("not a statement: result not used from list initializer"));
        }

        Class<?> valueType = ArrayList.class;

        PainlessConstructor constructor = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessConstructor(valueType, 0);

        if (constructor == null) {
            throw userListInitNode.createError(new IllegalArgumentException(
                    "constructor [" + typeToCanonicalTypeName(valueType) + ", <init>/0] not found"));
        }

        semanticScope.putDecoration(userListInitNode, new StandardPainlessConstructor(constructor));

        PainlessMethod method = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(valueType, false, "add", 1);

        if (method == null) {
            throw userListInitNode.createError(new IllegalArgumentException(
                    "method [" + typeToCanonicalTypeName(valueType) + ", add/1] not found"));
        }

        semanticScope.putDecoration(userListInitNode, new StandardPainlessMethod(method));

        for (AExpression userValueNode : userListInitNode.getValueNodes()) {
            semanticScope.setCondition(userValueNode, Read.class);
            semanticScope.putDecoration(userValueNode, new TargetType(def.class));
            semanticScope.setCondition(userValueNode, Internal.class);
            checkedVisit(userValueNode, semanticScope);
            decorateWithCast(userValueNode, semanticScope);
        }

        semanticScope.putDecoration(userListInitNode, new ValueType(valueType));
    }

    /**
     * Visits a map init expression which is a shortcut for initializing a map with pre-defined keys and values.
     * Checks: type validation
     */
    @Override
    public void visitMapInit(EMapInit userMapInitNode, SemanticScope semanticScope) {
        if (semanticScope.getCondition(userMapInitNode, Write.class)) {
            throw userMapInitNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to map initializer"));
        }

        if (semanticScope.getCondition(userMapInitNode, Read.class) == false) {
            throw userMapInitNode.createError(new IllegalArgumentException("not a statement: result not used from map initializer"));
        }

        Class<?> valueType = HashMap.class;

        PainlessConstructor constructor = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessConstructor(valueType, 0);

        if (constructor == null) {
            throw userMapInitNode.createError(new IllegalArgumentException(
                    "constructor [" + typeToCanonicalTypeName(valueType) + ", <init>/0] not found"));
        }

        semanticScope.putDecoration(userMapInitNode, new StandardPainlessConstructor(constructor));

        PainlessMethod method = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(valueType, false, "put", 2);

        if (method == null) {
            throw userMapInitNode.createError(new IllegalArgumentException(
                    "method [" + typeToCanonicalTypeName(valueType) + ", put/2] not found"));
        }

        semanticScope.putDecoration(userMapInitNode, new StandardPainlessMethod(method));

        List<AExpression> userKeyNodes = userMapInitNode.getKeyNodes();
        List<AExpression> userValueNodes = userMapInitNode.getValueNodes();

        if (userKeyNodes.size() != userValueNodes.size()) {
            throw userMapInitNode.createError(new IllegalStateException("Illegal tree structure."));
        }

        for (int i = 0; i < userKeyNodes.size(); ++i) {
            AExpression userKeyNode = userKeyNodes.get(i);
            semanticScope.setCondition(userKeyNode, Read.class);
            semanticScope.putDecoration(userKeyNode, new TargetType(def.class));
            semanticScope.setCondition(userKeyNode, Internal.class);
            checkedVisit(userKeyNode, semanticScope);
            decorateWithCast(userKeyNode, semanticScope);

            AExpression userValueNode = userValueNodes.get(i);
            semanticScope.setCondition(userValueNode, Read.class);
            semanticScope.putDecoration(userValueNode, new TargetType(def.class));
            semanticScope.setCondition(userValueNode, Internal.class);
            checkedVisit(userValueNode, semanticScope);
            decorateWithCast(userValueNode, semanticScope);
        }

        semanticScope.putDecoration(userMapInitNode, new ValueType(valueType));
    }

    /**
     * Visits a list init expression which either defines a standard array or initializes
     * a single-dimensional with pre-defined values.
     * Checks: type validation
     */
    @Override
    public void visitNewArray(ENewArray userNewArrayNode, SemanticScope semanticScope) {
        if (semanticScope.getCondition(userNewArrayNode, Write.class)) {
            throw userNewArrayNode.createError(new IllegalArgumentException("invalid assignment: cannot assign a value to new array"));
        }

        if (semanticScope.getCondition(userNewArrayNode, Read.class) == false) {
            throw userNewArrayNode.createError(new IllegalArgumentException("not a statement: result not used from new array"));
        }

        String canonicalTypeName = userNewArrayNode.getCanonicalTypeName();
        Class<?> valueType = semanticScope.getScriptScope().getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

        if (valueType == null) {
            throw userNewArrayNode.createError(new IllegalArgumentException("Not a type [" + canonicalTypeName + "]."));
        }

        for (AExpression userValueNode : userNewArrayNode.getValueNodes()) {
            semanticScope.setCondition(userValueNode, Read.class);
            semanticScope.putDecoration(userValueNode,
                    new TargetType(userNewArrayNode.isInitializer() ? valueType.getComponentType() : int.class));
            semanticScope.setCondition(userValueNode, Internal.class);
            checkedVisit(userValueNode, semanticScope);
            decorateWithCast(userValueNode, semanticScope);
        }

        semanticScope.putDecoration(userNewArrayNode, new ValueType(valueType));
    }

    /**
     * Visits a new obj expression which creates a new object and calls its constructor.
     * Checks: type validation
     */
    @Override
    public void visitNewObj(ENewObj userNewObjNode, SemanticScope semanticScope) {
        String canonicalTypeName =  userNewObjNode.getCanonicalTypeName();
        List<AExpression> userArgumentNodes = userNewObjNode.getArgumentNodes();
        int userArgumentsSize = userArgumentNodes.size();

        if (semanticScope.getCondition(userNewObjNode, Write.class)) {
            throw userNewObjNode.createError(new IllegalArgumentException(
                    "invalid assignment cannot assign a value to new object with constructor " +
                            "[" + canonicalTypeName + "/" + userArgumentsSize + "]"));
        }

        ScriptScope scriptScope = semanticScope.getScriptScope();
        Class<?> valueType = scriptScope.getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

        if (valueType == null) {
            throw userNewObjNode.createError(new IllegalArgumentException("Not a type [" + canonicalTypeName + "]."));
        }

        PainlessConstructor constructor = scriptScope.getPainlessLookup().lookupPainlessConstructor(valueType, userArgumentsSize);

        if (constructor == null) {
            throw userNewObjNode.createError(new IllegalArgumentException(
                    "constructor [" + typeToCanonicalTypeName(valueType) + ", <init>/" + userArgumentsSize + "] not found"));
        }

        scriptScope.putDecoration(userNewObjNode, new StandardPainlessConstructor(constructor));
        scriptScope.markNonDeterministic(constructor.annotations.containsKey(NonDeterministicAnnotation.class));

        Class<?>[] types = new Class<?>[constructor.typeParameters.size()];
        constructor.typeParameters.toArray(types);

        if (constructor.typeParameters.size() != userArgumentsSize) {
            throw userNewObjNode.createError(new IllegalArgumentException(
                    "When calling constructor on type [" + PainlessLookupUtility.typeToCanonicalTypeName(valueType) + "] " +
                    "expected [" + constructor.typeParameters.size() + "] arguments, but found [" + userArgumentsSize + "]."));
        }

        for (int i = 0; i < userArgumentsSize; ++i) {
            AExpression userArgumentNode = userArgumentNodes.get(i);

            semanticScope.setCondition(userArgumentNode, Read.class);
            semanticScope.putDecoration(userArgumentNode, new TargetType(types[i]));
            semanticScope.setCondition(userArgumentNode, Internal.class);
            checkedVisit(userArgumentNode, semanticScope);
            decorateWithCast(userArgumentNode, semanticScope);
        }

        semanticScope.putDecoration(userNewObjNode, new ValueType(valueType));
    }

    /**
     * Visits a call local expression which is a method call with no qualifier (prefix).
     * Checks: type validation, method resolution
     */
    @Override
    public void visitCallLocal(ECallLocal userCallLocalNode, SemanticScope semanticScope) {
        String methodName = userCallLocalNode.getMethodName();
        List<AExpression> userArgumentNodes = userCallLocalNode.getArgumentNodes();
        int userArgumentsSize = userArgumentNodes.size();

        if (semanticScope.getCondition(userCallLocalNode, Write.class)) {
            throw userCallLocalNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to function call [" + methodName + "/" + userArgumentsSize + "]"));
        }

        ScriptScope scriptScope = semanticScope.getScriptScope();

        FunctionTable.LocalFunction localFunction = null;
        PainlessMethod importedMethod = null;
        PainlessClassBinding classBinding = null;
        int classBindingOffset = 0;
        PainlessInstanceBinding instanceBinding = null;

        Class<?> valueType;

        localFunction = scriptScope.getFunctionTable().getFunction(methodName, userArgumentsSize);

        // user cannot call internal functions, reset to null if an internal function is found
        if (localFunction != null && localFunction.isInternal()) {
            localFunction = null;
        }

        if (localFunction == null) {
            importedMethod = scriptScope.getPainlessLookup().lookupImportedPainlessMethod(methodName, userArgumentsSize);

            if (importedMethod == null) {
                classBinding = scriptScope.getPainlessLookup().lookupPainlessClassBinding(methodName, userArgumentsSize);

                // check to see if this class binding requires an implicit this reference
                if (classBinding != null && classBinding.typeParameters.isEmpty() == false &&
                        classBinding.typeParameters.get(0) == scriptScope.getScriptClassInfo().getBaseClass()) {
                    classBinding = null;
                }

                if (classBinding == null) {
                    // This extra check looks for a possible match where the class binding requires an implicit this
                    // reference.  This is a temporary solution to allow the class binding access to data from the
                    // base script class without need for a user to add additional arguments.  A long term solution
                    // will likely involve adding a class instance binding where any instance can have a class binding
                    // as part of its API.  However, the situation at run-time is difficult and will modifications that
                    // are a substantial change if even possible to do.
                    classBinding = scriptScope.getPainlessLookup().lookupPainlessClassBinding(methodName, userArgumentsSize + 1);

                    if (classBinding != null) {
                        if (classBinding.typeParameters.isEmpty() == false &&
                                classBinding.typeParameters.get(0) == scriptScope.getScriptClassInfo().getBaseClass()) {
                            classBindingOffset = 1;
                        } else {
                            classBinding = null;
                        }
                    }

                    if (classBinding == null) {
                        instanceBinding = scriptScope.getPainlessLookup().lookupPainlessInstanceBinding(methodName, userArgumentsSize);

                        if (instanceBinding == null) {
                            throw userCallLocalNode.createError(new IllegalArgumentException(
                                    "Unknown call [" + methodName + "] with [" + userArgumentNodes + "] arguments."));
                        }
                    }
                }
            }
        }

        List<Class<?>> typeParameters;

        if (localFunction != null) {
            semanticScope.putDecoration(userCallLocalNode, new StandardLocalFunction(localFunction));

            typeParameters = new ArrayList<>(localFunction.getTypeParameters());
            valueType = localFunction.getReturnType();
        } else if (importedMethod != null) {
            semanticScope.putDecoration(userCallLocalNode, new StandardPainlessMethod(importedMethod));

            scriptScope.markNonDeterministic(importedMethod.annotations.containsKey(NonDeterministicAnnotation.class));
            typeParameters = new ArrayList<>(importedMethod.typeParameters);
            valueType = importedMethod.returnType;
        } else if (classBinding != null) {
            semanticScope.putDecoration(userCallLocalNode, new StandardPainlessClassBinding(classBinding));
            semanticScope.putDecoration(userCallLocalNode, new StandardConstant(classBindingOffset));

            scriptScope.markNonDeterministic(classBinding.annotations.containsKey(NonDeterministicAnnotation.class));
            typeParameters = new ArrayList<>(classBinding.typeParameters);
            valueType = classBinding.returnType;
        } else if (instanceBinding != null) {
            semanticScope.putDecoration(userCallLocalNode, new StandardPainlessInstanceBinding(instanceBinding));

            typeParameters = new ArrayList<>(instanceBinding.typeParameters);
            valueType = instanceBinding.returnType;
        } else {
            throw new IllegalStateException("Illegal tree structure.");
        }
        // if the class binding is using an implicit this reference then the arguments counted must
        // be incremented by 1 as the this reference will not be part of the arguments passed into
        // the class binding call
        for (int argument = 0; argument < userArgumentsSize; ++argument) {
            AExpression userArgumentNode = userArgumentNodes.get(argument);

            semanticScope.setCondition(userArgumentNode, Read.class);
            semanticScope.putDecoration(userArgumentNode, new TargetType(typeParameters.get(argument + classBindingOffset)));
            semanticScope.setCondition(userArgumentNode, Internal.class);
            checkedVisit(userArgumentNode, semanticScope);
            decorateWithCast(userArgumentNode, semanticScope);
        }

        semanticScope.putDecoration(userCallLocalNode, new ValueType(valueType));
    }

    /**
     * Visits a boolean constant expression.
     * Checks: type validation
     */
    @Override
    public void visitBooleanConstant(EBooleanConstant userBooleanConstantNode, SemanticScope semanticScope) {
        boolean bool = userBooleanConstantNode.getBool();

        if (semanticScope.getCondition(userBooleanConstantNode, Write.class)) {
            throw userBooleanConstantNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to boolean constant [" + bool + "]"));
        }

        if (semanticScope.getCondition(userBooleanConstantNode, Read.class) == false) {
            throw userBooleanConstantNode.createError(
                    new IllegalArgumentException("not a statement: boolean constant [" + bool + "] not used"));
        }

        semanticScope.putDecoration(userBooleanConstantNode, new ValueType(boolean.class));
        semanticScope.putDecoration(userBooleanConstantNode, new StandardConstant(bool));
    }

    /**
     * Visits a numeric constant expression which includes int and long.
     * Checks: type validation
     */
    @Override
    public void visitNumeric(ENumeric userNumericNode, SemanticScope semanticScope) {
        String numeric = userNumericNode.getNumeric();

        if (semanticScope.getCondition(userNumericNode, Negate.class)) {
            numeric = "-" + numeric;
        }

        if (semanticScope.getCondition(userNumericNode, Write.class)) {
            throw userNumericNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to numeric constant [" + numeric + "]"));
        }

        if (semanticScope.getCondition(userNumericNode, Read.class) == false) {
            throw userNumericNode.createError(new IllegalArgumentException(
                    "not a statement: numeric constant [" + numeric + "] not used"));
        }

        int radix = userNumericNode.getRadix();
        Class<?> valueType;
        Object constant;

        if (numeric.endsWith("d") || numeric.endsWith("D")) {
            if (radix != 10) {
                throw userNumericNode.createError(new IllegalStateException("Illegal tree structure."));
            }

            try {
                constant = Double.parseDouble(numeric.substring(0, numeric.length() - 1));
                valueType = double.class;
            } catch (NumberFormatException exception) {
                throw userNumericNode.createError(new IllegalArgumentException("Invalid double constant [" + numeric + "]."));
            }
        } else if (numeric.endsWith("f") || numeric.endsWith("F")) {
            if (radix != 10) {
                throw userNumericNode.createError(new IllegalStateException("Illegal tree structure."));
            }

            try {
                constant = Float.parseFloat(numeric.substring(0, numeric.length() - 1));
                valueType = float.class;
            } catch (NumberFormatException exception) {
                throw userNumericNode.createError(new IllegalArgumentException("Invalid float constant [" + numeric + "]."));
            }
        } else if (numeric.endsWith("l") || numeric.endsWith("L")) {
            try {
                constant = Long.parseLong(numeric.substring(0, numeric.length() - 1), radix);
                valueType = long.class;
            } catch (NumberFormatException exception) {
                throw userNumericNode.createError(new IllegalArgumentException("Invalid long constant [" + numeric + "]."));
            }
        } else {
            try {
                TargetType targetType = semanticScope.getDecoration(userNumericNode, TargetType.class);
                Class<?> sort = targetType == null ? int.class : targetType.getTargetType();
                int integer = Integer.parseInt(numeric, radix);

                if (sort == byte.class && integer >= Byte.MIN_VALUE && integer <= Byte.MAX_VALUE) {
                    constant = (byte)integer;
                    valueType = byte.class;
                } else if (sort == char.class && integer >= Character.MIN_VALUE && integer <= Character.MAX_VALUE) {
                    constant = (char)integer;
                    valueType = char.class;
                } else if (sort == short.class && integer >= Short.MIN_VALUE && integer <= Short.MAX_VALUE) {
                    constant = (short)integer;
                    valueType = short.class;
                } else {
                    constant = integer;
                    valueType = int.class;
                }
            } catch (NumberFormatException exception) {
                try {
                    // Check if we can parse as a long. If so then hint that the user might prefer that.
                    Long.parseLong(numeric, radix);
                    throw userNumericNode.createError(new IllegalArgumentException(
                            "Invalid int constant [" + numeric + "]. If you want a long constant then change it to [" + numeric + "L]."));
                } catch (NumberFormatException longNoGood) {
                    // Ignored
                }
                throw userNumericNode.createError(new IllegalArgumentException("Invalid int constant [" + numeric + "]."));
            }
        }

        semanticScope.putDecoration(userNumericNode, new ValueType(valueType));
        semanticScope.putDecoration(userNumericNode, new StandardConstant(constant));
    }

    /**
     * Visits a decimal constant expression which includes float and double.
     * Checks: type validation
     */
    @Override
    public void visitDecimal(EDecimal userDecimalNode, SemanticScope semanticScope) {
        String decimal = userDecimalNode.getDecimal();

        if (semanticScope.getCondition(userDecimalNode, Negate.class)) {
            decimal = "-" + decimal;
        }

        if (semanticScope.getCondition(userDecimalNode, Write.class)) {
            throw userDecimalNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to decimal constant [" + decimal + "]"));
        }

        if (semanticScope.getCondition(userDecimalNode, Read.class) == false) {
            throw userDecimalNode.createError(new IllegalArgumentException("not a statement: decimal constant [" + decimal + "] not used"));
        }

        Class<?> valueType;
        Object constant;

        if (decimal.endsWith("f") || decimal.endsWith("F")) {
            try {
                constant = Float.parseFloat(decimal.substring(0, decimal.length() - 1));
                valueType = float.class;
            } catch (NumberFormatException exception) {
                throw userDecimalNode.createError(new IllegalArgumentException("Invalid float constant [" + decimal + "]."));
            }
        } else {
            String toParse = decimal;
            if (toParse.endsWith("d") || decimal.endsWith("D")) {
                toParse = toParse.substring(0, decimal.length() - 1);
            }
            try {
                constant = Double.parseDouble(toParse);
                valueType = double.class;
            } catch (NumberFormatException exception) {
                throw userDecimalNode.createError(new IllegalArgumentException("Invalid double constant [" + decimal + "]."));
            }
        }

        semanticScope.putDecoration(userDecimalNode, new ValueType(valueType));
        semanticScope.putDecoration(userDecimalNode, new StandardConstant(constant));
    }

    /**
     * Visits a string constant expression.
     * Checks: type validation
     */
    @Override
    public void visitString(EString userStringNode, SemanticScope semanticScope) {
        String string = userStringNode.getString();

        if (semanticScope.getCondition(userStringNode, Write.class)) {
            throw userStringNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to string constant [" + string + "]"));
        }

        if (semanticScope.getCondition(userStringNode, Read.class) == false) {
            throw userStringNode.createError(new IllegalArgumentException("not a statement: string constant [" + string + "] not used"));
        }

        semanticScope.putDecoration(userStringNode, new ValueType(String.class));
        semanticScope.putDecoration(userStringNode, new StandardConstant(string));
    }

    /**
     * Visits a null constant expression.
     * Checks: type validation
     */
    @Override
    public void visitNull(ENull userNullNode, SemanticScope semanticScope) {
        if (semanticScope.getCondition(userNullNode, Write.class)) {
            throw userNullNode.createError(new IllegalArgumentException("invalid assignment: cannot assign a value to null constant"));
        }

        if (semanticScope.getCondition(userNullNode, Read.class) == false) {
            throw userNullNode.createError(new IllegalArgumentException("not a statement: null constant not used"));
        }

        TargetType targetType = semanticScope.getDecoration(userNullNode, TargetType.class);
        Class<?> valueType;

        if (targetType != null) {
            if (targetType.getTargetType().isPrimitive()) {
                throw userNullNode.createError(new IllegalArgumentException(
                        "Cannot cast null to a primitive type [" + targetType.getTargetCanonicalTypeName() + "]."));
            }

            valueType = targetType.getTargetType();
        } else {
            valueType = Object.class;
        }

        semanticScope.putDecoration(userNullNode, new ValueType(valueType));
    }

    /**
     * Visits a regex constant expression which does additional work to initialize a regex using pre-defined flags.
     * Checks: type validation
     */
    @Override
    public void visitRegex(ERegex userRegexNode, SemanticScope semanticScope) {
        String pattern = userRegexNode.getPattern();
        String flags = userRegexNode.getFlags();

        if (semanticScope.getCondition(userRegexNode, Write.class)) {
            throw userRegexNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to regex constant [" + pattern + "] with flags [" + flags + "]"));
        }

        if (semanticScope.getCondition(userRegexNode, Read.class) == false) {
            throw userRegexNode.createError(new IllegalArgumentException(
                    "not a statement: regex constant [" + pattern + "] with flags [" + flags + "] not used"));
        }

        if (semanticScope.getScriptScope().getCompilerSettings().areRegexesEnabled() == false) {
            throw userRegexNode.createError(new IllegalStateException("Regexes are disabled. Set [script.painless.regex.enabled] to [true] "
                    + "in elasticsearch.yaml to allow them. Be careful though, regexes break out of Painless's protection against deep "
                    + "recursion and long loops."));
        }

        Location location = userRegexNode.getLocation();

        int constant = 0;

        for (int i = 0; i < flags.length(); ++i) {
            char flag = flags.charAt(i);

            switch (flag) {
                case 'c':
                    constant |= Pattern.CANON_EQ;
                    break;
                case 'i':
                    constant |= Pattern.CASE_INSENSITIVE;
                    break;
                case 'l':
                    constant |= Pattern.LITERAL;
                    break;
                case 'm':
                    constant |= Pattern.MULTILINE;
                    break;
                case 's':
                    constant |= Pattern.DOTALL;
                    break;
                case 'U':
                    constant |= Pattern.UNICODE_CHARACTER_CLASS;
                    break;
                case 'u':
                    constant |= Pattern.UNICODE_CASE;
                    break;
                case 'x':
                    constant |= Pattern.COMMENTS;
                    break;
                default:
                    throw new IllegalArgumentException("invalid regular expression: unknown flag [" + flag + "]");
            }
        }

        try {
            Pattern.compile(pattern, constant);
        } catch (PatternSyntaxException pse) {
            throw new Location(location.getSourceName(), location.getOffset() + 1 + pse.getIndex()).createError(
                    new IllegalArgumentException("invalid regular expression: " +
                            "could not compile regex constant [" + pattern + "] with flags [" + flags + "]", pse));
        }

        semanticScope.putDecoration(userRegexNode, new ValueType(Pattern.class));
        semanticScope.putDecoration(userRegexNode, new StandardConstant(constant));
    }

    /**
     * Visits a lambda expression which adds a new internal method to the class as defined by the lambda.
     * Checks: control flow, type validation
     */
    @Override
    public void visitLambda(ELambda userLambdaNode, SemanticScope semanticScope) {
        if (semanticScope.getCondition(userLambdaNode, Write.class)) {
            throw userLambdaNode.createError(new IllegalArgumentException("invalid assignment: cannot assign a value to a lambda"));
        }

        if (semanticScope.getCondition(userLambdaNode, Read.class) == false) {
            throw userLambdaNode.createError(new IllegalArgumentException("not a statement: lambda not used"));
        }

        ScriptScope scriptScope = semanticScope.getScriptScope();
        TargetType targetType = semanticScope.getDecoration(userLambdaNode, TargetType.class);
        List<String> canonicalTypeNameParameters = userLambdaNode.getCanonicalTypeNameParameters();

        Class<?> returnType;
        List<Class<?>> typeParameters;
        PainlessMethod interfaceMethod;

        // inspect the target first, set interface method if we know it.
        if (targetType == null) {
            // we don't know anything: treat as def
            returnType = def.class;
            // don't infer any types, replace any null types with def
            typeParameters = new ArrayList<>(canonicalTypeNameParameters.size());
            for (String type : canonicalTypeNameParameters) {
                if (type == null) {
                    typeParameters.add(def.class);
                } else {
                    Class<?> typeParameter = scriptScope.getPainlessLookup().canonicalTypeNameToType(type);

                    if (typeParameter == null) {
                        throw userLambdaNode.createError(new IllegalArgumentException("cannot resolve type [" + type + "]"));
                    }

                    typeParameters.add(typeParameter);
                }
            }
        } else {
            // we know the method statically, infer return type and any unknown/def types
            interfaceMethod = scriptScope.getPainlessLookup().lookupFunctionalInterfacePainlessMethod(targetType.getTargetType());
            if (interfaceMethod == null) {
                throw userLambdaNode.createError(new IllegalArgumentException("Cannot pass lambda to " +
                        "[" + targetType.getTargetCanonicalTypeName() + "], not a functional interface"));
            }
            // check arity before we manipulate parameters
            if (interfaceMethod.typeParameters.size() != canonicalTypeNameParameters.size())
                throw new IllegalArgumentException("Incorrect number of parameters for [" + interfaceMethod.javaMethod.getName() +
                        "] in [" + targetType.getTargetCanonicalTypeName() + "]");
            // for method invocation, its allowed to ignore the return value
            if (interfaceMethod.returnType == void.class) {
                returnType = def.class;
            } else {
                returnType = interfaceMethod.returnType;
            }
            // replace any null types with the actual type
            typeParameters = new ArrayList<>(canonicalTypeNameParameters.size());
            for (int i = 0; i < canonicalTypeNameParameters.size(); i++) {
                String paramType = canonicalTypeNameParameters.get(i);
                if (paramType == null) {
                    typeParameters.add(interfaceMethod.typeParameters.get(i));
                } else {
                    Class<?> typeParameter = scriptScope.getPainlessLookup().canonicalTypeNameToType(paramType);

                    if (typeParameter == null) {
                        throw userLambdaNode.createError(new IllegalArgumentException("cannot resolve type [" + paramType + "]"));
                    }

                    typeParameters.add(typeParameter);
                }
            }
        }

        Location location = userLambdaNode.getLocation();
        List<String> parameterNames = userLambdaNode.getParameterNames();
        LambdaScope lambdaScope = semanticScope.newLambdaScope(returnType);

        for (int index = 0; index < typeParameters.size(); ++index) {
            Class<?> type = typeParameters.get(index);
            String parameterName = parameterNames.get(index);
            lambdaScope.defineVariable(location, type, parameterName, true);
        }

        SBlock userBlockNode = userLambdaNode.getBlockNode();

        if (userBlockNode.getStatementNodes().isEmpty()) {
            throw userLambdaNode.createError(new IllegalArgumentException("cannot generate empty lambda"));
        }

        semanticScope.setCondition(userBlockNode, LastSource.class);
        visit(userBlockNode, lambdaScope);

        if (semanticScope.getCondition(userBlockNode, MethodEscape.class) == false) {
            throw userLambdaNode.createError(new IllegalArgumentException("not all paths return a value for lambda"));
        }

        // prepend capture list to lambda's arguments
        List<Variable> capturedVariables = new ArrayList<>(lambdaScope.getCaptures());
        List<Class<?>> typeParametersWithCaptures = new ArrayList<>(capturedVariables.size() + typeParameters.size());
        List<String> parameterNamesWithCaptures = new ArrayList<>(capturedVariables.size() + parameterNames.size());

        for (Variable capturedVariable : capturedVariables) {
            typeParametersWithCaptures.add(capturedVariable.getType());
            parameterNamesWithCaptures.add(capturedVariable.getName());
        }

        typeParametersWithCaptures.addAll(typeParameters);
        parameterNamesWithCaptures.addAll(parameterNames);

        // desugar lambda body into a synthetic method
        String name = scriptScope.getNextSyntheticName("lambda");
        scriptScope.getFunctionTable().addFunction(name, returnType, typeParametersWithCaptures, true, true);

        Class<?> valueType;
        // setup method reference to synthetic method
        if (targetType == null) {
            String defReferenceEncoding = "Sthis." + name + "," + capturedVariables.size();
            valueType = String.class;
            semanticScope.putDecoration(userLambdaNode, new EncodingDecoration(defReferenceEncoding));
        } else {
            FunctionRef ref = FunctionRef.create(scriptScope.getPainlessLookup(), scriptScope.getFunctionTable(),
                    location, targetType.getTargetType(), "this", name, capturedVariables.size());
            valueType = targetType.getTargetType();
            semanticScope.putDecoration(userLambdaNode, new ReferenceDecoration(ref));
        }

        semanticScope.putDecoration(userLambdaNode, new ValueType(valueType));
        semanticScope.putDecoration(userLambdaNode, new MethodNameDecoration(name));
        semanticScope.putDecoration(userLambdaNode, new ReturnType(returnType));
        semanticScope.putDecoration(userLambdaNode, new TypeParameters(typeParametersWithCaptures));
        semanticScope.putDecoration(userLambdaNode, new ParameterNames(parameterNamesWithCaptures));
        semanticScope.putDecoration(userLambdaNode, new CapturesDecoration(capturedVariables));
    }

    /**
     * Visits a function ref expression which covers class function references,
     * constructor function references, and local function references.
     * Checks: type validation
     */
    @Override
    public void visitFunctionRef(EFunctionRef userFunctionRefNode, SemanticScope semanticScope) {
        ScriptScope scriptScope = semanticScope.getScriptScope();

        Location location = userFunctionRefNode.getLocation();
        String symbol = userFunctionRefNode.getSymbol();
        String methodName = userFunctionRefNode.getMethodName();
        boolean read = semanticScope.getCondition(userFunctionRefNode, Read.class);

        Class<?> type = scriptScope.getPainlessLookup().canonicalTypeNameToType(symbol);
        TargetType targetType = semanticScope.getDecoration(userFunctionRefNode, TargetType.class);
        Class<?> valueType;

        if (symbol.equals("this") || type != null)  {
            if (semanticScope.getCondition(userFunctionRefNode, Write.class)) {
                throw userFunctionRefNode.createError(new IllegalArgumentException(
                        "invalid assignment: cannot assign a value to function reference [" + symbol + ":" + methodName + "]"));
            }

            if (read == false) {
                throw userFunctionRefNode.createError(new IllegalArgumentException(
                        "not a statement: function reference [" + symbol + ":" + methodName + "] not used"));
            }

            if (targetType == null) {
                valueType = String.class;
                String defReferenceEncoding = "S" + symbol + "." + methodName + ",0";
                semanticScope.putDecoration(userFunctionRefNode, new EncodingDecoration(defReferenceEncoding));
            } else {
                FunctionRef ref = FunctionRef.create(scriptScope.getPainlessLookup(), scriptScope.getFunctionTable(),
                        location, targetType.getTargetType(), symbol, methodName, 0);
                valueType = targetType.getTargetType();
                semanticScope.putDecoration(userFunctionRefNode, new ReferenceDecoration(ref));
            }
        } else {
            if (semanticScope.getCondition(userFunctionRefNode, Write.class)) {
                throw userFunctionRefNode.createError(new IllegalArgumentException(
                        "invalid assignment: cannot assign a value to capturing function reference [" + symbol + ":"  + methodName + "]"));
            }

            if (read == false) {
                throw userFunctionRefNode.createError(new IllegalArgumentException(
                        "not a statement: capturing function reference [" + symbol + ":"  + methodName + "] not used"));
            }

            SemanticScope.Variable captured = semanticScope.getVariable(location, symbol);
            semanticScope.putDecoration(userFunctionRefNode, new CapturesDecoration(Collections.singletonList(captured)));
            if (targetType == null) {
                String defReferenceEncoding;
                if (captured.getType() == def.class) {
                    // dynamic implementation
                    defReferenceEncoding = "D" + symbol + "." + methodName + ",1";
                } else {
                    // typed implementation
                    defReferenceEncoding = "S" + captured.getCanonicalTypeName() + "." + methodName + ",1";
                }
                valueType = String.class;
                semanticScope.putDecoration(userFunctionRefNode, new EncodingDecoration(defReferenceEncoding));
            } else {
                valueType = targetType.getTargetType();
                // static case
                if (captured.getType() != def.class) {
                    FunctionRef ref = FunctionRef.create(scriptScope.getPainlessLookup(), scriptScope.getFunctionTable(), location,
                            targetType.getTargetType(), captured.getCanonicalTypeName(), methodName, 1);
                    semanticScope.putDecoration(userFunctionRefNode, new ReferenceDecoration(ref));
                }
            }
        }

        semanticScope.putDecoration(userFunctionRefNode, new ValueType(valueType));
    }

    /**
     * Visits a new array function ref expression which covers only a new array function reference
     * and generates an internal method to define the new array.
     * Checks: type validation
     */
    @Override
    public void visitNewArrayFunctionRef(ENewArrayFunctionRef userNewArrayFunctionRefNode, SemanticScope semanticScope) {
        String canonicalTypeName = userNewArrayFunctionRefNode.getCanonicalTypeName();

        if (semanticScope.getCondition(userNewArrayFunctionRefNode, Write.class)) {
            throw userNewArrayFunctionRefNode.createError(new IllegalArgumentException(
                    "cannot assign a value to new array function reference with target type [ + " + canonicalTypeName  + "]"));
        }

        if (semanticScope.getCondition(userNewArrayFunctionRefNode, Read.class) == false) {
            throw userNewArrayFunctionRefNode.createError(new IllegalArgumentException(
                    "not a statement: new array function reference with target type [" + canonicalTypeName + "] not used"));
        }

        ScriptScope scriptScope = semanticScope.getScriptScope();
        TargetType targetType = semanticScope.getDecoration(userNewArrayFunctionRefNode, TargetType.class);

        Class<?> valueType;
        Class<?> clazz = scriptScope.getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);
        semanticScope.putDecoration(userNewArrayFunctionRefNode, new ReturnType(clazz));

        if (clazz == null) {
            throw userNewArrayFunctionRefNode.createError(new IllegalArgumentException("Not a type [" + canonicalTypeName + "]."));
        }

        String name = scriptScope.getNextSyntheticName("newarray");
        scriptScope.getFunctionTable().addFunction(name, clazz, Collections.singletonList(int.class), true, true);
        semanticScope.putDecoration(userNewArrayFunctionRefNode, new MethodNameDecoration(name));

        if (targetType == null) {
            String defReferenceEncoding = "Sthis." + name + ",0";
            valueType = String.class;
            scriptScope.putDecoration(userNewArrayFunctionRefNode, new EncodingDecoration(defReferenceEncoding));
        } else {
            FunctionRef ref = FunctionRef.create(scriptScope.getPainlessLookup(), scriptScope.getFunctionTable(),
                    userNewArrayFunctionRefNode.getLocation(), targetType.getTargetType(), "this", name, 0);
            valueType = targetType.getTargetType();
            semanticScope.putDecoration(userNewArrayFunctionRefNode, new ReferenceDecoration(ref));
        }

        semanticScope.putDecoration(userNewArrayFunctionRefNode, new ValueType(valueType));
    }

    /**
     * Visits a symbol expression which covers static types, partial canonical types,
     * and variables.
     * Checks: type checking, type resolution, variable resolution
     */
    @Override
    public void visitSymbol(ESymbol userSymbolNode, SemanticScope semanticScope) {
        boolean read = semanticScope.getCondition(userSymbolNode, Read.class);
        boolean write = semanticScope.getCondition(userSymbolNode, Write.class);
        String symbol = userSymbolNode.getSymbol();
        Class<?> staticType = semanticScope.getScriptScope().getPainlessLookup().canonicalTypeNameToType(symbol);

        if (staticType != null)  {
            if (write) {
                throw userSymbolNode.createError(new IllegalArgumentException("invalid assignment: " +
                        "cannot write a value to a static type [" + PainlessLookupUtility.typeToCanonicalTypeName(staticType) + "]"));
            }

            if (read == false) {
                throw userSymbolNode.createError(new IllegalArgumentException("not a statement: " +
                        "static type [" + PainlessLookupUtility.typeToCanonicalTypeName(staticType) + "] not used"));
            }

            semanticScope.putDecoration(userSymbolNode, new StaticType(staticType));
        } else if (semanticScope.isVariableDefined(symbol)) {
            if (read == false && write == false) {
                throw userSymbolNode.createError(new IllegalArgumentException("not a statement: variable [" + symbol + "] not used"));
            }

            Location location = userSymbolNode.getLocation();
            Variable variable = semanticScope.getVariable(location, symbol);

            if (write && variable.isFinal()) {
                throw userSymbolNode.createError(new IllegalArgumentException("Variable [" + variable.getName() + "] is read-only."));
            }

            Class<?> valueType = variable.getType();
            semanticScope.putDecoration(userSymbolNode, new ValueType(valueType));
        } else {
            semanticScope.putDecoration(userSymbolNode, new PartialCanonicalTypeName(symbol));
        }
    }

    /**
     * Visits a dot expression which is a field index with a qualifier (prefix) and
     * may resolve to a static type, a partial canonical type, a field, a shortcut to a
     * getter/setter method on a type, or a getter/setter for a Map or List.
     * Checks: type validation, method resolution, field resolution
     */
    @Override
    public void visitDot(EDot userDotNode, SemanticScope semanticScope) {
        boolean read = semanticScope.getCondition(userDotNode, Read.class);
        boolean write = semanticScope.getCondition(userDotNode, Write.class);

        if (read == false && write == false) {
            throw userDotNode.createError(new IllegalArgumentException("not a statement: result of dot operator [.] not used"));
        }

        ScriptScope scriptScope = semanticScope.getScriptScope();
        String index = userDotNode.getIndex();

        AExpression userPrefixNode = userDotNode.getPrefixNode();
        semanticScope.setCondition(userPrefixNode, Read.class);
        visit(userPrefixNode, semanticScope);
        ValueType prefixValueType = semanticScope.getDecoration(userPrefixNode, ValueType.class);
        StaticType prefixStaticType = semanticScope.getDecoration(userPrefixNode, StaticType.class);

        if (prefixValueType != null && prefixStaticType != null) {
            throw userDotNode.createError(new IllegalStateException("cannot have both " +
                    "value [" + prefixValueType.getValueCanonicalTypeName() + "] " +
                    "and type [" + prefixStaticType.getStaticCanonicalTypeName() + "]"));
        }

        if (semanticScope.hasDecoration(userPrefixNode, PartialCanonicalTypeName.class)) {
            if (prefixValueType != null) {
                throw userDotNode.createError(new IllegalArgumentException("value required: instead found unexpected type " +
                        "[" + prefixValueType.getValueCanonicalTypeName() + "]"));
            }

            if (prefixStaticType != null) {
                throw userDotNode.createError(new IllegalArgumentException("value required: instead found unexpected type " +
                        "[" + prefixStaticType.getStaticType() + "]"));
            }

            String canonicalTypeName =
                    semanticScope.getDecoration(userPrefixNode, PartialCanonicalTypeName.class).getPartialCanonicalTypeName() + "." + index;
            Class<?> type = scriptScope.getPainlessLookup().canonicalTypeNameToType(canonicalTypeName);

            if (type == null) {
                semanticScope.putDecoration(userDotNode, new PartialCanonicalTypeName(canonicalTypeName));
            } else {
                if (write) {
                    throw userDotNode.createError(new IllegalArgumentException("invalid assignment: " +
                            "cannot write a value to a static type [" + PainlessLookupUtility.typeToCanonicalTypeName(type) + "]"));
                }

                semanticScope.putDecoration(userDotNode, new StaticType(type));
            }
        } else {
            Class<?> valueType = null;

            if (prefixValueType != null && prefixValueType.getValueType().isArray()) {
                if ("length".equals(index)) {
                    if (write) {
                        throw userDotNode.createError(new IllegalArgumentException(
                                "invalid assignment: cannot assign a value write to read-only field [length] for an array."));
                    }

                    valueType = int.class;
                } else {
                    throw userDotNode.createError(new IllegalArgumentException(
                            "Field [" + index + "] does not exist for type [" + prefixValueType.getValueCanonicalTypeName() + "]."));
                }
            } else if (prefixValueType != null && prefixValueType.getValueType() == def.class) {
                TargetType targetType = semanticScope.getDecoration(userDotNode, TargetType.class);
                // TODO: remove ZonedDateTime exception when JodaCompatibleDateTime is removed
                valueType = targetType == null || targetType.getTargetType() == ZonedDateTime.class ||
                        semanticScope.getCondition(userDotNode, Explicit.class) ? def.class : targetType.getTargetType();
                semanticScope.setCondition(userDotNode, DefOptimized.class);
            } else {
                Class<?> prefixType;
                String prefixCanonicalTypeName;
                boolean isStatic;

                if (prefixValueType != null) {
                    prefixType = prefixValueType.getValueType();
                    prefixCanonicalTypeName = prefixValueType.getValueCanonicalTypeName();
                    isStatic = false;
                } else if (prefixStaticType != null) {
                    prefixType = prefixStaticType.getStaticType();
                    prefixCanonicalTypeName = prefixStaticType.getStaticCanonicalTypeName();
                    isStatic = true;
                } else {
                    throw userDotNode.createError(new IllegalStateException("value required: instead found no value"));
                }

                PainlessField field = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessField(prefixType, isStatic, index);

                if (field == null) {
                    PainlessMethod getter;
                    PainlessMethod setter;

                    getter = scriptScope.getPainlessLookup().lookupPainlessMethod(prefixType, isStatic,
                            "get" + Character.toUpperCase(index.charAt(0)) + index.substring(1), 0);

                    if (getter == null) {
                        getter = scriptScope.getPainlessLookup().lookupPainlessMethod(prefixType, isStatic,
                                "is" + Character.toUpperCase(index.charAt(0)) + index.substring(1), 0);
                    }

                    setter = scriptScope.getPainlessLookup().lookupPainlessMethod(prefixType, isStatic,
                            "set" + Character.toUpperCase(index.charAt(0)) + index.substring(1), 0);

                    if (getter != null || setter != null) {
                        if (getter != null && (getter.returnType == void.class || !getter.typeParameters.isEmpty())) {
                            throw userDotNode.createError(new IllegalArgumentException(
                                    "Illegal get shortcut on field [" + index + "] for type [" + prefixCanonicalTypeName + "]."));
                        }

                        if (setter != null && (setter.returnType != void.class || setter.typeParameters.size() != 1)) {
                            throw userDotNode.createError(new IllegalArgumentException(
                                    "Illegal set shortcut on field [" + index + "] for type [" + prefixCanonicalTypeName + "]."));
                        }

                        if (getter != null && setter != null && setter.typeParameters.get(0) != getter.returnType) {
                            throw userDotNode.createError(new IllegalArgumentException("Shortcut argument types must match."));
                        }

                        if ((read == false || getter != null) && (write == false || setter != null)) {
                            valueType = setter != null ? setter.typeParameters.get(0) : getter.returnType;
                        } else {
                            throw userDotNode.createError(new IllegalArgumentException(
                                    "Illegal shortcut on field [" + index + "] for type [" + prefixCanonicalTypeName + "]."));
                        }

                        if (getter != null) {
                            semanticScope.putDecoration(userDotNode, new GetterPainlessMethod(getter));
                        }

                        if (setter != null) {
                            semanticScope.putDecoration(userDotNode, new SetterPainlessMethod(setter));
                        }

                        semanticScope.setCondition(userDotNode, Shortcut.class);
                    } else if (isStatic == false) {
                        if (Map.class.isAssignableFrom(prefixValueType.getValueType())) {
                            getter = scriptScope.getPainlessLookup().lookupPainlessMethod(prefixType, false, "get", 1);
                            setter = scriptScope.getPainlessLookup().lookupPainlessMethod(prefixType, false, "put", 2);

                            if (getter != null && (getter.returnType == void.class || getter.typeParameters.size() != 1)) {
                                throw userDotNode.createError(new IllegalArgumentException(
                                        "Illegal map get shortcut for type [" + prefixCanonicalTypeName + "]."));
                            }

                            if (setter != null && setter.typeParameters.size() != 2) {
                                throw userDotNode.createError(new IllegalArgumentException(
                                        "Illegal map set shortcut for type [" + prefixCanonicalTypeName + "]."));
                            }

                            if (getter != null && setter != null && (!getter.typeParameters.get(0).equals(setter.typeParameters.get(0)) ||
                                    getter.returnType.equals(setter.typeParameters.get(1)) == false)) {
                                throw userDotNode.createError(new IllegalArgumentException("Shortcut argument types must match."));
                            }

                            if ((read == false || getter != null) && (write == false || setter != null)) {
                                valueType = setter != null ? setter.typeParameters.get(1) : getter.returnType;
                            } else {
                                throw userDotNode.createError(new IllegalArgumentException(
                                        "Illegal map shortcut for type [" + prefixCanonicalTypeName + "]."));
                            }

                            if (getter != null) {
                                semanticScope.putDecoration(userDotNode, new GetterPainlessMethod(getter));
                            }

                            if (setter != null) {
                                semanticScope.putDecoration(userDotNode, new SetterPainlessMethod(setter));
                            }

                            semanticScope.setCondition(userDotNode, MapShortcut.class);
                        }

                        if (List.class.isAssignableFrom(prefixType)) {
                            try {
                                scriptScope.putDecoration(userDotNode, new StandardConstant(Integer.parseInt(index)));
                            } catch (NumberFormatException nfe) {
                                throw userDotNode.createError(new IllegalArgumentException("invalid list index [" + index + "]", nfe));
                            }

                            getter = scriptScope.getPainlessLookup().lookupPainlessMethod(prefixType, false, "get", 1);
                            setter = scriptScope.getPainlessLookup().lookupPainlessMethod(prefixType, false, "set", 2);

                            if (getter != null && (getter.returnType == void.class || getter.typeParameters.size() != 1 ||
                                    getter.typeParameters.get(0) != int.class)) {
                                throw userDotNode.createError(new IllegalArgumentException(
                                        "Illegal list get shortcut for type [" + prefixCanonicalTypeName + "]."));
                            }

                            if (setter != null && (setter.typeParameters.size() != 2 || setter.typeParameters.get(0) != int.class)) {
                                throw userDotNode.createError(new IllegalArgumentException(
                                        "Illegal list set shortcut for type [" + prefixCanonicalTypeName + "]."));
                            }

                            if (getter != null && setter != null && (!getter.typeParameters.get(0).equals(setter.typeParameters.get(0))
                                    || !getter.returnType.equals(setter.typeParameters.get(1)))) {
                                throw userDotNode.createError(new IllegalArgumentException("Shortcut argument types must match."));
                            }

                            if ((read == false || getter != null) && (write == false || setter != null)) {
                                valueType = setter != null ? setter.typeParameters.get(1) : getter.returnType;
                            } else {
                                throw userDotNode.createError(new IllegalArgumentException(
                                        "Illegal list shortcut for type [" + prefixCanonicalTypeName + "]."));
                            }

                            if (getter != null) {
                                semanticScope.putDecoration(userDotNode, new GetterPainlessMethod(getter));
                            }

                            if (setter != null) {
                                semanticScope.putDecoration(userDotNode, new SetterPainlessMethod(setter));
                            }

                            semanticScope.setCondition(userDotNode, ListShortcut.class);
                        }
                    }

                    if (valueType == null) {
                        if (prefixValueType != null) {
                            throw userDotNode.createError(new IllegalArgumentException(
                                    "field [" + prefixValueType.getValueCanonicalTypeName() + ", " + index + "] not found"));
                        } else {
                            throw userDotNode.createError(new IllegalArgumentException(
                                    "field [" + prefixStaticType.getStaticCanonicalTypeName() + ", " + index + "] not found"));
                        }
                    }
                } else {
                    if (write && Modifier.isFinal(field.javaField.getModifiers())) {
                        throw userDotNode.createError(new IllegalArgumentException(
                                "invalid assignment: cannot assign a value to read-only field [" + field.javaField.getName() + "]"));
                    }

                    semanticScope.putDecoration(userDotNode, new StandardPainlessField(field));
                    valueType = field.typeParameter;
                }
            }

            semanticScope.putDecoration(userDotNode, new ValueType(valueType));

            if (userDotNode.isNullSafe()) {
                if (write) {
                    throw userDotNode.createError(new IllegalArgumentException(
                            "invalid assignment: cannot assign a value to a null safe operation [?.]"));
                }

                if (valueType.isPrimitive()) {
                    throw new IllegalArgumentException("Result of null safe operator must be nullable");
                }
            }
        }
    }

    /**
     * Visits a brace expression which is an array index with a qualifier (prefix) and
     * may resolve to an array index, or a getter/setter for a Map or List.
     * Checks: type validation, method resolution, field resolution
     */
    @Override
    public void visitBrace(EBrace userBraceNode, SemanticScope semanticScope) {
        boolean read = semanticScope.getCondition(userBraceNode, Read.class);
        boolean write = semanticScope.getCondition(userBraceNode, Write.class);

        if (read == false && write == false) {
            throw userBraceNode.createError(new IllegalArgumentException("not a statement: result of brace operator not used"));
        }

        AExpression userPrefixNode = userBraceNode.getPrefixNode();
        semanticScope.setCondition(userPrefixNode, Read.class);
        checkedVisit(userPrefixNode, semanticScope);
        Class<?> prefixValueType = semanticScope.getDecoration(userPrefixNode, ValueType.class).getValueType();

        AExpression userIndexNode = userBraceNode.getIndexNode();
        Class<?> valueType;

        if (prefixValueType.isArray()) {
            semanticScope.setCondition(userIndexNode, Read.class);
            semanticScope.putDecoration(userIndexNode, new TargetType(int.class));
            checkedVisit(userIndexNode, semanticScope);
            decorateWithCast(userIndexNode, semanticScope);
            valueType = prefixValueType.getComponentType();
        } else if (prefixValueType == def.class) {
            semanticScope.setCondition(userIndexNode, Read.class);
            checkedVisit(userIndexNode, semanticScope);
            TargetType targetType = semanticScope.getDecoration(userBraceNode, TargetType.class);
            // TODO: remove ZonedDateTime exception when JodaCompatibleDateTime is removed
            valueType = targetType == null || targetType.getTargetType() == ZonedDateTime.class ||
                    semanticScope.getCondition(userBraceNode, Explicit.class) ? def.class : targetType.getTargetType();
            semanticScope.setCondition(userBraceNode, DefOptimized.class);
        } else if (Map.class.isAssignableFrom(prefixValueType)) {
            String canonicalClassName = PainlessLookupUtility.typeToCanonicalTypeName(prefixValueType);

            PainlessMethod getter =
                    semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(prefixValueType, false, "get", 1);
            PainlessMethod setter =
                    semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(prefixValueType, false, "put", 2);

            if (getter != null && (getter.returnType == void.class || getter.typeParameters.size() != 1)) {
                throw userBraceNode.createError(new IllegalArgumentException(
                        "Illegal map get shortcut for type [" + canonicalClassName + "]."));
            }

            if (setter != null && setter.typeParameters.size() != 2) {
                throw userBraceNode.createError(new IllegalArgumentException(
                        "Illegal map set shortcut for type [" + canonicalClassName + "]."));
            }

            if (getter != null && setter != null && (!getter.typeParameters.get(0).equals(setter.typeParameters.get(0)) ||
                    getter.returnType.equals(setter.typeParameters.get(1)) == false)) {
                throw userBraceNode.createError(new IllegalArgumentException("Shortcut argument types must match."));
            }

            if ((read == false || getter != null) && (write == false || setter != null)) {
                semanticScope.setCondition(userIndexNode, Read.class);
                semanticScope.putDecoration(userIndexNode,
                        new TargetType(setter != null ? setter.typeParameters.get(0) : getter.typeParameters.get(0)));
                checkedVisit(userIndexNode, semanticScope);
                decorateWithCast(userIndexNode, semanticScope);

                valueType = setter != null ? setter.typeParameters.get(1) : getter.returnType;

                if (getter != null) {
                    semanticScope.putDecoration(userBraceNode, new GetterPainlessMethod(getter));
                }

                if (setter != null) {
                    semanticScope.putDecoration(userBraceNode, new SetterPainlessMethod(setter));
                }
            } else {
                throw userBraceNode.createError(new IllegalArgumentException(
                        "Illegal map shortcut for type [" + canonicalClassName + "]."));
            }

            semanticScope.setCondition(userBraceNode, MapShortcut.class);
        } else if (List.class.isAssignableFrom(prefixValueType)) {
            String canonicalClassName = PainlessLookupUtility.typeToCanonicalTypeName(prefixValueType);

            PainlessMethod getter =
                    semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(prefixValueType, false, "get", 1);
            PainlessMethod setter =
                    semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(prefixValueType, false, "set", 2);

            if (getter != null && (getter.returnType == void.class || getter.typeParameters.size() != 1 ||
                    getter.typeParameters.get(0) != int.class)) {
                throw userBraceNode.createError(new IllegalArgumentException(
                        "Illegal list get shortcut for type [" + canonicalClassName + "]."));
            }

            if (setter != null && (setter.typeParameters.size() != 2 || setter.typeParameters.get(0) != int.class)) {
                throw userBraceNode.createError(new IllegalArgumentException(
                        "Illegal list set shortcut for type [" + canonicalClassName + "]."));
            }

            if (getter != null && setter != null && (!getter.typeParameters.get(0).equals(setter.typeParameters.get(0))
                    || !getter.returnType.equals(setter.typeParameters.get(1)))) {
                throw userBraceNode.createError(new IllegalArgumentException("Shortcut argument types must match."));
            }

            if ((read == false || getter != null) && (write == false || setter != null)) {
                semanticScope.setCondition(userIndexNode, Read.class);
                semanticScope.putDecoration(userIndexNode, new TargetType(int.class));
                checkedVisit(userIndexNode, semanticScope);
                decorateWithCast(userIndexNode, semanticScope);

                valueType = setter != null ? setter.typeParameters.get(1) : getter.returnType;

                if (getter != null) {
                    semanticScope.putDecoration(userBraceNode, new GetterPainlessMethod(getter));
                }

                if (setter != null) {
                    semanticScope.putDecoration(userBraceNode, new SetterPainlessMethod(setter));
                }
            } else {
                throw userBraceNode.createError(new IllegalArgumentException(
                        "Illegal list shortcut for type [" + canonicalClassName + "]."));
            }

            semanticScope.setCondition(userBraceNode, ListShortcut.class);
        } else {
            throw userBraceNode.createError(new IllegalArgumentException("Illegal array access on type " +
                    "[" + PainlessLookupUtility.typeToCanonicalTypeName(prefixValueType) + "]."));
        }

        semanticScope.putDecoration(userBraceNode, new ValueType(valueType));
    }

    /**
     * Visits a call expression which is a method call with a qualifier (prefix).
     * Checks: type validation, method resolution
     */
    @Override
    public void visitCall(ECall userCallNode, SemanticScope semanticScope) {
        String methodName = userCallNode.getMethodName();
        List<AExpression> userArgumentNodes = userCallNode.getArgumentNodes();
        int userArgumentsSize = userArgumentNodes.size();

        if (semanticScope.getCondition(userCallNode, Write.class)) {
            throw userCallNode.createError(new IllegalArgumentException(
                    "invalid assignment: cannot assign a value to method call [" + methodName + "/" + userArgumentsSize + "]"));
        }

        AExpression userPrefixNode = userCallNode.getPrefixNode();
        semanticScope.setCondition(userPrefixNode, Read.class);
        visit(userPrefixNode, semanticScope);
        ValueType prefixValueType = semanticScope.getDecoration(userPrefixNode, ValueType.class);
        StaticType prefixStaticType = semanticScope.getDecoration(userPrefixNode, StaticType.class);

        if (prefixValueType != null && prefixStaticType != null) {
            throw userCallNode.createError(new IllegalStateException("cannot have both " +
                    "value [" + prefixValueType.getValueCanonicalTypeName() + "] " +
                    "and type [" + prefixStaticType.getStaticCanonicalTypeName() + "]"));
        }

        if (semanticScope.hasDecoration(userPrefixNode, PartialCanonicalTypeName.class)) {
            throw userCallNode.createError(new IllegalArgumentException("cannot resolve symbol " +
                    "[" + semanticScope.getDecoration(userPrefixNode, PartialCanonicalTypeName.class).getPartialCanonicalTypeName() + "]"));
        }

        Class<?> valueType;

        if (prefixValueType != null && prefixValueType.getValueType() == def.class) {
            for (AExpression userArgumentNode : userArgumentNodes) {
                semanticScope.setCondition(userArgumentNode, Read.class);
                semanticScope.setCondition(userArgumentNode, Internal.class);
                checkedVisit(userArgumentNode, semanticScope);
                Class<?> argumentValueType = semanticScope.getDecoration(userArgumentNode, ValueType.class).getValueType();

                if (argumentValueType == void.class) {
                    throw userCallNode.createError(new IllegalArgumentException(
                            "Argument(s) cannot be of [void] type when calling method [" + methodName + "]."));
                }
            }

            TargetType targetType = semanticScope.getDecoration(userCallNode, TargetType.class);
            // TODO: remove ZonedDateTime exception when JodaCompatibleDateTime is removed
            valueType = targetType == null || targetType.getTargetType() == ZonedDateTime.class ||
                    semanticScope.getCondition(userCallNode, Explicit.class) ? def.class : targetType.getTargetType();
        } else {
            PainlessMethod method;

            if (prefixValueType != null) {
                method = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(
                        prefixValueType.getValueType(), false, methodName, userArgumentsSize);

                if (method == null) {
                    throw userCallNode.createError(new IllegalArgumentException("member method " +
                            "[" + prefixValueType.getValueCanonicalTypeName() + ", " + methodName + "/" + userArgumentsSize + "] " +
                            "not found"));
                }
            } else if (prefixStaticType != null) {
                method = semanticScope.getScriptScope().getPainlessLookup().lookupPainlessMethod(
                        prefixStaticType.getStaticType(), true, methodName, userArgumentsSize);

                if (method == null) {
                    throw userCallNode.createError(new IllegalArgumentException("static method " +
                            "[" + prefixStaticType.getStaticCanonicalTypeName() + ", " + methodName + "/" + userArgumentsSize + "] " +
                            "not found"));
                }
            } else {
                throw userCallNode.createError(new IllegalStateException("value required: instead found no value"));
            }

            semanticScope.getScriptScope().markNonDeterministic(method.annotations.containsKey(NonDeterministicAnnotation.class));

            for (int argument = 0; argument < userArgumentsSize; ++argument) {
                AExpression userArgumentNode = userArgumentNodes.get(argument);

                semanticScope.setCondition(userArgumentNode, Read.class);
                semanticScope.putDecoration(userArgumentNode, new TargetType(method.typeParameters.get(argument)));
                semanticScope.setCondition(userArgumentNode, Internal.class);
                checkedVisit(userArgumentNode, semanticScope);
                decorateWithCast(userArgumentNode, semanticScope);
            }

            semanticScope.putDecoration(userCallNode, new StandardPainlessMethod(method));
            valueType = method.returnType;
        }

        if (userCallNode.isNullSafe() && valueType.isPrimitive()) {
            throw new IllegalArgumentException("Result of null safe operator must be nullable");
        }

        semanticScope.putDecoration(userCallNode, new ValueType(valueType));
    }
}
