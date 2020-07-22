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
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.lookup.PainlessCast;
import org.elasticsearch.painless.node.AExpression;
import org.elasticsearch.painless.node.ANode;
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
import org.elasticsearch.painless.symbol.Decorations.Explicit;
import org.elasticsearch.painless.symbol.Decorations.ExpressionPainlessCast;
import org.elasticsearch.painless.symbol.Decorations.Internal;
import org.elasticsearch.painless.symbol.Decorations.PartialCanonicalTypeName;
import org.elasticsearch.painless.symbol.Decorations.StaticType;
import org.elasticsearch.painless.symbol.Decorations.TargetType;
import org.elasticsearch.painless.symbol.Decorations.ValueType;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.painless.symbol.SemanticScope;

public class DefaultSemanticAnalysisPhase extends UserTreeBaseVisitor<SemanticScope> {

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

    public void visit(ANode userNode, SemanticScope semanticScope) {
        if (userNode != null) {
            userNode.visit(this, semanticScope);
        }
    }

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

    public void visitClass(SClass userClassNode, ScriptScope scriptScope) {
        SClass.visitDefaultSemanticAnalysis(this, userClassNode, scriptScope);
    }

    public void visitFunction(SFunction userFunctionNode, ScriptScope scriptScope) {
        SFunction.visitDefaultSemanticAnalysis(this, userFunctionNode, scriptScope);
    }

    @Override
    public void visitBlock(SBlock userBlockNode, SemanticScope semanticScope) {
        SBlock.visitDefaultSemanticAnalysis(this, userBlockNode, semanticScope);


    }

    @Override
    public void visitIf(SIf userIfNode, SemanticScope semanticScope) {
        SIf.visitDefaultSemanticAnalysis(this, userIfNode, semanticScope);


    }

    @Override
    public void visitIfElse(SIfElse userIfElseNode, SemanticScope semanticScope) {
        SIfElse.visitDefaultSemanticAnalysis(this, userIfElseNode, semanticScope);


    }

    @Override
    public void visitWhile(SWhile userWhileNode, SemanticScope semanticScope) {
        SWhile.visitDefaultSemanticAnalysis(this, userWhileNode, semanticScope);


    }

    @Override
    public void visitDo(SDo userDoNode, SemanticScope semanticScope) {
        SDo.visitDefaultSemanticAnalysis(this, userDoNode, semanticScope);


    }

    @Override
    public void visitFor(SFor userForNode, SemanticScope semanticScope) {
        SFor.visitDefaultSemanticAnalysis(this, userForNode, semanticScope);


    }

    @Override
    public void visitEach(SEach userEachNode, SemanticScope semanticScope) {
        SEach.visitDefaultSemanticAnalysis(this, userEachNode, semanticScope);


    }

    @Override
    public void visitDeclBlock(SDeclBlock userDeclBlockNode, SemanticScope semanticScope) {
        SDeclBlock.visitDefaultSemanticAnalysis(this, userDeclBlockNode, semanticScope);


    }

    @Override
    public void visitDeclaration(SDeclaration userDeclarationNode, SemanticScope semanticScope) {
        SDeclaration.visitDefaultSemanticAnalysis(this, userDeclarationNode, semanticScope);


    }

    @Override
    public void visitReturn(SReturn userReturnNode, SemanticScope semanticScope) {
        SReturn.visitDefaultSemanticAnalysis(this, userReturnNode, semanticScope);


    }

    @Override
    public void visitExpression(SExpression userExpressionNode, SemanticScope semanticScope) {
        SExpression.visitDefaultSemanticAnalysis(this, userExpressionNode, semanticScope);


    }

    @Override
    public void visitTry(STry userTryNode, SemanticScope semanticScope) {
        STry.visitDefaultSemanticAnalysis(this, userTryNode, semanticScope);


    }

    @Override
    public void visitCatch(SCatch userCatchNode, SemanticScope semanticScope) {
        SCatch.visitDefaultSemanticAnalysis(this, userCatchNode, semanticScope);


    }

    @Override
    public void visitThrow(SThrow userThrowNode, SemanticScope semanticScope) {
        SThrow.visitDefaultSemanticAnalysis(this, userThrowNode, semanticScope);


    }

    @Override
    public void visitContinue(SContinue userContinueNode, SemanticScope semanticScope) {
        SContinue.visitDefaultSemanticAnalysis(this, userContinueNode, semanticScope);


    }

    @Override
    public void visitBreak(SBreak userBreakNode, SemanticScope semanticScope) {
        SBreak.visitDefaultSemanticAnalysis(this, userBreakNode, semanticScope);


    }

    @Override
    public void visitAssignment(EAssignment userAssignmentNode, SemanticScope semanticScope) {
        EAssignment.visitDefaultSemanticAnalysis(this, userAssignmentNode, semanticScope);


    }

    @Override
    public void visitUnary(EUnary userUnaryNode, SemanticScope semanticScope) {
        EUnary.visitDefaultSemanticAnalysis(this, userUnaryNode, semanticScope);


    }

    @Override
    public void visitBinary(EBinary userBinaryNode, SemanticScope semanticScope) {
        EBinary.visitDefaultSemanticAnalysis(this, userBinaryNode, semanticScope);


    }

    @Override
    public void visitBooleanComp(EBooleanComp userBooleanCompNode, SemanticScope semanticScope) {
        EBooleanComp.visitDefaultSemanticAnalysis(this, userBooleanCompNode, semanticScope);


    }

    @Override
    public void visitComp(EComp userCompNode, SemanticScope semanticScope) {
        EComp.visitDefaultSemanticAnalysis(this, userCompNode, semanticScope);


    }

    @Override
    public void visitExplicit(EExplicit userExplicitNode, SemanticScope semanticScope) {
        EExplicit.visitDefaultSemanticAnalysis(this, userExplicitNode, semanticScope);


    }

    @Override
    public void visitInstanceof(EInstanceof userInstanceofNode, SemanticScope semanticScope) {
        EInstanceof.visitDefaultSemanticAnalysis(this, userInstanceofNode, semanticScope);


    }

    @Override
    public void visitConditional(EConditional userConditionalNode, SemanticScope semanticScope) {
        EConditional.visitDefaultSemanticAnalysis(this, userConditionalNode, semanticScope);


    }

    @Override
    public void visitElvis(EElvis userElvisNode, SemanticScope semanticScope) {
        EElvis.visitDefaultSemanticAnalysis(this, userElvisNode, semanticScope);


    }

    @Override
    public void visitListInit(EListInit userListInitNode, SemanticScope semanticScope) {
        EListInit.visitDefaultSemanticAnalysis(this, userListInitNode, semanticScope);


    }

    @Override
    public void visitMapInit(EMapInit userMapInitNode, SemanticScope semanticScope) {
        EMapInit.visitDefaultSemanticAnalysis(this, userMapInitNode, semanticScope);


    }

    @Override
    public void visitNewArray(ENewArray userNewArrayNode, SemanticScope semanticScope) {
        ENewArray.visitDefaultSemanticAnalysis(this, userNewArrayNode, semanticScope);


    }

    @Override
    public void visitNewObj(ENewObj userNewObjNode, SemanticScope semanticScope) {
        ENewObj.visitDefaultSemanticAnalysis(this, userNewObjNode, semanticScope);


    }

    @Override
    public void visitCallLocal(ECallLocal userCallLocalNode, SemanticScope semanticScope) {
        ECallLocal.visitDefaultSemanticAnalysis(this, userCallLocalNode, semanticScope);


    }

    @Override
    public void visitBooleanConstant(EBooleanConstant userBooleanConstantNode, SemanticScope semanticScope) {
        EBooleanConstant.visitDefaultSemanticAnalysis(this, userBooleanConstantNode, semanticScope);


    }

    @Override
    public void visitNumeric(ENumeric userNumericNode, SemanticScope semanticScope) {
        ENumeric.visitDefaultSemanticAnalysis(this, userNumericNode, semanticScope);


    }

    @Override
    public void visitDecimal(EDecimal userDecimalNode, SemanticScope semanticScope) {
        EDecimal.visitDefaultSemanticAnalysis(this, userDecimalNode, semanticScope);


    }

    @Override
    public void visitString(EString userStringNode, SemanticScope semanticScope) {
        EString.visitDefaultSemanticAnalysis(this, userStringNode, semanticScope);


    }

    @Override
    public void visitNull(ENull userNullNode, SemanticScope semanticScope) {
        ENull.visitDefaultSemanticAnalysis(this, userNullNode, semanticScope);


    }

    @Override
    public void visitRegex(ERegex userRegexNode, SemanticScope semanticScope) {
        ERegex.visitDefaultSemanticAnalysis(this, userRegexNode, semanticScope);


    }

    @Override
    public void visitLambda(ELambda userLambdaNode, SemanticScope semanticScope) {
        ELambda.visitDefaultSemanticAnalysis(this, userLambdaNode, semanticScope);


    }

    @Override
    public void visitFunctionRef(EFunctionRef userFunctionRefNode, SemanticScope semanticScope) {
        EFunctionRef.visitDefaultSemanticAnalysis(this, userFunctionRefNode, semanticScope);


    }

    @Override
    public void visitNewArrayFunctionRef(ENewArrayFunctionRef userNewArrayFunctionRefNode, SemanticScope semanticScope) {
        ENewArrayFunctionRef.visitDefaultSemanticAnalysis(this, userNewArrayFunctionRefNode, semanticScope);


    }

    @Override
    public void visitSymbol(ESymbol userSymbolNode, SemanticScope semanticScope) {
        ESymbol.visitDefaultSemanticAnalysis(this, userSymbolNode, semanticScope);


    }

    @Override
    public void visitDot(EDot userDotNode, SemanticScope semanticScope) {
        EDot.visitDefaultSemanticAnalysis(this, userDotNode, semanticScope);


    }

    @Override
    public void visitBrace(EBrace userBraceNode, SemanticScope semanticScope) {
        EBrace.visitDefaultSemanticAnalysis(this, userBraceNode, semanticScope);


    }

    @Override
    public void visitCall(ECall userCallNode, SemanticScope semanticScope) {
        ECall.visitDefaultSemanticAnalysis(this, userCallNode, semanticScope);


    }
}
