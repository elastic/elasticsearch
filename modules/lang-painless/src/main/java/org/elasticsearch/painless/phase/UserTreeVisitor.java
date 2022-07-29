/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.phase;

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

public interface UserTreeVisitor<Scope> {

    void visitClass(SClass userClassNode, Scope scope);

    void visitFunction(SFunction userFunctionNode, Scope scope);

    void visitBlock(SBlock userBlockNode, Scope scope);

    void visitIf(SIf userIfNode, Scope scope);

    void visitIfElse(SIfElse userIfElseNode, Scope scope);

    void visitWhile(SWhile userWhileNode, Scope scope);

    void visitDo(SDo userDoNode, Scope scope);

    void visitFor(SFor userForNode, Scope scope);

    void visitEach(SEach userEachNode, Scope scope);

    void visitDeclBlock(SDeclBlock userDeclBlockNode, Scope scope);

    void visitDeclaration(SDeclaration userDeclarationNode, Scope scope);

    void visitReturn(SReturn userReturnNode, Scope scope);

    void visitExpression(SExpression userExpressionNode, Scope scope);

    void visitTry(STry userTryNode, Scope scope);

    void visitCatch(SCatch userCatchNode, Scope scope);

    void visitThrow(SThrow userThrowNode, Scope scope);

    void visitContinue(SContinue userContinueNode, Scope scope);

    void visitBreak(SBreak userBreakNode, Scope scope);

    void visitAssignment(EAssignment userAssignmentNode, Scope scope);

    void visitUnary(EUnary userUnaryNode, Scope scope);

    void visitBinary(EBinary userBinaryNode, Scope scope);

    void visitBooleanComp(EBooleanComp userBooleanCompNode, Scope scope);

    void visitComp(EComp userCompNode, Scope scope);

    void visitExplicit(EExplicit userExplicitNode, Scope scope);

    void visitInstanceof(EInstanceof userInstanceofNode, Scope scope);

    void visitConditional(EConditional userConditionalNode, Scope scope);

    void visitElvis(EElvis userElvisNode, Scope scope);

    void visitListInit(EListInit userListInitNode, Scope scope);

    void visitMapInit(EMapInit userMapInitNode, Scope scope);

    void visitNewArray(ENewArray userNewArrayNode, Scope scope);

    void visitNewObj(ENewObj userNewObjectNode, Scope scope);

    void visitCallLocal(ECallLocal userCallLocalNode, Scope scope);

    void visitBooleanConstant(EBooleanConstant userBooleanConstantNode, Scope scope);

    void visitNumeric(ENumeric userNumericNode, Scope scope);

    void visitDecimal(EDecimal userDecimalNode, Scope scope);

    void visitString(EString userStringNode, Scope scope);

    void visitNull(ENull userNullNode, Scope scope);

    void visitRegex(ERegex userRegexNode, Scope scope);

    void visitLambda(ELambda userLambdaNode, Scope scope);

    void visitFunctionRef(EFunctionRef userFunctionRefNode, Scope scope);

    void visitNewArrayFunctionRef(ENewArrayFunctionRef userNewArrayFunctionRefNode, Scope scope);

    void visitSymbol(ESymbol userSymbolNode, Scope scope);

    void visitDot(EDot userDotNode, Scope scope);

    void visitBrace(EBrace userBraceNode, Scope scope);

    void visitCall(ECall userCallNode, Scope scope);
}
