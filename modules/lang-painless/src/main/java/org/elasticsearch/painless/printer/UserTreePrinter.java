/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.printer;

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
import org.elasticsearch.painless.phase.UserTreeBaseVisitor;

public class UserTreePrinter extends UserTreeBaseVisitor<PrintScope> {
    private void start(String label, PrintScope printScope) {
        System.out.print(printScope.tabs() + "(");
        System.out.println(label);
        printScope.ntabs++;
    }

    private void end(PrintScope printScope) {
        printScope.ntabs--;
        System.out.println(printScope.tabs() + ")");
    }

    @Override
    public void visitClass(SClass userClassNode, PrintScope printScope) {
        start("class", printScope);
        super.visitClass(userClassNode, printScope);
        end(printScope);
    }

    @Override
    public void visitFunction(SFunction userClassNode, PrintScope printScope) {
        start("function", printScope);
        super.visitFunction(userClassNode, printScope);
        end(printScope);
    }

    @Override
    public void visitBlock(SBlock userBlockNode, PrintScope printScope) {
        start("block", printScope);
        super.visitBlock(userBlockNode, printScope);
        end(printScope);
    }

    @Override
    public void visitIf(SIf userIfNode, PrintScope printScope) {
        start("if", printScope);
        super.visitIf(userIfNode, printScope);
        end(printScope);
    }

    @Override
    public void visitIfElse(SIfElse userIfElseNode, PrintScope printScope) {
        start("ifElse", printScope);
        super.visitIfElse(userIfElseNode, printScope);
        end(printScope);
    }

    @Override
    public void visitWhile(SWhile userWhileNode, PrintScope printScope) {
        start("while", printScope);
        super.visitWhile(userWhileNode, printScope);
        end(printScope);
    }

    @Override
    public void visitDo(SDo userDoNode, PrintScope printScope) {
        start("do", printScope);
        super.visitDo(userDoNode, printScope);
        end(printScope);
    }

    @Override
    public void visitFor(SFor userForNode, PrintScope printScope) {
        start("for", printScope);
        super.visitFor(userForNode, printScope);
        end(printScope);
    }

    @Override
    public void visitEach(SEach userEachNode, PrintScope printScope) {
        start("each", printScope);
        super.visitEach(userEachNode, printScope);
        end(printScope);
    }

    @Override
    public void visitDeclBlock(SDeclBlock userDeclBlockNode, PrintScope printScope) {
        start("declBlock", printScope);
        super.visitDeclBlock(userDeclBlockNode, printScope);
        end(printScope);
    }

    @Override
    public void visitDeclaration(SDeclaration userDeclarationNode, PrintScope printScope) {
        start("declaration", printScope);
        super.visitDeclaration(userDeclarationNode, printScope);
        end(printScope);
    }

    @Override
    public void visitReturn(SReturn userReturnNode, PrintScope printScope) {
        start("return", printScope);
        super.visitReturn(userReturnNode, printScope);
        end(printScope);
    }

    @Override
    public void visitExpression(SExpression userExpressionNode, PrintScope printScope) {
        start("expression", printScope);
        super.visitExpression(userExpressionNode, printScope);
        end(printScope);
    }

    @Override
    public void visitTry(STry userTryNode, PrintScope printScope) {
        start("try", printScope);
        super.visitTry(userTryNode, printScope);
        end(printScope);
    }

    @Override
    public void visitCatch(SCatch userCatchNode, PrintScope printScope) {
        start("catch", printScope);
        super.visitCatch(userCatchNode, printScope);
        end(printScope);
    }

    @Override
    public void visitThrow(SThrow userThrowNode, PrintScope printScope) {
        start("throw", printScope);
        super.visitThrow(userThrowNode, printScope);
        end(printScope);
    }

    @Override
    public void visitContinue(SContinue userContinueNode, PrintScope printScope) {
        start("continue", printScope);
        super.visitContinue(userContinueNode, printScope);
        end(printScope);
    }

    @Override
    public void visitBreak(SBreak userBreakNode, PrintScope printScope) {
        start("break", printScope);
        super.visitBreak(userBreakNode, printScope);
        end(printScope);
    }

    @Override
    public void visitAssignment(EAssignment userAssignmentNode, PrintScope printScope) {
        start("assignment", printScope);
        super.visitAssignment(userAssignmentNode, printScope);
        end(printScope);
    }

    @Override
    public void visitUnary(EUnary userUnaryNode, PrintScope printScope) {
        start("unary", printScope);
        super.visitUnary(userUnaryNode, printScope);
        end(printScope);
    }

    @Override
    public void visitBinary(EBinary userBinaryNode, PrintScope printScope) {
        start("binary", printScope);
        super.visitBinary(userBinaryNode, printScope);
        end(printScope);
    }

    @Override
    public void visitBooleanComp(EBooleanComp userBooleanCompNode, PrintScope printScope) {
        start("booleanComp", printScope);
        super.visitBooleanComp(userBooleanCompNode, printScope);
        end(printScope);
    }

    @Override
    public void visitComp(EComp userCompNode, PrintScope printScope) {
        start("comp", printScope);
        super.visitComp(userCompNode, printScope);
        end(printScope);
    }

    @Override
    public void visitExplicit(EExplicit userExplicitNode, PrintScope printScope) {
        start("explicit", printScope);
        super.visitExplicit(userExplicitNode, printScope);
        end(printScope);
    }

    @Override
    public void visitInstanceof(EInstanceof userInstanceofNode, PrintScope printScope) {
        start("instanceof", printScope);
        super.visitInstanceof(userInstanceofNode, printScope);
        end(printScope);
    }

    @Override
    public void visitConditional(EConditional userConditionalNode, PrintScope printScope) {
        start("conditional", printScope);
        super.visitConditional(userConditionalNode, printScope);
        end(printScope);
    }

    @Override
    public void visitElvis(EElvis userElvisNode, PrintScope printScope) {
        start("elvis", printScope);
        super.visitElvis(userElvisNode, printScope);
        end(printScope);
    }

    @Override
    public void visitListInit(EListInit userListInitNode, PrintScope printScope) {
        start("listInit", printScope);
        super.visitListInit(userListInitNode, printScope);
        end(printScope);
    }

    @Override
    public void visitMapInit(EMapInit userMapInitNode, PrintScope printScope) {
        start("mapInit", printScope);
        super.visitMapInit(userMapInitNode, printScope);
        end(printScope);
    }

    @Override
    public void visitNewArray(ENewArray userNewArrayNode, PrintScope printScope) {
        start("newArray", printScope);
        super.visitNewArray(userNewArrayNode, printScope);
        end(printScope);
    }

    @Override
    public void visitNewObj(ENewObj userNewObjNode, PrintScope printScope) {
        start("newObj", printScope);
        super.visitNewObj(userNewObjNode, printScope);
        end(printScope);
    }

    @Override
    public void visitCallLocal(ECallLocal userCallLocalNode, PrintScope printScope) {
        start("callLocal", printScope);
        super.visitCallLocal(userCallLocalNode, printScope);
        end(printScope);
    }

    @Override
    public void visitBooleanConstant(EBooleanConstant userBooleanConstantNode, PrintScope printScope) {
        start("booleanConstant", printScope);
        super.visitBooleanConstant(userBooleanConstantNode, printScope);
        end(printScope);
    }

    @Override
    public void visitNumeric(ENumeric userNumericNode, PrintScope printScope) {
        start("numeric", printScope);
        super.visitNumeric(userNumericNode, printScope);
        end(printScope);
    }

    @Override
    public void visitDecimal(EDecimal userDecimalNode, PrintScope printScope) {
        start("decimal", printScope);
        super.visitDecimal(userDecimalNode, printScope);
        end(printScope);
    }

    @Override
    public void visitString(EString userStringNode, PrintScope printScope) {
        start("string", printScope);
        super.visitString(userStringNode, printScope);
        end(printScope);
    }

    @Override
    public void visitNull(ENull userNullNode, PrintScope printScope) {
        start("null", printScope);
        super.visitNull(userNullNode, printScope);
        end(printScope);
    }

    @Override
    public void visitRegex(ERegex userRegexNode, PrintScope printScope) {
        start("regex", printScope);
        super.visitRegex(userRegexNode, printScope);
        end(printScope);
    }

    @Override
    public void visitLambda(ELambda userLambdaNode, PrintScope printScope) {
        start("lambda", printScope);
        super.visitLambda(userLambdaNode, printScope);
        end(printScope);
    }

    @Override
    public void visitFunctionRef(EFunctionRef userFunctionRefNode, PrintScope printScope) {
        start("functionRef", printScope);
        super.visitFunctionRef(userFunctionRefNode, printScope);
        end(printScope);
    }

    @Override
    public void visitNewArrayFunctionRef(ENewArrayFunctionRef userNewArrayFunctionRefNode, PrintScope printScope) {
        start("newArrayFunctionRef", printScope);
        super.visitNewArrayFunctionRef(userNewArrayFunctionRefNode, printScope);
        end(printScope);
    }

    @Override
    public void visitSymbol(ESymbol userSymbolNode, PrintScope printScope) {
        start("symbol", printScope);
        super.visitSymbol(userSymbolNode, printScope);
        end(printScope);
    }

    @Override
    public void visitDot(EDot userDotNode, PrintScope printScope) {
        start("dot", printScope);
        super.visitDot(userDotNode, printScope);
        end(printScope);
    }

    @Override
    public void visitBrace(EBrace userBraceNode, PrintScope printScope) {
        start("brace", printScope);
        super.visitBrace(userBraceNode, printScope);
        end(printScope);
    }

    @Override
    public void visitCall(ECall userCallNode, PrintScope printScope) {
        start("call", printScope);
        super.visitCall(userCallNode, printScope);
        end(printScope);
    }
}
