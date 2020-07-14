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

public class UserTreeBaseVisitor<Input, Output> implements UserTreeVisitor<Input, Output> {

    @Override
    public Output visitClass(SClass userClassNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitFunction(SFunction userFunctionNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitBlock(SBlock userBlockNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitIf(SIf userIfNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitIfElse(SIfElse userIfElseNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitWhile(SWhile userWhileNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitDo(SDo userDoNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitFor(SFor userForNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitEach(SEach userEachNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitDeclBlock(SDeclBlock userDeclBlockNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitDeclaration(SDeclaration userDeclarationNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitReturn(SReturn userReturnNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitExpression(SExpression userExpressionNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitTry(STry userTryNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitCatch(SCatch userCatchNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitThrow(SThrow userThrowNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitContinue(SContinue userContinueNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitBreak(SBreak userBreakNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitAssignment(EAssignment userAssignmentNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitUnary(EUnary userUnaryNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitBinary(EBinary userBinaryNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitBool(EBooleanComp userBoolNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitComp(EComp userCompNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitExplicit(EExplicit userExplicitNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitInstanceof(EInstanceof userInstanceofNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitConditional(EConditional userConditionalNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitElvis(EElvis userElvisNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitListInit(EListInit userListInitNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitMapInit(EMapInit userMapInitNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitNewArray(ENewArray userNewArrayNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitNewObj(ENewObj userNewObjNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitCallLocal(ECallLocal userCallLocalNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitBoolean(EBooleanConstant userBooleanNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitNumeric(ENumeric userNumericNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitDecimal(EDecimal userDecimalNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitString(EString userStringNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitNull(ENull userNullNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitRegex(ERegex userRegexNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitLambda(ELambda userLambdaNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitFunctionRef(EFunctionRef userFunctionRefNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitNewArrayFunctionRef(ENewArrayFunctionRef userNewArrayFunctionRefNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitSymbol(ESymbol userSymbolNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitDot(EDot userDotNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitBrace(EBrace userBraceNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitCall(ECall userCallNode, Input input) {
        throw new UnsupportedOperationException();
    }
}
