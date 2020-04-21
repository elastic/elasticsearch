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
import org.elasticsearch.painless.node.EBool;
import org.elasticsearch.painless.node.EBoolean;
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
import org.elasticsearch.painless.symbol.ScriptScope;

public class SemanticAnalysisPhase implements UserTreeVisitor<ScriptScope, Void> {

    @Override
    public Void visitClass(SClass userClassNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitFunction(SFunction userFunctionNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitBlock(SBlock userBlockNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitIf(SIf userIfNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitIfElse(SIfElse userIfElseNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitWhile(SWhile userWhileNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitDo(SDo userDoNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitFor(SFor userForNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitEach(SEach userEachNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitDeclBlock(SDeclBlock userDeclBlockNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitDeclaration(SDeclaration userDeclarationNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitReturn(SReturn userReturnNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitExpression(SExpression userExpressionNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitTry(STry userTryNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitCatch(SCatch userCatchNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitThrow(SThrow userThrowNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitContinue(SContinue userContinueNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitBreak(SBreak userBreakNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitAssignment(EAssignment userAssignmentNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitUnary(EUnary userUnaryNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitBinary(EBinary userBinaryNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitBool(EBool userBoolNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitComp(EComp userCompNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitExplicit(EExplicit userExplicitNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitInstanceof(EInstanceof userInstanceofNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitConditional(EConditional userConditionalNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitElvis(EElvis userElvisNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitListInit(EListInit userListInitNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitMapInit(EMapInit userMapInitNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitNewArray(ENewArray userNewArrayNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitNewObj(ENewObj userNewObjectNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitCallLocal(ECallLocal userCallLocalNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitBoolean(EBoolean userBooleanNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitNumeric(ENumeric userNumericNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitDecimal(EDecimal userDecimalNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitString(EString userStringNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitNull(ENull userNullNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitRegex(ERegex userRegexNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitLambda(ELambda userLambdaNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitFunctionRef(EFunctionRef userFunctionRefNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitNewArrayFunctionRef(ENewArrayFunctionRef userNewArrayFunctionRefNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitSymbol(ESymbol userSymbolNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitDot(EDot userDotNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitBrace(EBrace userBraceNode, ScriptScope scriptScope) {
        return null;
    }

    @Override
    public Void visitCall(ECall userCallNode, ScriptScope scriptScope) {
        return null;
    }
}
