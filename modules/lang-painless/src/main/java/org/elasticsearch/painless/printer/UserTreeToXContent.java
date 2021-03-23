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

public class UserTreeToXContent extends UserTreeBaseVisitor<UserTreePrinterScope> {
    static final class Fields {
        static final String NODE = "node";
        static final String LOCATION = "location";

    }

    @Override
    public void visitClass(SClass userClassNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "class");
        scope.field(Fields.LOCATION, userClassNode.getLocation().getOffset());
        scope.field("source", scope.scriptScope.getScriptSource());
        scope.startArray("functions");
        userClassNode.visitChildren(this, scope);
        scope.endArray();
        scope.endObject();
    }

    @Override
    public void visitFunction(SFunction userFunctionNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "function");
        scope.field(Fields.LOCATION, userFunctionNode.getLocation().getOffset());
        scope.field("name", userFunctionNode.getFunctionName());
        scope.field("returns", userFunctionNode.getReturnCanonicalTypeName());
        scope.field("parameters", userFunctionNode.getParameterNames());
        scope.field("parameterTypes", userFunctionNode.getCanonicalTypeNameParameters());
        scope.field("isInternal", userFunctionNode.isInternal());
        scope.field("isStatic", userFunctionNode.isStatic());
        scope.field("isSynthetic", userFunctionNode.isSynthetic());
        scope.field("isAutoReturnEnabled", userFunctionNode.isAutoReturnEnabled());

        scope.startArray("block");
        userFunctionNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitBlock(SBlock userBlockNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "block");
        scope.field(Fields.LOCATION, userBlockNode.getLocation().getOffset());

        scope.startArray("statements");
        userBlockNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitIf(SIf userIfNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "if");
        scope.field(Fields.LOCATION, userIfNode.getLocation().getOffset());

        scope.startArray("condition");
        userIfNode.getConditionNode().visit(this, scope);
        scope.endArray();

        block("ifBlock", userIfNode.getIfBlockNode(), scope);

        scope.endObject();
    }

    @Override
    public void visitIfElse(SIfElse userIfElseNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "ifElse");
        scope.field(Fields.LOCATION, userIfElseNode.getLocation().getOffset());

        scope.startArray("condition");
        userIfElseNode.getConditionNode().visit(this, scope);
        scope.endArray();

        block("ifBlock", userIfElseNode.getIfBlockNode(), scope);
        block("elseBlock", userIfElseNode.getElseBlockNode(), scope);

        scope.endObject();
    }

    @Override
    public void visitWhile(SWhile userWhileNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "while");
        scope.field(Fields.LOCATION, userWhileNode.getLocation().getOffset());

        scope.startArray("condition");
        userWhileNode.getConditionNode().visit(this, scope);
        scope.endArray();

        block(userWhileNode.getBlockNode(), scope);

        scope.endObject();
    }

    @Override
    public void visitDo(SDo userDoNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "do");
        scope.field(Fields.LOCATION, userDoNode.getLocation().getOffset());

        scope.startArray("");
        userDoNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitFor(SFor userForNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "for");
        scope.field(Fields.LOCATION, userForNode.getLocation().getOffset());

        scope.startArray("");
        userForNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitEach(SEach userEachNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "each");
        scope.field(Fields.LOCATION, userEachNode.getLocation().getOffset());

        scope.startArray("");
        userEachNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitDeclBlock(SDeclBlock userDeclBlockNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "declBlock");
        scope.field(Fields.LOCATION, userDeclBlockNode.getLocation().getOffset());

        scope.startArray("declarations");
        userDeclBlockNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitDeclaration(SDeclaration userDeclarationNode, UserTreePrinterScope scope) {scope.startObject();
        scope.field(Fields.NODE, "declaration");
        scope.field(Fields.LOCATION, userDeclarationNode.getLocation().getOffset());
        scope.field("type", userDeclarationNode.getCanonicalTypeName());
        scope.field("symbol", userDeclarationNode.getSymbol());
        scope.startArray("value");
        userDeclarationNode.visitChildren(this, scope);
        scope.endArray();
        scope.endObject();
    }

    @Override
    public void visitReturn(SReturn userReturnNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "return");
        scope.field(Fields.LOCATION, userReturnNode.getLocation().getOffset());

        scope.startArray("value");
        userReturnNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitExpression(SExpression userExpressionNode, UserTreePrinterScope scope) {scope.startObject();
        scope.field(Fields.NODE, "expression");
        scope.field(Fields.LOCATION, userExpressionNode.getLocation().getOffset());

        scope.startArray("statement");
        userExpressionNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitTry(STry userTryNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "try");
        scope.field(Fields.LOCATION, userTryNode.getLocation().getOffset());

        block(userTryNode.getBlockNode(), scope);

        scope.startArray("catch");
        for (SCatch catchNode : userTryNode.getCatchNodes()) {
            catchNode.visit(this, scope);
        }
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitCatch(SCatch userCatchNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "catch");
        scope.field(Fields.LOCATION, userCatchNode.getLocation().getOffset());

        scope.field("exception", userCatchNode.getBaseException());
        scope.field("type", userCatchNode.getCanonicalTypeName());
        scope.field("symbol", userCatchNode.getSymbol());

        scope.startArray("block");
        userCatchNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitThrow(SThrow userThrowNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "throw");
        scope.field(Fields.LOCATION, userThrowNode.getLocation().getOffset());

        scope.startArray("expression");
        userThrowNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitContinue(SContinue userContinueNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "continue");
        scope.field(Fields.LOCATION, userContinueNode.getLocation().getOffset());
        scope.endObject();
    }

    @Override
    public void visitBreak(SBreak userBreakNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "break");
        scope.field(Fields.LOCATION, userBreakNode.getLocation().getOffset());

        scope.startArray("");
        userBreakNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitAssignment(EAssignment userAssignmentNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "assignment");
        scope.field(Fields.LOCATION, userAssignmentNode.getLocation().getOffset());

        scope.startArray("");
        userAssignmentNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitUnary(EUnary userUnaryNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "unary");
        scope.field(Fields.LOCATION, userUnaryNode.getLocation().getOffset());

        scope.startArray("");
        userUnaryNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitBinary(EBinary userBinaryNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "binary");
        scope.field(Fields.LOCATION, userBinaryNode.getLocation().getOffset());

        scope.startArray("");
        userBinaryNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitBooleanComp(EBooleanComp userBooleanCompNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "booleanComp");
        scope.field(Fields.LOCATION, userBooleanCompNode.getLocation().getOffset());

        scope.startArray("");
        userBooleanCompNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitComp(EComp userCompNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "comp");
        scope.field(Fields.LOCATION, userCompNode.getLocation().getOffset());

        scope.startArray("");
        userCompNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitExplicit(EExplicit userExplicitNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "explicitCast");
        scope.field(Fields.LOCATION, userExplicitNode.getLocation().getOffset());

        scope.startArray("");
        userExplicitNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitInstanceof(EInstanceof userInstanceofNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "instanceof");
        scope.field(Fields.LOCATION, userInstanceofNode.getLocation().getOffset());

        scope.startArray("");
        userInstanceofNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitConditional(EConditional userConditionalNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "conditional");
        scope.field(Fields.LOCATION, userConditionalNode.getLocation().getOffset());

        scope.startArray("");
        userConditionalNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitElvis(EElvis userElvisNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "elvis");
        scope.field(Fields.LOCATION, userElvisNode.getLocation().getOffset());

        scope.startArray("");
        userElvisNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitListInit(EListInit userListInitNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "listInit");
        scope.field(Fields.LOCATION, userListInitNode.getLocation().getOffset());

        scope.startArray("");
        userListInitNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitMapInit(EMapInit userMapInitNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "mapInit");
        scope.field(Fields.LOCATION, userMapInitNode.getLocation().getOffset());

        scope.startArray("");
        userMapInitNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitNewArray(ENewArray userNewArrayNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "newArray");
        scope.field(Fields.LOCATION, userNewArrayNode.getLocation().getOffset());

        scope.startArray("");
        userNewArrayNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitNewObj(ENewObj userNewObjNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "newObject");
        scope.field(Fields.LOCATION, userNewObjNode.getLocation().getOffset());

        scope.startArray("");
        userNewObjNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitCallLocal(ECallLocal userCallLocalNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "callLocal");
        scope.field(Fields.LOCATION, userCallLocalNode.getLocation().getOffset());

        scope.startArray("");
        userCallLocalNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitBooleanConstant(EBooleanConstant userBooleanConstantNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "booleanConstant");
        scope.field(Fields.LOCATION, userBooleanConstantNode.getLocation().getOffset());

        scope.startArray("");
        userBooleanConstantNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitNumeric(ENumeric userNumericNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "numeric");
        scope.field(Fields.LOCATION, userNumericNode.getLocation().getOffset());

        scope.startArray("");
        userNumericNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitDecimal(EDecimal userDecimalNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "decimal");
        scope.field(Fields.LOCATION, userDecimalNode.getLocation().getOffset());

        scope.startArray("");
        userDecimalNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitString(EString userStringNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "string");
        scope.field(Fields.LOCATION, userStringNode.getLocation().getOffset());

        scope.startArray("");
        userStringNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitNull(ENull userNullNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "null");
        scope.field(Fields.LOCATION, userNullNode.getLocation().getOffset());

        scope.startArray("");
        userNullNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitRegex(ERegex userRegexNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "regex");
        scope.field(Fields.LOCATION, userRegexNode.getLocation().getOffset());

        scope.startArray("");
        userRegexNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitLambda(ELambda userLambdaNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "lambda");
        scope.field(Fields.LOCATION, userLambdaNode.getLocation().getOffset());

        scope.startArray("");
        userLambdaNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitFunctionRef(EFunctionRef userFunctionRefNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "functionRef");
        scope.field(Fields.LOCATION, userFunctionRefNode.getLocation().getOffset());

        scope.startArray("");
        userFunctionRefNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitNewArrayFunctionRef(ENewArrayFunctionRef userNewArrayFunctionRefNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "newArrayFunctionRef");
        scope.field(Fields.LOCATION, userNewArrayFunctionRefNode.getLocation().getOffset());

        scope.startArray("");
        userNewArrayFunctionRefNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitSymbol(ESymbol userSymbolNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "symbol");
        scope.field(Fields.LOCATION, userSymbolNode.getLocation().getOffset());

        scope.startArray("");
        userSymbolNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitDot(EDot userDotNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "dot");
        scope.field(Fields.LOCATION, userDotNode.getLocation().getOffset());

        scope.startArray("");
        userDotNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitBrace(EBrace userBraceNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "brace");
        scope.field(Fields.LOCATION, userBraceNode.getLocation().getOffset());

        scope.startArray("");
        userBraceNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitCall(ECall userCallNode, UserTreePrinterScope scope) {
        // TODO(stu): complete
        scope.startObject();
        scope.field(Fields.NODE, "call");
        scope.field(Fields.LOCATION, userCallNode.getLocation().getOffset());

        scope.startArray("");
        userCallNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    private void block(String name, SBlock block, UserTreePrinterScope scope) {
        scope.startArray(name);
        if (block != null) {
            block.visit(this, scope);
        }
        scope.endArray();
    }

    private void block(SBlock block, UserTreePrinterScope scope) {
        block("block", block, scope);
    }
}
