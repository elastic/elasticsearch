/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.printer;

import org.elasticsearch.painless.Operation;
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
import org.elasticsearch.painless.phase.UserTreeBaseVisitor;

import java.util.List;

public class UserTreeToXContent extends UserTreeBaseVisitor<UserTreePrinterScope> {
    static final class Fields {
        static final String NODE = "node";
        static final String LOCATION = "location";
        static final String LEFT = "left";
        static final String RIGHT = "right";
        static final String BLOCK = "block";
        static final String CONDITION = "condition";
        static final String TYPE = "type";
        static final String SYMBOL = "symbol";

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

        scope.startArray(Fields.BLOCK);
        userFunctionNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitBlock(SBlock userBlockNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, Fields.BLOCK);
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

        scope.startArray(Fields.CONDITION);
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

        scope.startArray(Fields.CONDITION);
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

        loop(userWhileNode.getConditionNode(), userWhileNode.getBlockNode(), scope);

        scope.endObject();
    }

    @Override
    public void visitDo(SDo userDoNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "do");
        scope.field(Fields.LOCATION, userDoNode.getLocation().getOffset());

        loop(userDoNode.getConditionNode(), userDoNode.getBlockNode(), scope);

        scope.endObject();
    }

    @Override
    public void visitFor(SFor userForNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "for");
        scope.field(Fields.LOCATION, userForNode.getLocation().getOffset());

        // TODO(stu): why is initializerNode ANode instead of an expression
        ANode initializerNode = userForNode.getInitializerNode();
        scope.startArray("initializer");
        if (initializerNode != null) {
            initializerNode.visit(this, scope);
        }
        scope.endArray();

        scope.startArray("condition");
        AExpression conditionNode = userForNode.getConditionNode();
        if (conditionNode != null) {
            conditionNode.visit(this, scope);
        }
        scope.endArray();

        scope.startArray("afterthought");
        AExpression afterthoughtNode = userForNode.getAfterthoughtNode();
        if (afterthoughtNode != null) {
            afterthoughtNode.visit(this, scope);
        }
        scope.endArray();

        block(userForNode.getBlockNode(), scope);

        scope.endObject();
    }

    @Override
    public void visitEach(SEach userEachNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "each");
        scope.field(Fields.LOCATION, userEachNode.getLocation().getOffset());

        scope.field(Fields.TYPE, userEachNode.getCanonicalTypeName());
        scope.field(Fields.SYMBOL, userEachNode.getSymbol());

        scope.startArray("iterable");
        userEachNode.getIterableNode().visitChildren(this, scope);
        scope.endArray();

        block(userEachNode.getBlockNode(), scope);

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
    public void visitDeclaration(SDeclaration userDeclarationNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "declaration");
        scope.field(Fields.LOCATION, userDeclarationNode.getLocation().getOffset());
        scope.field(Fields.TYPE, userDeclarationNode.getCanonicalTypeName());
        scope.field(Fields.SYMBOL, userDeclarationNode.getSymbol());

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
        scope.field(Fields.TYPE, userCatchNode.getCanonicalTypeName());
        scope.field(Fields.SYMBOL, userCatchNode.getSymbol());

        scope.startArray(Fields.BLOCK);
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
        scope.startObject();
        scope.field(Fields.NODE, "break");
        scope.field(Fields.LOCATION, userBreakNode.getLocation().getOffset());
        scope.endObject();
    }

    @Override
    public void visitAssignment(EAssignment userAssignmentNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "assignment");
        scope.field(Fields.LOCATION, userAssignmentNode.getLocation().getOffset());
        // TODO(stu): why would operation be null?
        scope.field("postIfRead", userAssignmentNode.postIfRead());
        binaryOperation(userAssignmentNode.getOperation(), userAssignmentNode.getLeftNode(), userAssignmentNode.getRightNode(), scope);

        scope.endObject();
    }

    @Override
    public void visitUnary(EUnary userUnaryNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "unary");
        scope.field(Fields.LOCATION, userUnaryNode.getLocation().getOffset());
        operation(userUnaryNode.getOperation(), scope);

        scope.startArray("child");
        userUnaryNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitBinary(EBinary userBinaryNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "binary");
        scope.field(Fields.LOCATION, userBinaryNode.getLocation().getOffset());

        binaryOperation(userBinaryNode.getOperation(), userBinaryNode.getLeftNode(), userBinaryNode.getRightNode(), scope);

        scope.endObject();
    }

    @Override
    public void visitBooleanComp(EBooleanComp userBooleanCompNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "booleanComp");
        scope.field(Fields.LOCATION, userBooleanCompNode.getLocation().getOffset());

        binaryOperation(userBooleanCompNode.getOperation(), userBooleanCompNode.getLeftNode(), userBooleanCompNode.getRightNode(), scope);

        scope.endObject();
    }

    @Override
    public void visitComp(EComp userCompNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "comp");
        scope.field(Fields.LOCATION, userCompNode.getLocation().getOffset());

        binaryOperation(userCompNode.getOperation(), userCompNode.getLeftNode(), userCompNode.getRightNode(), scope);

        scope.endObject();
    }

    @Override
    public void visitExplicit(EExplicit userExplicitNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "explicitCast");
        scope.field(Fields.LOCATION, userExplicitNode.getLocation().getOffset());

        scope.field(Fields.TYPE, userExplicitNode.getCanonicalTypeName());
        scope.startArray("child");
        userExplicitNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitInstanceof(EInstanceof userInstanceofNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "instanceof");
        scope.field(Fields.LOCATION, userInstanceofNode.getLocation().getOffset());

        scope.field(Fields.TYPE, userInstanceofNode.getCanonicalTypeName());
        scope.startArray("child");
        userInstanceofNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitConditional(EConditional userConditionalNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "conditional");
        scope.field(Fields.LOCATION, userConditionalNode.getLocation().getOffset());

        scope.startArray("condition");
        userConditionalNode.getConditionNode().visit(this, scope);
        scope.endArray();

        scope.startArray("true");
        userConditionalNode.getTrueNode().visit(this, scope);
        scope.endArray();

        scope.startArray("false");
        userConditionalNode.getFalseNode().visit(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitElvis(EElvis userElvisNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "elvis");
        scope.field(Fields.LOCATION, userElvisNode.getLocation().getOffset());

        scope.startArray(Fields.LEFT);
        userElvisNode.getLeftNode().visit(this, scope);
        scope.endArray();

        scope.startArray(Fields.RIGHT);
        userElvisNode.getRightNode().visit(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitListInit(EListInit userListInitNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "listInit");
        scope.field(Fields.LOCATION, userListInitNode.getLocation().getOffset());

        scope.startArray("values");
        userListInitNode.visitChildren(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitMapInit(EMapInit userMapInitNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "mapInit");
        scope.field(Fields.LOCATION, userMapInitNode.getLocation().getOffset());

        expressions("keys", userMapInitNode.getKeyNodes(), scope);
        expressions("values", userMapInitNode.getValueNodes(), scope);

        scope.endObject();
    }

    @Override
    public void visitNewArray(ENewArray userNewArrayNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "newArray");
        scope.field(Fields.LOCATION, userNewArrayNode.getLocation().getOffset());

        scope.field(Fields.TYPE, userNewArrayNode.getCanonicalTypeName());
        scope.field("isInitializer", userNewArrayNode.isInitializer());
        expressions("values", userNewArrayNode.getValueNodes(), scope);

        scope.endObject();
    }

    @Override
    public void visitNewObj(ENewObj userNewObjNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "newObject");
        scope.field(Fields.LOCATION, userNewObjNode.getLocation().getOffset());

        scope.field(Fields.TYPE, userNewObjNode.getCanonicalTypeName());
        arguments(userNewObjNode.getArgumentNodes(), scope);

        scope.endObject();
    }

    @Override
    public void visitCallLocal(ECallLocal userCallLocalNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "callLocal");
        scope.field(Fields.LOCATION, userCallLocalNode.getLocation().getOffset());

        scope.field("methodName", userCallLocalNode.getMethodName());
        arguments(userCallLocalNode.getArgumentNodes(), scope);

        scope.endObject();
    }

    @Override
    public void visitBooleanConstant(EBooleanConstant userBooleanConstantNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "booleanConstant");
        scope.field(Fields.LOCATION, userBooleanConstantNode.getLocation().getOffset());

        scope.field("value", userBooleanConstantNode.getBool());

        scope.endObject();
    }

    @Override
    public void visitNumeric(ENumeric userNumericNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "numeric");
        scope.field(Fields.LOCATION, userNumericNode.getLocation().getOffset());

        scope.field("numeric", userNumericNode.getNumeric());
        scope.field("radix", userNumericNode.getRadix());

        scope.endObject();
    }

    @Override
    public void visitDecimal(EDecimal userDecimalNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "decimal");
        scope.field(Fields.LOCATION, userDecimalNode.getLocation().getOffset());

        scope.field("value", userDecimalNode.getDecimal());

        scope.endObject();
    }

    @Override
    public void visitString(EString userStringNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "string");
        scope.field(Fields.LOCATION, userStringNode.getLocation().getOffset());

        scope.field("value", userStringNode.getString());

        scope.endObject();
    }

    @Override
    public void visitNull(ENull userNullNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "null");
        scope.field(Fields.LOCATION, userNullNode.getLocation().getOffset());

        scope.endObject();
    }

    @Override
    public void visitRegex(ERegex userRegexNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "regex");
        scope.field(Fields.LOCATION, userRegexNode.getLocation().getOffset());

        scope.field("pattern", userRegexNode.getPattern());
        scope.field("flags", userRegexNode.getFlags());

        scope.endObject();
    }

    @Override
    public void visitLambda(ELambda userLambdaNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "lambda");
        scope.field(Fields.LOCATION, userLambdaNode.getLocation().getOffset());

        scope.field("types", userLambdaNode.getCanonicalTypeNameParameters());
        scope.field("parameters", userLambdaNode.getParameterNames());
        block(userLambdaNode.getBlockNode(), scope);

        scope.endObject();
    }

    @Override
    public void visitFunctionRef(EFunctionRef userFunctionRefNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "functionRef");
        scope.field(Fields.LOCATION, userFunctionRefNode.getLocation().getOffset());

        scope.field(Fields.SYMBOL, userFunctionRefNode.getSymbol());
        scope.field("methodName", userFunctionRefNode.getMethodName());

        scope.endObject();
    }

    @Override
    public void visitNewArrayFunctionRef(ENewArrayFunctionRef userNewArrayFunctionRefNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "newArrayFunctionRef");
        scope.field(Fields.LOCATION, userNewArrayFunctionRefNode.getLocation().getOffset());

        scope.field(Fields.TYPE, userNewArrayFunctionRefNode.getCanonicalTypeName());

        scope.endObject();
    }

    @Override
    public void visitSymbol(ESymbol userSymbolNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, Fields.SYMBOL);
        scope.field(Fields.LOCATION, userSymbolNode.getLocation().getOffset());

        scope.field(Fields.SYMBOL, userSymbolNode.getSymbol());

        scope.endObject();
    }

    @Override
    public void visitDot(EDot userDotNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "dot");
        scope.field(Fields.LOCATION, userDotNode.getLocation().getOffset());

        scope.startArray("prefix");
        userDotNode.visitChildren(this, scope);
        scope.endArray();

        scope.field("index", userDotNode.getIndex());
        scope.field("nullSafe", userDotNode.isNullSafe());

        scope.endObject();
    }

    @Override
    public void visitBrace(EBrace userBraceNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "brace");
        scope.field(Fields.LOCATION, userBraceNode.getLocation().getOffset());

        scope.startArray("prefix");
        userBraceNode.getPrefixNode().visit(this, scope);
        scope.endArray();

        scope.startArray("index");
        userBraceNode.getIndexNode().visit(this, scope);
        scope.endArray();

        scope.endObject();
    }

    @Override
    public void visitCall(ECall userCallNode, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, "call");
        scope.field(Fields.LOCATION, userCallNode.getLocation().getOffset());

        scope.startArray("prefix");
        userCallNode.getPrefixNode().visitChildren(this, scope);
        scope.endArray();

        scope.field("isNullSafe", userCallNode.isNullSafe());
        scope.field("methodName", userCallNode.getMethodName());

        arguments(userCallNode.getArgumentNodes(), scope);

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
        block(Fields.BLOCK, block, scope);
    }

    private void loop(AExpression condition, SBlock block, UserTreePrinterScope scope) {
        scope.startArray(Fields.CONDITION);
        condition.visit(this, scope);
        scope.endArray();

        block(block, scope);
    }

    private void operation(Operation op, UserTreePrinterScope scope) {
        scope.startObject("operation");
        if (op != null) {
            scope.field(Fields.SYMBOL, op.symbol);
            scope.field("name", op.name);
        }
        scope.endObject();
    }

    private void binaryOperation(Operation op, AExpression left, AExpression right, UserTreePrinterScope scope) {
        operation(op, scope);

        scope.startArray(Fields.LEFT);
        left.visit(this, scope);
        scope.endArray();

        scope.startArray(Fields.RIGHT);
        right.visit(this, scope);
        scope.endArray();
    }

    private void arguments(List<AExpression> arguments, UserTreePrinterScope scope) {
        expressions("arguments", arguments, scope);
    }

    private void expressions(String name, List<AExpression> expressions, UserTreePrinterScope scope) {
        scope.startArray(name);
        for (AExpression expression : expressions) {
            expression.visit(this, scope);
        }
        scope.endArray();

    }
}
