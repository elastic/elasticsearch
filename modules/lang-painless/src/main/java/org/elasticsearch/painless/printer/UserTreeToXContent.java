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
import org.elasticsearch.painless.symbol.Decorations;
import org.elasticsearch.painless.symbol.Decorator;
import org.elasticsearch.painless.symbol.Decorator.Condition;
import org.elasticsearch.painless.symbol.Decorator.Decoration;
import org.elasticsearch.painless.symbol.ScriptScope;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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
        static final String DECORATIONS = "decorations";
        static final String CONDITIONS = "conditions";
    }

    @Override
    public void visitClass(SClass userClassNode, UserTreePrinterScope scope) {
        start(userClassNode, scope);

        scope.field("source", scope.scriptScope.getScriptSource());
        scope.startArray("functions");
        userClassNode.visitChildren(this, scope);
        scope.endArray();

        end(userClassNode, scope);
    }

    @Override
    public void visitFunction(SFunction userFunctionNode, UserTreePrinterScope scope) {
        start(userFunctionNode, scope);

        scope.field("name", userFunctionNode.getFunctionName());
        scope.field("returns", userFunctionNode.getReturnCanonicalTypeName());
        if (userFunctionNode.getParameterNames().isEmpty() == false) {
            scope.field("parameters", userFunctionNode.getParameterNames());
        }
        if (userFunctionNode.getCanonicalTypeNameParameters().isEmpty() == false) {
            scope.field("parameterTypes", userFunctionNode.getCanonicalTypeNameParameters());
        }
        scope.field("isInternal", userFunctionNode.isInternal());
        scope.field("isStatic", userFunctionNode.isStatic());
        scope.field("isSynthetic", userFunctionNode.isSynthetic());
        scope.field("isAutoReturnEnabled", userFunctionNode.isAutoReturnEnabled());

        scope.startArray(Fields.BLOCK);
        userFunctionNode.visitChildren(this, scope);
        scope.endArray();

        end(userFunctionNode, scope);
    }

    @Override
    public void visitBlock(SBlock userBlockNode, UserTreePrinterScope scope) {
        start(userBlockNode, scope);

        scope.startArray("statements");
         userBlockNode.visitChildren(this, scope);
        scope.endArray();

        end(userBlockNode, scope);
    }

    @Override
    public void visitIf(SIf userIfNode, UserTreePrinterScope scope) {
        start(userIfNode, scope);

        scope.startArray(Fields.CONDITION);
        userIfNode.getConditionNode().visit(this, scope);
        scope.endArray();

        block("ifBlock", userIfNode.getIfBlockNode(), scope);

        end(userIfNode, scope);
    }

    @Override
    public void visitIfElse(SIfElse userIfElseNode, UserTreePrinterScope scope) {
        start(userIfElseNode, scope);

        scope.startArray(Fields.CONDITION);
        userIfElseNode.getConditionNode().visit(this, scope);
        scope.endArray();

        block("ifBlock", userIfElseNode.getIfBlockNode(), scope);
        block("elseBlock", userIfElseNode.getElseBlockNode(), scope);

        end(userIfElseNode, scope);
    }

    @Override
    public void visitWhile(SWhile userWhileNode, UserTreePrinterScope scope) {
        start(userWhileNode, scope);
        loop(userWhileNode.getConditionNode(), userWhileNode.getBlockNode(), scope);
        end(userWhileNode, scope);
    }

    @Override
    public void visitDo(SDo userDoNode, UserTreePrinterScope scope) {
        start(userDoNode, scope);

        loop(userDoNode.getConditionNode(), userDoNode.getBlockNode(), scope);

        end(userDoNode, scope);
    }

    @Override
    public void visitFor(SFor userForNode, UserTreePrinterScope scope) {
        start(userForNode, scope);

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

        end(userForNode, scope);
    }

    @Override
    public void visitEach(SEach userEachNode, UserTreePrinterScope scope) {
        start(userEachNode, scope);

        scope.field(Fields.TYPE, userEachNode.getCanonicalTypeName());
        scope.field(Fields.SYMBOL, userEachNode.getSymbol());

        scope.startArray("iterable");
        userEachNode.getIterableNode().visitChildren(this, scope);
        scope.endArray();

        block(userEachNode.getBlockNode(), scope);

        end(userEachNode, scope);
    }

    @Override
    public void visitDeclBlock(SDeclBlock userDeclBlockNode, UserTreePrinterScope scope) {
        start(userDeclBlockNode, scope);

        scope.startArray("declarations");
        userDeclBlockNode.visitChildren(this, scope);
        scope.endArray();

        end(userDeclBlockNode, scope);
    }

    @Override
    public void visitDeclaration(SDeclaration userDeclarationNode, UserTreePrinterScope scope) {
        start(userDeclarationNode, scope);

        scope.field(Fields.TYPE, userDeclarationNode.getCanonicalTypeName());
        scope.field(Fields.SYMBOL, userDeclarationNode.getSymbol());

        scope.startArray("value");
        userDeclarationNode.visitChildren(this, scope);
        scope.endArray();

        end(userDeclarationNode, scope);
    }

    @Override
    public void visitReturn(SReturn userReturnNode, UserTreePrinterScope scope) {
        start(userReturnNode, scope);

        scope.startArray("value");
        userReturnNode.visitChildren(this, scope);
        scope.endArray();

        end(userReturnNode, scope);
    }

    @Override
    public void visitExpression(SExpression userExpressionNode, UserTreePrinterScope scope) {
        start(userExpressionNode, scope);

        scope.startArray("statement");
        userExpressionNode.visitChildren(this, scope);
        scope.endArray();

        end(userExpressionNode, scope);
    }

    @Override
    public void visitTry(STry userTryNode, UserTreePrinterScope scope) {
        start(userTryNode, scope);

        block(userTryNode.getBlockNode(), scope);

        scope.startArray("catch");
        for (SCatch catchNode : userTryNode.getCatchNodes()) {
            catchNode.visit(this, scope);
        }
        scope.endArray();

        end(userTryNode, scope);
    }

    @Override
    public void visitCatch(SCatch userCatchNode, UserTreePrinterScope scope) {
        start(userCatchNode, scope);

        scope.field("exception", userCatchNode.getBaseException());
        scope.field(Fields.TYPE, userCatchNode.getCanonicalTypeName());
        scope.field(Fields.SYMBOL, userCatchNode.getSymbol());

        scope.startArray(Fields.BLOCK);
        userCatchNode.visitChildren(this, scope);
        scope.endArray();

        end(userCatchNode, scope);
    }

    @Override
    public void visitThrow(SThrow userThrowNode, UserTreePrinterScope scope) {
        start(userThrowNode, scope);

        scope.startArray("expression");
        userThrowNode.visitChildren(this, scope);
        scope.endArray();

        end(userThrowNode, scope);
    }

    @Override
    public void visitContinue(SContinue userContinueNode, UserTreePrinterScope scope) {
        start(userContinueNode, scope);
        end(userContinueNode, scope);
    }

    @Override
    public void visitBreak(SBreak userBreakNode, UserTreePrinterScope scope) {
        start(userBreakNode, scope);
        end(userBreakNode, scope);
    }

    @Override
    public void visitAssignment(EAssignment userAssignmentNode, UserTreePrinterScope scope) {
        start(userAssignmentNode, scope);
        // TODO(stu): why would operation be null?
        scope.field("postIfRead", userAssignmentNode.postIfRead());
        binaryOperation(userAssignmentNode.getOperation(), userAssignmentNode.getLeftNode(), userAssignmentNode.getRightNode(), scope);
        end(userAssignmentNode, scope);
    }

    @Override
    public void visitUnary(EUnary userUnaryNode, UserTreePrinterScope scope) {
        start(userUnaryNode, scope);

        operation(userUnaryNode.getOperation(), scope);

        scope.startArray("child");
        userUnaryNode.visitChildren(this, scope);
        scope.endArray();

        end(userUnaryNode, scope);
    }

    @Override
    public void visitBinary(EBinary userBinaryNode, UserTreePrinterScope scope) {
        start(userBinaryNode, scope);
        binaryOperation(userBinaryNode.getOperation(), userBinaryNode.getLeftNode(), userBinaryNode.getRightNode(), scope);
        end(userBinaryNode, scope);
    }

    @Override
    public void visitBooleanComp(EBooleanComp userBooleanCompNode, UserTreePrinterScope scope) {
        start(userBooleanCompNode, scope);
        binaryOperation(userBooleanCompNode.getOperation(), userBooleanCompNode.getLeftNode(), userBooleanCompNode.getRightNode(), scope);
        end(userBooleanCompNode, scope);
    }

    @Override
    public void visitComp(EComp userCompNode, UserTreePrinterScope scope) {
        start(userCompNode, scope);
        binaryOperation(userCompNode.getOperation(), userCompNode.getLeftNode(), userCompNode.getRightNode(), scope);
        end(userCompNode, scope);
    }

    @Override
    public void visitExplicit(EExplicit userExplicitNode, UserTreePrinterScope scope) {
        start(userExplicitNode, scope);

        scope.field(Fields.TYPE, userExplicitNode.getCanonicalTypeName());
        scope.startArray("child");
        userExplicitNode.visitChildren(this, scope);
        scope.endArray();

        end(userExplicitNode, scope);
    }

    @Override
    public void visitInstanceof(EInstanceof userInstanceofNode, UserTreePrinterScope scope) {
        start(userInstanceofNode, scope);

        scope.field(Fields.TYPE, userInstanceofNode.getCanonicalTypeName());
        scope.startArray("child");
        userInstanceofNode.visitChildren(this, scope);
        scope.endArray();

        end(userInstanceofNode, scope);
    }

    @Override
    public void visitConditional(EConditional userConditionalNode, UserTreePrinterScope scope) {
        start(userConditionalNode, scope);

        scope.startArray("condition");
        userConditionalNode.getConditionNode().visit(this, scope);
        scope.endArray();

        scope.startArray("true");
        userConditionalNode.getTrueNode().visit(this, scope);
        scope.endArray();

        scope.startArray("false");
        userConditionalNode.getFalseNode().visit(this, scope);
        scope.endArray();

        end(userConditionalNode, scope);
    }

    @Override
    public void visitElvis(EElvis userElvisNode, UserTreePrinterScope scope) {
        start(userElvisNode, scope);

        scope.startArray(Fields.LEFT);
        userElvisNode.getLeftNode().visit(this, scope);
        scope.endArray();

        scope.startArray(Fields.RIGHT);
        userElvisNode.getRightNode().visit(this, scope);
        scope.endArray();

        end(userElvisNode, scope);
    }

    @Override
    public void visitListInit(EListInit userListInitNode, UserTreePrinterScope scope) {
        start(userListInitNode, scope);
        scope.startArray("values");
        userListInitNode.visitChildren(this, scope);
        scope.endArray();
        end(userListInitNode, scope);
    }

    @Override
    public void visitMapInit(EMapInit userMapInitNode, UserTreePrinterScope scope) {
        start(userMapInitNode, scope);
        expressions("keys", userMapInitNode.getKeyNodes(), scope);
        expressions("values", userMapInitNode.getValueNodes(), scope);
        end(userMapInitNode, scope);
    }

    @Override
    public void visitNewArray(ENewArray userNewArrayNode, UserTreePrinterScope scope) {
        start(userNewArrayNode, scope);
        scope.field(Fields.TYPE, userNewArrayNode.getCanonicalTypeName());
        scope.field("isInitializer", userNewArrayNode.isInitializer());
        expressions("values", userNewArrayNode.getValueNodes(), scope);
        end(userNewArrayNode, scope);
    }

    @Override
    public void visitNewObj(ENewObj userNewObjNode, UserTreePrinterScope scope) {
        start(userNewObjNode, scope);
        scope.field(Fields.TYPE, userNewObjNode.getCanonicalTypeName());
        arguments(userNewObjNode.getArgumentNodes(), scope);
        end(userNewObjNode, scope);
    }

    @Override
    public void visitCallLocal(ECallLocal userCallLocalNode, UserTreePrinterScope scope) {
        start(userCallLocalNode, scope);
        scope.field("methodName", userCallLocalNode.getMethodName());
        arguments(userCallLocalNode.getArgumentNodes(), scope);
        end(userCallLocalNode, scope);
    }

    @Override
    public void visitBooleanConstant(EBooleanConstant userBooleanConstantNode, UserTreePrinterScope scope) {
        start(userBooleanConstantNode, scope);
        scope.field("value", userBooleanConstantNode.getBool());
        end(userBooleanConstantNode, scope);
    }

    @Override
    public void visitNumeric(ENumeric userNumericNode, UserTreePrinterScope scope) {
        start(userNumericNode, scope);
        scope.field("numeric", userNumericNode.getNumeric());
        scope.field("radix", userNumericNode.getRadix());
        end(userNumericNode, scope);
    }

    @Override
    public void visitDecimal(EDecimal userDecimalNode, UserTreePrinterScope scope) {
        start(userDecimalNode, scope);
        scope.field("value", userDecimalNode.getDecimal());
        end(userDecimalNode, scope);
    }

    @Override
    public void visitString(EString userStringNode, UserTreePrinterScope scope) {
        start(userStringNode, scope);
        scope.field("value", userStringNode.getString());
        end(userStringNode, scope);
    }

    @Override
    public void visitNull(ENull userNullNode, UserTreePrinterScope scope) {
        start(userNullNode, scope);
        end(userNullNode, scope);
    }

    @Override
    public void visitRegex(ERegex userRegexNode, UserTreePrinterScope scope) {
        start(userRegexNode, scope);
        scope.field("pattern", userRegexNode.getPattern());
        scope.field("flags", userRegexNode.getFlags());
        end(userRegexNode, scope);
    }

    @Override
    public void visitLambda(ELambda userLambdaNode, UserTreePrinterScope scope) {
        start(userLambdaNode, scope);
        scope.field("types", userLambdaNode.getCanonicalTypeNameParameters());
        scope.field("parameters", userLambdaNode.getParameterNames());
        block(userLambdaNode.getBlockNode(), scope);
        end(userLambdaNode, scope);
    }

    @Override
    public void visitFunctionRef(EFunctionRef userFunctionRefNode, UserTreePrinterScope scope) {
        start(userFunctionRefNode, scope);
        scope.field(Fields.SYMBOL, userFunctionRefNode.getSymbol());
        scope.field("methodName", userFunctionRefNode.getMethodName());
        end(userFunctionRefNode, scope);
    }

    @Override
    public void visitNewArrayFunctionRef(ENewArrayFunctionRef userNewArrayFunctionRefNode, UserTreePrinterScope scope) {
        start(userNewArrayFunctionRefNode, scope);
        scope.field(Fields.TYPE, userNewArrayFunctionRefNode.getCanonicalTypeName());
        end(userNewArrayFunctionRefNode, scope);
    }

    @Override
    public void visitSymbol(ESymbol userSymbolNode, UserTreePrinterScope scope) {
        start(userSymbolNode, scope);
        scope.field(Fields.SYMBOL, userSymbolNode.getSymbol());
        end(userSymbolNode, scope);
    }

    @Override
    public void visitDot(EDot userDotNode, UserTreePrinterScope scope) {
        start(userDotNode, scope);

        scope.startArray("prefix");
        userDotNode.visitChildren(this, scope);
        scope.endArray();

        scope.field("index", userDotNode.getIndex());
        scope.field("nullSafe", userDotNode.isNullSafe());

        end(userDotNode, scope);
    }

    @Override
    public void visitBrace(EBrace userBraceNode, UserTreePrinterScope scope) {
        start(userBraceNode, scope);

        scope.startArray("prefix");
        userBraceNode.getPrefixNode().visit(this, scope);
        scope.endArray();

        scope.startArray("index");
        userBraceNode.getIndexNode().visit(this, scope);
        scope.endArray();

        end(userBraceNode, scope);
    }

    @Override
    public void visitCall(ECall userCallNode, UserTreePrinterScope scope) {
        start(userCallNode, scope);

        scope.startArray("prefix");
        userCallNode.getPrefixNode().visitChildren(this, scope);
        scope.endArray();

        scope.field("isNullSafe", userCallNode.isNullSafe());
        scope.field("methodName", userCallNode.getMethodName());

        arguments(userCallNode.getArgumentNodes(), scope);

        end(userCallNode, scope);
    }

    private void start(ANode node, UserTreePrinterScope scope) {
        scope.startObject();
        scope.field(Fields.NODE, node.getClass().getSimpleName());
        scope.field(Fields.LOCATION, node.getLocation().getOffset());
    }

    private void end(ANode node, UserTreePrinterScope scope) {
        decorations(node, scope);
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
        if (arguments.isEmpty() == false) {
            expressions("arguments", arguments, scope);
        }
    }

    private void expressions(String name, List<AExpression> expressions, UserTreePrinterScope scope) {
        if (expressions.isEmpty() == false) {
            scope.startArray(name);
            for (AExpression expression : expressions) {
                expression.visit(this, scope);
            }
            scope.endArray();
        }
    }

    private void decorations(ANode node, UserTreePrinterScope scope) {
        Set<Class<? extends Condition>> conditions = scope.scriptScope.getAllConditions(node.getIdentifier());
        if (conditions.isEmpty() == false) {
            scope.field(Fields.CONDITIONS, conditions.stream().map(Class::getSimpleName).sorted().collect(Collectors.toList()));
        }

        Map<Class<? extends Decoration>, Decoration> decorations = scope.scriptScope.getAllDecorations(node.getIdentifier());
        if (decorations.isEmpty() == false) {
            scope.startArray(Fields.DECORATIONS);

            List<Class<? extends Decoration>> dkeys = decorations.keySet().stream()
                    .sorted(Comparator.comparing(Class::getName))
                    .collect(Collectors.toList());

            for (Class<? extends Decoration> dkey : dkeys) {
                DecorationToXContent.ToXContent(decorations.get(dkey), scope);
            }
            scope.endArray();
        }
    }
}
