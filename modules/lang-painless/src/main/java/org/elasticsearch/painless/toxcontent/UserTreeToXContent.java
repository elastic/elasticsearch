/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.toxcontent;

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
import org.elasticsearch.painless.symbol.Decorator.Condition;
import org.elasticsearch.painless.symbol.Decorator.Decoration;
import org.elasticsearch.painless.symbol.ScriptScope;
import org.elasticsearch.xcontent.XContentBuilder;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Serialize the user tree
 */
public class UserTreeToXContent extends UserTreeBaseVisitor<ScriptScope> {
    public final XContentBuilderWrapper builder;

    public UserTreeToXContent(XContentBuilder builder) {
        this.builder = new XContentBuilderWrapper(Objects.requireNonNull(builder));
    }

    public UserTreeToXContent() {
        this.builder = new XContentBuilderWrapper();
    }

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
    public void visitClass(SClass userClassNode, ScriptScope scope) {
        start(userClassNode);

        builder.field("source", scope.getScriptSource());
        builder.startArray("functions");
        userClassNode.visitChildren(this, scope);
        builder.endArray();

        end(userClassNode, scope);
    }

    @Override
    public void visitFunction(SFunction userFunctionNode, ScriptScope scope) {
        start(userFunctionNode);

        builder.field("name", userFunctionNode.getFunctionName());
        builder.field("returns", userFunctionNode.getReturnCanonicalTypeName());
        if (userFunctionNode.getParameterNames().isEmpty() == false) {
            builder.field("parameters", userFunctionNode.getParameterNames());
        }
        if (userFunctionNode.getCanonicalTypeNameParameters().isEmpty() == false) {
            builder.field("parameterTypes", userFunctionNode.getCanonicalTypeNameParameters());
        }
        builder.field("isInternal", userFunctionNode.isInternal());
        builder.field("isStatic", userFunctionNode.isStatic());
        builder.field("isSynthetic", userFunctionNode.isSynthetic());
        builder.field("isAutoReturnEnabled", userFunctionNode.isAutoReturnEnabled());

        builder.startArray(Fields.BLOCK);
        userFunctionNode.visitChildren(this, scope);
        builder.endArray();

        end(userFunctionNode, scope);
    }

    @Override
    public void visitBlock(SBlock userBlockNode, ScriptScope scope) {
        start(userBlockNode);

        builder.startArray("statements");
        userBlockNode.visitChildren(this, scope);
        builder.endArray();

        end(userBlockNode, scope);
    }

    @Override
    public void visitIf(SIf userIfNode, ScriptScope scope) {
        start(userIfNode);

        builder.startArray(Fields.CONDITION);
        userIfNode.getConditionNode().visit(this, scope);
        builder.endArray();

        block("ifBlock", userIfNode.getIfBlockNode(), scope);

        end(userIfNode, scope);
    }

    @Override
    public void visitIfElse(SIfElse userIfElseNode, ScriptScope scope) {
        start(userIfElseNode);

        builder.startArray(Fields.CONDITION);
        userIfElseNode.getConditionNode().visit(this, scope);
        builder.endArray();

        block("ifBlock", userIfElseNode.getIfBlockNode(), scope);
        block("elseBlock", userIfElseNode.getElseBlockNode(), scope);

        end(userIfElseNode, scope);
    }

    @Override
    public void visitWhile(SWhile userWhileNode, ScriptScope scope) {
        start(userWhileNode);
        loop(userWhileNode.getConditionNode(), userWhileNode.getBlockNode(), scope);
        end(userWhileNode, scope);
    }

    @Override
    public void visitDo(SDo userDoNode, ScriptScope scope) {
        start(userDoNode);
        loop(userDoNode.getConditionNode(), userDoNode.getBlockNode(), scope);
        end(userDoNode, scope);
    }

    @Override
    public void visitFor(SFor userForNode, ScriptScope scope) {
        start(userForNode);

        ANode initializerNode = userForNode.getInitializerNode();
        builder.startArray("initializer");
        if (initializerNode != null) {
            initializerNode.visit(this, scope);
        }
        builder.endArray();

        builder.startArray("condition");
        AExpression conditionNode = userForNode.getConditionNode();
        if (conditionNode != null) {
            conditionNode.visit(this, scope);
        }
        builder.endArray();

        builder.startArray("afterthought");
        AExpression afterthoughtNode = userForNode.getAfterthoughtNode();
        if (afterthoughtNode != null) {
            afterthoughtNode.visit(this, scope);
        }
        builder.endArray();

        block(userForNode.getBlockNode(), scope);

        end(userForNode, scope);
    }

    @Override
    public void visitEach(SEach userEachNode, ScriptScope scope) {
        start(userEachNode);

        builder.field(Fields.TYPE, userEachNode.getCanonicalTypeName());
        builder.field(Fields.SYMBOL, userEachNode.getSymbol());

        builder.startArray("iterable");
        userEachNode.getIterableNode().visitChildren(this, scope);
        builder.endArray();

        block(userEachNode.getBlockNode(), scope);

        end(userEachNode, scope);
    }

    @Override
    public void visitDeclBlock(SDeclBlock userDeclBlockNode, ScriptScope scope) {
        start(userDeclBlockNode);

        builder.startArray("declarations");
        userDeclBlockNode.visitChildren(this, scope);
        builder.endArray();

        end(userDeclBlockNode, scope);
    }

    @Override
    public void visitDeclaration(SDeclaration userDeclarationNode, ScriptScope scope) {
        start(userDeclarationNode);

        builder.field(Fields.TYPE, userDeclarationNode.getCanonicalTypeName());
        builder.field(Fields.SYMBOL, userDeclarationNode.getSymbol());

        builder.startArray("value");
        userDeclarationNode.visitChildren(this, scope);
        builder.endArray();

        end(userDeclarationNode, scope);
    }

    @Override
    public void visitReturn(SReturn userReturnNode, ScriptScope scope) {
        start(userReturnNode);

        builder.startArray("value");
        userReturnNode.visitChildren(this, scope);
        builder.endArray();

        end(userReturnNode, scope);
    }

    @Override
    public void visitExpression(SExpression userExpressionNode, ScriptScope scope) {
        start(userExpressionNode);

        builder.startArray("statement");
        userExpressionNode.visitChildren(this, scope);
        builder.endArray();

        end(userExpressionNode, scope);
    }

    @Override
    public void visitTry(STry userTryNode, ScriptScope scope) {
        start(userTryNode);

        block(userTryNode.getBlockNode(), scope);

        builder.startArray("catch");
        for (SCatch catchNode : userTryNode.getCatchNodes()) {
            catchNode.visit(this, scope);
        }
        builder.endArray();

        end(userTryNode, scope);
    }

    @Override
    public void visitCatch(SCatch userCatchNode, ScriptScope scope) {
        start(userCatchNode);

        builder.field("exception", userCatchNode.getBaseException());
        builder.field(Fields.TYPE, userCatchNode.getCanonicalTypeName());
        builder.field(Fields.SYMBOL, userCatchNode.getSymbol());

        builder.startArray(Fields.BLOCK);
        userCatchNode.visitChildren(this, scope);
        builder.endArray();

        end(userCatchNode, scope);
    }

    @Override
    public void visitThrow(SThrow userThrowNode, ScriptScope scope) {
        start(userThrowNode);

        builder.startArray("expression");
        userThrowNode.visitChildren(this, scope);
        builder.endArray();

        end(userThrowNode, scope);
    }

    @Override
    public void visitContinue(SContinue userContinueNode, ScriptScope scope) {
        start(userContinueNode);
        end(userContinueNode, scope);
    }

    @Override
    public void visitBreak(SBreak userBreakNode, ScriptScope scope) {
        start(userBreakNode);
        end(userBreakNode, scope);
    }

    @Override
    public void visitAssignment(EAssignment userAssignmentNode, ScriptScope scope) {
        start(userAssignmentNode);
        // TODO(stu): why would operation be null?
        builder.field("postIfRead", userAssignmentNode.postIfRead());
        binaryOperation(userAssignmentNode.getOperation(), userAssignmentNode.getLeftNode(), userAssignmentNode.getRightNode(), scope);
        end(userAssignmentNode, scope);
    }

    @Override
    public void visitUnary(EUnary userUnaryNode, ScriptScope scope) {
        start(userUnaryNode);

        operation(userUnaryNode.getOperation());

        builder.startArray("child");
        userUnaryNode.visitChildren(this, scope);
        builder.endArray();

        end(userUnaryNode, scope);
    }

    @Override
    public void visitBinary(EBinary userBinaryNode, ScriptScope scope) {
        start(userBinaryNode);
        binaryOperation(userBinaryNode.getOperation(), userBinaryNode.getLeftNode(), userBinaryNode.getRightNode(), scope);
        end(userBinaryNode, scope);
    }

    @Override
    public void visitBooleanComp(EBooleanComp userBooleanCompNode, ScriptScope scope) {
        start(userBooleanCompNode);
        binaryOperation(userBooleanCompNode.getOperation(), userBooleanCompNode.getLeftNode(), userBooleanCompNode.getRightNode(), scope);
        end(userBooleanCompNode, scope);
    }

    @Override
    public void visitComp(EComp userCompNode, ScriptScope scope) {
        start(userCompNode);
        binaryOperation(userCompNode.getOperation(), userCompNode.getLeftNode(), userCompNode.getRightNode(), scope);
        end(userCompNode, scope);
    }

    @Override
    public void visitExplicit(EExplicit userExplicitNode, ScriptScope scope) {
        start(userExplicitNode);

        builder.field(Fields.TYPE, userExplicitNode.getCanonicalTypeName());
        builder.startArray("child");
        userExplicitNode.visitChildren(this, scope);
        builder.endArray();

        end(userExplicitNode, scope);
    }

    @Override
    public void visitInstanceof(EInstanceof userInstanceofNode, ScriptScope scope) {
        start(userInstanceofNode);

        builder.field(Fields.TYPE, userInstanceofNode.getCanonicalTypeName());
        builder.startArray("child");
        userInstanceofNode.visitChildren(this, scope);
        builder.endArray();

        end(userInstanceofNode, scope);
    }

    @Override
    public void visitConditional(EConditional userConditionalNode, ScriptScope scope) {
        start(userConditionalNode);

        builder.startArray("condition");
        userConditionalNode.getConditionNode().visit(this, scope);
        builder.endArray();

        builder.startArray("true");
        userConditionalNode.getTrueNode().visit(this, scope);
        builder.endArray();

        builder.startArray("false");
        userConditionalNode.getFalseNode().visit(this, scope);
        builder.endArray();

        end(userConditionalNode, scope);
    }

    @Override
    public void visitElvis(EElvis userElvisNode, ScriptScope scope) {
        start(userElvisNode);

        builder.startArray(Fields.LEFT);
        userElvisNode.getLeftNode().visit(this, scope);
        builder.endArray();

        builder.startArray(Fields.RIGHT);
        userElvisNode.getRightNode().visit(this, scope);
        builder.endArray();

        end(userElvisNode, scope);
    }

    @Override
    public void visitListInit(EListInit userListInitNode, ScriptScope scope) {
        start(userListInitNode);
        builder.startArray("values");
        userListInitNode.visitChildren(this, scope);
        builder.endArray();
        end(userListInitNode, scope);
    }

    @Override
    public void visitMapInit(EMapInit userMapInitNode, ScriptScope scope) {
        start(userMapInitNode);
        expressions("keys", userMapInitNode.getKeyNodes(), scope);
        expressions("values", userMapInitNode.getValueNodes(), scope);
        end(userMapInitNode, scope);
    }

    @Override
    public void visitNewArray(ENewArray userNewArrayNode, ScriptScope scope) {
        start(userNewArrayNode);
        builder.field(Fields.TYPE, userNewArrayNode.getCanonicalTypeName());
        builder.field("isInitializer", userNewArrayNode.isInitializer());
        expressions("values", userNewArrayNode.getValueNodes(), scope);
        end(userNewArrayNode, scope);
    }

    @Override
    public void visitNewObj(ENewObj userNewObjNode, ScriptScope scope) {
        start(userNewObjNode);
        builder.field(Fields.TYPE, userNewObjNode.getCanonicalTypeName());
        arguments(userNewObjNode.getArgumentNodes(), scope);
        end(userNewObjNode, scope);
    }

    @Override
    public void visitCallLocal(ECallLocal userCallLocalNode, ScriptScope scope) {
        start(userCallLocalNode);
        builder.field("methodName", userCallLocalNode.getMethodName());
        arguments(userCallLocalNode.getArgumentNodes(), scope);
        end(userCallLocalNode, scope);
    }

    @Override
    public void visitBooleanConstant(EBooleanConstant userBooleanConstantNode, ScriptScope scope) {
        start(userBooleanConstantNode);
        builder.field("value", userBooleanConstantNode.getBool());
        end(userBooleanConstantNode, scope);
    }

    @Override
    public void visitNumeric(ENumeric userNumericNode, ScriptScope scope) {
        start(userNumericNode);
        builder.field("numeric", userNumericNode.getNumeric());
        builder.field("radix", userNumericNode.getRadix());
        end(userNumericNode, scope);
    }

    @Override
    public void visitDecimal(EDecimal userDecimalNode, ScriptScope scope) {
        start(userDecimalNode);
        builder.field("value", userDecimalNode.getDecimal());
        end(userDecimalNode, scope);
    }

    @Override
    public void visitString(EString userStringNode, ScriptScope scope) {
        start(userStringNode);
        builder.field("value", userStringNode.getString());
        end(userStringNode, scope);
    }

    @Override
    public void visitNull(ENull userNullNode, ScriptScope scope) {
        start(userNullNode);
        end(userNullNode, scope);
    }

    @Override
    public void visitRegex(ERegex userRegexNode, ScriptScope scope) {
        start(userRegexNode);
        builder.field("pattern", userRegexNode.getPattern());
        builder.field("flags", userRegexNode.getFlags());
        end(userRegexNode, scope);
    }

    @Override
    public void visitLambda(ELambda userLambdaNode, ScriptScope scope) {
        start(userLambdaNode);
        builder.field("types", userLambdaNode.getCanonicalTypeNameParameters());
        builder.field("parameters", userLambdaNode.getParameterNames());
        block(userLambdaNode.getBlockNode(), scope);
        end(userLambdaNode, scope);
    }

    @Override
    public void visitFunctionRef(EFunctionRef userFunctionRefNode, ScriptScope scope) {
        start(userFunctionRefNode);
        builder.field(Fields.SYMBOL, userFunctionRefNode.getSymbol());
        builder.field("methodName", userFunctionRefNode.getMethodName());
        end(userFunctionRefNode, scope);
    }

    @Override
    public void visitNewArrayFunctionRef(ENewArrayFunctionRef userNewArrayFunctionRefNode, ScriptScope scope) {
        start(userNewArrayFunctionRefNode);
        builder.field(Fields.TYPE, userNewArrayFunctionRefNode.getCanonicalTypeName());
        end(userNewArrayFunctionRefNode, scope);
    }

    @Override
    public void visitSymbol(ESymbol userSymbolNode, ScriptScope scope) {
        start(userSymbolNode);
        builder.field(Fields.SYMBOL, userSymbolNode.getSymbol());
        end(userSymbolNode, scope);
    }

    @Override
    public void visitDot(EDot userDotNode, ScriptScope scope) {
        start(userDotNode);

        builder.startArray("prefix");
        userDotNode.visitChildren(this, scope);
        builder.endArray();

        builder.field("index", userDotNode.getIndex());
        builder.field("nullSafe", userDotNode.isNullSafe());

        end(userDotNode, scope);
    }

    @Override
    public void visitBrace(EBrace userBraceNode, ScriptScope scope) {
        start(userBraceNode);

        builder.startArray("prefix");
        userBraceNode.getPrefixNode().visit(this, scope);
        builder.endArray();

        builder.startArray("index");
        userBraceNode.getIndexNode().visit(this, scope);
        builder.endArray();

        end(userBraceNode, scope);
    }

    @Override
    public void visitCall(ECall userCallNode, ScriptScope scope) {
        start(userCallNode);

        builder.startArray("prefix");
        userCallNode.getPrefixNode().visitChildren(this, scope);
        builder.endArray();

        builder.field("isNullSafe", userCallNode.isNullSafe());
        builder.field("methodName", userCallNode.getMethodName());

        arguments(userCallNode.getArgumentNodes(), scope);

        end(userCallNode, scope);
    }

    private void start(ANode node) {
        builder.startObject();
        builder.field(Fields.NODE, node.getClass().getSimpleName());
        builder.field(Fields.LOCATION, node.getLocation().getOffset());
    }

    private void end(ANode node, ScriptScope scope) {
        decorations(node, scope);
        builder.endObject();
    }

    private void block(String name, SBlock block, ScriptScope scope) {
        builder.startArray(name);
        if (block != null) {
            block.visit(this, scope);
        }
        builder.endArray();
    }

    private void block(SBlock block, ScriptScope scope) {
        block(Fields.BLOCK, block, scope);
    }

    private void loop(AExpression condition, SBlock block, ScriptScope scope) {
        builder.startArray(Fields.CONDITION);
        condition.visit(this, scope);
        builder.endArray();

        block(block, scope);
    }

    private void operation(Operation op) {
        builder.startObject("operation");
        if (op != null) {
            builder.field(Fields.SYMBOL, op.symbol);
            builder.field("name", op.name);
        }
        builder.endObject();
    }

    private void binaryOperation(Operation op, AExpression left, AExpression right, ScriptScope scope) {
        operation(op);

        builder.startArray(Fields.LEFT);
        left.visit(this, scope);
        builder.endArray();

        builder.startArray(Fields.RIGHT);
        right.visit(this, scope);
        builder.endArray();
    }

    private void arguments(List<AExpression> arguments, ScriptScope scope) {
        if (arguments.isEmpty() == false) {
            expressions("arguments", arguments, scope);
        }
    }

    private void expressions(String name, List<AExpression> expressions, ScriptScope scope) {
        if (expressions.isEmpty() == false) {
            builder.startArray(name);
            for (AExpression expression : expressions) {
                expression.visit(this, scope);
            }
            builder.endArray();
        }
    }

    private void decorations(ANode node, ScriptScope scope) {
        Set<Class<? extends Condition>> conditions = scope.getAllConditions(node.getIdentifier());
        if (conditions.isEmpty() == false) {
            builder.field(Fields.CONDITIONS, conditions.stream().map(Class::getSimpleName).sorted().collect(Collectors.toList()));
        }

        Map<Class<? extends Decoration>, Decoration> decorations = scope.getAllDecorations(node.getIdentifier());
        if (decorations.isEmpty() == false) {
            builder.startArray(Fields.DECORATIONS);

            decorations.keySet()
                .stream()
                .sorted(Comparator.comparing(Class::getName))
                .forEachOrdered(dkey -> DecorationToXContent.ToXContent(decorations.get(dkey), builder));
            builder.endArray();
        }
    }

    @Override
    public String toString() {
        return builder.toString();
    }
}
