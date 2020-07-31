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

import org.elasticsearch.painless.ir.AssignmentNode;
import org.elasticsearch.painless.ir.BinaryMathNode;
import org.elasticsearch.painless.ir.BlockNode;
import org.elasticsearch.painless.ir.BooleanNode;
import org.elasticsearch.painless.ir.BraceNode;
import org.elasticsearch.painless.ir.BraceSubDefNode;
import org.elasticsearch.painless.ir.BraceSubNode;
import org.elasticsearch.painless.ir.BreakNode;
import org.elasticsearch.painless.ir.CallNode;
import org.elasticsearch.painless.ir.CallSubDefNode;
import org.elasticsearch.painless.ir.CallSubNode;
import org.elasticsearch.painless.ir.CastNode;
import org.elasticsearch.painless.ir.CatchNode;
import org.elasticsearch.painless.ir.ClassNode;
import org.elasticsearch.painless.ir.ComparisonNode;
import org.elasticsearch.painless.ir.ConditionalNode;
import org.elasticsearch.painless.ir.ConstantNode;
import org.elasticsearch.painless.ir.ContinueNode;
import org.elasticsearch.painless.ir.DeclarationBlockNode;
import org.elasticsearch.painless.ir.DeclarationNode;
import org.elasticsearch.painless.ir.DefInterfaceReferenceNode;
import org.elasticsearch.painless.ir.DoWhileLoopNode;
import org.elasticsearch.painless.ir.DotNode;
import org.elasticsearch.painless.ir.DotSubArrayLengthNode;
import org.elasticsearch.painless.ir.DotSubDefNode;
import org.elasticsearch.painless.ir.DotSubNode;
import org.elasticsearch.painless.ir.DotSubShortcutNode;
import org.elasticsearch.painless.ir.ElvisNode;
import org.elasticsearch.painless.ir.FieldNode;
import org.elasticsearch.painless.ir.ForEachLoopNode;
import org.elasticsearch.painless.ir.ForEachSubArrayNode;
import org.elasticsearch.painless.ir.ForEachSubIterableNode;
import org.elasticsearch.painless.ir.ForLoopNode;
import org.elasticsearch.painless.ir.FunctionNode;
import org.elasticsearch.painless.ir.IfElseNode;
import org.elasticsearch.painless.ir.IfNode;
import org.elasticsearch.painless.ir.InstanceofNode;
import org.elasticsearch.painless.ir.ListInitializationNode;
import org.elasticsearch.painless.ir.ListSubShortcutNode;
import org.elasticsearch.painless.ir.MapInitializationNode;
import org.elasticsearch.painless.ir.MapSubShortcutNode;
import org.elasticsearch.painless.ir.MemberCallNode;
import org.elasticsearch.painless.ir.MemberFieldLoadNode;
import org.elasticsearch.painless.ir.MemberFieldStoreNode;
import org.elasticsearch.painless.ir.NewArrayNode;
import org.elasticsearch.painless.ir.NewObjectNode;
import org.elasticsearch.painless.ir.NullNode;
import org.elasticsearch.painless.ir.NullSafeSubNode;
import org.elasticsearch.painless.ir.ReturnNode;
import org.elasticsearch.painless.ir.StatementExpressionNode;
import org.elasticsearch.painless.ir.StaticNode;
import org.elasticsearch.painless.ir.ThrowNode;
import org.elasticsearch.painless.ir.TryNode;
import org.elasticsearch.painless.ir.TypedCaptureReferenceNode;
import org.elasticsearch.painless.ir.TypedInterfaceReferenceNode;
import org.elasticsearch.painless.ir.UnaryMathNode;
import org.elasticsearch.painless.ir.VariableNode;
import org.elasticsearch.painless.ir.WhileLoopNode;

public class IRTreeBaseVisitor<Input, Output> implements IRTreeVisitor<Input, Output> {

    @Override
    public Output visitClass(ClassNode irClassNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitFunction(FunctionNode irFunctionNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitField(FieldNode irFieldNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitBlock(BlockNode irBlockNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitIf(IfNode irIfNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitIfElse(IfElseNode irIfElseNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitWhileLoop(WhileLoopNode irWhileLoopNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitDoWhileLoop(DoWhileLoopNode irDoWhileLoopNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitForLoop(ForLoopNode irForLoopNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitForEachLoop(ForEachLoopNode irForEachLoopNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitForEachSubArrayLoop(ForEachSubArrayNode irForEachSubArrayNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitForEachSubIterableLoop(ForEachSubIterableNode irForEachSubIterableNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitDeclarationBlock(DeclarationBlockNode irDeclarationBlockNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitDeclaration(DeclarationNode irDeclarationNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitReturn(ReturnNode irReturnNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitStatementExpression(StatementExpressionNode irStatementExpressionNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitTry(TryNode irTryNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitCatch(CatchNode irCatchNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitThrow(ThrowNode irThrowNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitContinue(ContinueNode irContinueNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitBreak(BreakNode irBreakNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitAssignment(AssignmentNode irAssignmentNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitUnaryMath(UnaryMathNode irUnaryMathNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitBinaryMath(BinaryMathNode irBinaryMathNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitBoolean(BooleanNode irBoolNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitComparison(ComparisonNode irComparisonNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitCast(CastNode irCastNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitInstanceof(InstanceofNode irInstanceofNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitConditional(ConditionalNode irConditionalNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitElvis(ElvisNode irElvisNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitListInitialization(ListInitializationNode irListInitializationNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitMapInitialization(MapInitializationNode irMapInitializationNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitNewArray(NewArrayNode irNewArrayNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitNewObject(NewObjectNode irNewObjectNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitConstant(ConstantNode irConstantNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitNull(NullNode irNullNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitDefInterfaceReference(DefInterfaceReferenceNode irDefInterfaceReferenceNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitTypedInterfaceReference(TypedInterfaceReferenceNode irTypedInterfaceReferenceNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitTypeCaptureReference(TypedCaptureReferenceNode irTypedCaptureReferenceNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitStatic(StaticNode irStaticNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitVariable(VariableNode irVariableNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitNullSafeSub(NullSafeSubNode irNullSafeSubNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitDot(DotNode irDotNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitDotSubArrayLength(DotSubArrayLengthNode irDotSubArrayLengthNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitDotSubDef(DotSubDefNode irDotSubDefNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitDotSub(DotSubNode irDotSubNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitDotSubShortcut(DotSubShortcutNode irDotSubShortcutNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitListSubShortcut(ListSubShortcutNode irListSubShortcutNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitMapSubShortcut(MapSubShortcutNode irMapSubShorcutNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitMemberFieldLoad(MemberFieldLoadNode irMemberFieldLoadNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitMemberFieldStore(MemberFieldStoreNode irMemberFieldStoreNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitBrace(BraceNode irBraceNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitBraceSubDef(BraceSubDefNode irBraceSubDefNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitBraceSub(BraceSubNode irBraceSubNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitCall(CallNode irCallNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitCallSubDef(CallSubDefNode irCallSubDefNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitCallSub(CallSubNode irCallSubNode, Input input) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Output visitMemberCall(MemberCallNode irMemberCallNode, Input input) {
        throw new UnsupportedOperationException();
    }
}
