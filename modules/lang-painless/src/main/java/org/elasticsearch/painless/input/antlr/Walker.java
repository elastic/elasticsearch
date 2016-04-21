/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless.input.antlr;

import org.antlr.v4.runtime.ParserRuleContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.SourceContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.StatementContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.IfContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.WhileContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.DoContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ForContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.DeclContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ContinueContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.BreakContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ReturnContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.TryContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ThrowContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.TrapContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ExprContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.MultipleContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.SingleContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.EmptyContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.InitializerContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.AfterthoughtContext;
import org.elasticsearch.painless.tree.node.Node;

import static org.elasticsearch.painless.tree.node.Type.BLOCK;
import static org.elasticsearch.painless.tree.node.Type.BREAK;
import static org.elasticsearch.painless.tree.node.Type.CONTINUE;
import static org.elasticsearch.painless.tree.node.Type.EXPRESSION;
import static org.elasticsearch.painless.tree.node.Type.RETURN;
import static org.elasticsearch.painless.tree.node.Type.SOURCE;
import static org.elasticsearch.painless.tree.node.Type.IF;
import static org.elasticsearch.painless.tree.node.Type.THROW;
import static org.elasticsearch.painless.tree.node.Type.TRY;
import static org.elasticsearch.painless.tree.node.Type.WHILE;
import static org.elasticsearch.painless.tree.node.Type.DO;
import static org.elasticsearch.painless.tree.node.Type.FOR;

public class Walker extends PainlessParserBaseVisitor<Node> {
    public static Node buildPainlessTree(final SourceContext source) {
        return new Walker(source).source;
    }

    private Node source;

    private Walker(final SourceContext source) {
        this.source = visit(source);
    }

    private String location(final ParserRuleContext ctx) {
        return "[ " + ctx.getStart().getLine() + " : " + ctx.getStart().getCharPositionInLine() + " ]";
    }

    @Override
    public Node visitSource(final SourceContext ctx) {
        final Node node = new Node(location(ctx), SOURCE);

        for (final StatementContext statement : ctx.statement()) {
            source.children.add(visit(statement));
        }

        return node;
    }

    @Override
    public Node visitIf(final IfContext ctx) {
        final Node node = new Node(location(ctx), IF);

        node.children.add(visit(ctx.expression()));
        node.children.add(visit(ctx.block(0)));
        node.children.add(ctx.block(1) == null ? null : visit(ctx.block(1)));

        return node;
    }

    @Override
    public Node visitWhile(final WhileContext ctx) {
        final Node node = new Node(location(ctx), WHILE);

        node.children.add(visit(ctx.expression()));
        node.children.add(ctx.block() == null ? null : visit(ctx.block()));

        return node;
    }

    @Override
    public Node visitDo(final DoContext ctx) {
        final Node node = new Node(location(ctx), DO);

        node.children.add(visit(ctx.block()));
        node.children.add(visit(ctx.expression()));

        return node;
    }

    @Override
    public Node visitFor(final ForContext ctx) {
        final Node node = new Node(location(ctx), FOR);

        node.children.add(ctx.initializer() == null ? null : visit(ctx.initializer()));
        node.children.add(ctx.expression() == null ? null : visit(ctx.expression()));
        node.children.add(ctx.afterthought() == null ? null : visit(ctx.afterthought()));
        node.children.add(ctx.block() == null ? null : visit(ctx.block()));

        return node;
    }

    @Override
    public Node visitDecl(final DeclContext ctx) {
        return visit(ctx.declaration());
    }

    @Override
    public Node visitContinue(final ContinueContext ctx) {
        return new Node(location(ctx), CONTINUE);
    }

    @Override
    public Node visitBreak(final BreakContext ctx) {
        return new Node(location(ctx), BREAK);
    }

    @Override
    public Node visitReturn(final ReturnContext ctx) {
        final Node node = new Node(location(ctx), RETURN);

        node.children.add(visit(ctx.expression()));

        return node;
    }

    @Override
    public Node visitTry(final TryContext ctx) {
        final Node node = new Node(location(ctx), TRY);

        node.children.add(visit(ctx.block());

        for (final TrapContext trap : ctx.trap()) {
            node.children.add(visit(trap));
        }

        return node;
    }

    @Override
    public Node visitThrow(final ThrowContext ctx) {
        final Node node = new Node(location(ctx), THROW);

        node.children.add(visit(ctx.expression()));

        return node;
    }

    @Override
    public Node visitExpr(final ExprContext ctx) {
        final Node node = new Node(location(ctx), EXPRESSION);

        node.children.add(visit(ctx.expression()));

        return node;
    }

    @Override
    public Node visitMultiple(final MultipleContext ctx) {
        final Node node = new Node(location(ctx), BLOCK);

        for (final StatementContext statement : ctx.statement()) {
            node.children.add(visit(statement));
        }

        return node;
    }

    @Override
    public Node visitSingle(final SingleContext ctx) {
        final Node node = new Node(location(ctx), BLOCK);

        node.children.add(visit(ctx.statement()));

        return node;
    }

    @Override
    public Node visitEmpty(final EmptyContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public Node visitEmptyscope(PainlessParser.EmptyscopeContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public Node visitInitializer(final InitializerContext ctx) {
        if (ctx.declaration() != null) {
            return visit(ctx.declaration());
        } else if (ctx.expression() != null) {
            return visit(ctx.expression());
        }

        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public Node visitAfterthought(final AfterthoughtContext ctx) {
        return
    }

    @Override
    public Node visitDeclaration(PainlessParser.DeclarationContext ctx) {
        return null;
    }

    @Override
    public Node visitDecltype(PainlessParser.DecltypeContext ctx) {
        return null;
    }

    @Override
    public Node visitDeclvar(PainlessParser.DeclvarContext ctx) {
        return null;
    }

    @Override
    public Node visitTrap(PainlessParser.TrapContext ctx) {
        return null;
    }

    @Override
    public Node visitIdentifier(PainlessParser.IdentifierContext ctx) {
        return null;
    }

    @Override
    public Node visitGeneric(PainlessParser.GenericContext ctx) {
        return null;
    }

    @Override
    public Node visitConditional(PainlessParser.ConditionalContext ctx) {
        return null;
    }

    @Override
    public Node visitAssignment(PainlessParser.AssignmentContext ctx) {
        return null;
    }

    @Override
    public Node visitFalse(PainlessParser.FalseContext ctx) {
        return null;
    }

    @Override
    public Node visitNumeric(PainlessParser.NumericContext ctx) {
        return null;
    }

    @Override
    public Node visitUnary(PainlessParser.UnaryContext ctx) {
        return null;
    }

    @Override
    public Node visitPrecedence(PainlessParser.PrecedenceContext ctx) {
        return null;
    }

    @Override
    public Node visitPreinc(PainlessParser.PreincContext ctx) {
        return null;
    }

    @Override
    public Node visitPostinc(PainlessParser.PostincContext ctx) {
        return null;
    }

    @Override
    public Node visitCast(PainlessParser.CastContext ctx) {
        return null;
    }

    @Override
    public Node visitExternal(PainlessParser.ExternalContext ctx) {
        return null;
    }

    @Override
    public Node visitNull(PainlessParser.NullContext ctx) {
        return null;
    }

    @Override
    public Node visitBinary(PainlessParser.BinaryContext ctx) {
        return null;
    }

    @Override
    public Node visitChar(PainlessParser.CharContext ctx) {
        return null;
    }

    @Override
    public Node visitTrue(PainlessParser.TrueContext ctx) {
        return null;
    }

    @Override
    public Node visitExtstart(PainlessParser.ExtstartContext ctx) {
        return null;
    }

    @Override
    public Node visitExtprec(PainlessParser.ExtprecContext ctx) {
        return null;
    }

    @Override
    public Node visitExtcast(PainlessParser.ExtcastContext ctx) {
        return null;
    }

    @Override
    public Node visitExtbrace(PainlessParser.ExtbraceContext ctx) {
        return null;
    }

    @Override
    public Node visitExtdot(PainlessParser.ExtdotContext ctx) {
        return null;
    }

    @Override
    public Node visitExtcall(PainlessParser.ExtcallContext ctx) {
        return null;
    }

    @Override
    public Node visitExtvar(PainlessParser.ExtvarContext ctx) {
        return null;
    }

    @Override
    public Node visitExtfield(PainlessParser.ExtfieldContext ctx) {
        return null;
    }

    @Override
    public Node visitExtnew(PainlessParser.ExtnewContext ctx) {
        return null;
    }

    @Override
    public Node visitExtstring(PainlessParser.ExtstringContext ctx) {
        return null;
    }

    @Override
    public Node visitArguments(PainlessParser.ArgumentsContext ctx) {
        return null;
    }
}
