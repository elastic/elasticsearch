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

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.elasticsearch.painless.tree.utility.Operation;
import org.elasticsearch.painless.tree.utility.Variables.Shortcut;
import org.elasticsearch.painless.input.antlr.PainlessParser.AfterthoughtContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ArgumentsContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.AssignmentContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.BinaryContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.BreakContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.CastContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ConditionalContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ContinueContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.DeclContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.DeclarationContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.DecltypeContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.DeclvarContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.DoContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.EmptyContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.EmptyscopeContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ExprContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ExpressionContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ExtbraceContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ExtcallContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ExtcastContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ExtdotContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ExternalContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ExtfieldContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ExtnewContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ExtprecContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ExtstartContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ExtstringContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ExtvarContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.FalseContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ForContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.GenericContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.IdentifierContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.IfContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.InitializerContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.MultipleContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.NullContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.NumericContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.PostincContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.PrecedenceContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.PreincContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ReturnContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.SingleContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.SourceContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.StatementContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ThrowContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.TrapContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.TrueContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.TryContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.UnaryContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.WhileContext;
import org.elasticsearch.painless.tree.node.AExpression;
import org.elasticsearch.painless.tree.node.ALink;
import org.elasticsearch.painless.tree.node.ANode;
import org.elasticsearch.painless.tree.node.AStatement;
import org.elasticsearch.painless.tree.node.EBinary;
import org.elasticsearch.painless.tree.node.EBool;
import org.elasticsearch.painless.tree.node.EBoolean;
import org.elasticsearch.painless.tree.node.EExplicit;
import org.elasticsearch.painless.tree.node.EChain;
import org.elasticsearch.painless.tree.node.EComp;
import org.elasticsearch.painless.tree.node.EConditional;
import org.elasticsearch.painless.tree.node.EDecimal;
import org.elasticsearch.painless.tree.node.ENull;
import org.elasticsearch.painless.tree.node.ENumeric;
import org.elasticsearch.painless.tree.node.EUnary;
import org.elasticsearch.painless.tree.node.LBrace;
import org.elasticsearch.painless.tree.node.LCall;
import org.elasticsearch.painless.tree.node.LCast;
import org.elasticsearch.painless.tree.node.LField;
import org.elasticsearch.painless.tree.node.LNewArray;
import org.elasticsearch.painless.tree.node.LNewObj;
import org.elasticsearch.painless.tree.node.LString;
import org.elasticsearch.painless.tree.node.LVariable;
import org.elasticsearch.painless.tree.node.SBlock;
import org.elasticsearch.painless.tree.node.SBreak;
import org.elasticsearch.painless.tree.node.SContinue;
import org.elasticsearch.painless.tree.node.SDeclBlock;
import org.elasticsearch.painless.tree.node.SDeclaration;
import org.elasticsearch.painless.tree.node.SDo;
import org.elasticsearch.painless.tree.node.SExpression;
import org.elasticsearch.painless.tree.node.SFor;
import org.elasticsearch.painless.tree.node.SIfElse;
import org.elasticsearch.painless.tree.node.SReturn;
import org.elasticsearch.painless.tree.node.SSource;
import org.elasticsearch.painless.tree.node.SThrow;
import org.elasticsearch.painless.tree.node.STrap;
import org.elasticsearch.painless.tree.node.STry;
import org.elasticsearch.painless.tree.node.SWhile;

import java.util.ArrayList;
import java.util.List;

public class Walker extends PainlessParserBaseVisitor<ANode> {
    public static SSource buildPainlessTree(final String source, final Shortcut shortcut) {
        return new Walker(source, shortcut).source;
    }

    private final Shortcut shortcut;
    private final SSource source;

    private Walker(final String source, final Shortcut shortcut) {
        this.shortcut = shortcut;
        this.source = (SSource)visit(buildAntlrTree(source));
    }

    private SourceContext buildAntlrTree(final String source) {
        final ANTLRInputStream stream = new ANTLRInputStream(source);
        final PainlessLexer lexer = new ErrorHandlingLexer(stream);
        final PainlessParser parser = new PainlessParser(new CommonTokenStream(lexer));
        final ParserErrorStrategy strategy = new ParserErrorStrategy();

        lexer.removeErrorListeners();
        parser.removeErrorListeners();
        parser.setErrorHandler(strategy);

        return parser.source();
    }

    private String location(final ParserRuleContext ctx) {
        return "[ " + ctx.getStart().getLine() + " : " + ctx.getStart().getCharPositionInLine() + " ]";
    }

    @Override
    public ANode visitSource(final SourceContext ctx) {
        final List<AStatement> statements = new ArrayList<>();

        for (final StatementContext statement : ctx.statement()) {
            statements.add((AStatement)visit(statement));
        }

        return new SSource(location(ctx), statements);
    }

    @Override
    public ANode visitIf(final IfContext ctx) {
        final AExpression condition = (AExpression)visit(ctx.expression());
        final AStatement ifblock = (AStatement)visit(ctx.block(0));
        final AStatement elseblock = ctx.block(1) == null ? null : (AStatement)visit(ctx.block(1));

        return new SIfElse(location(ctx), condition, ifblock, elseblock);
    }

    @Override
    public ANode visitWhile(final WhileContext ctx) {
        final AExpression condition = (AExpression)visit(ctx.expression());
        final AStatement block = ctx.block() == null ? null : (AStatement)visit(ctx.block());

        shortcut.loop = true;

        return new SWhile(location(ctx), condition, block);
    }

    @Override
    public ANode visitDo(final DoContext ctx) {
        final AStatement block = ctx.block() == null ? null : (AStatement)visit(ctx.block());
        final AExpression condition = (AExpression)visit(ctx.expression());

        shortcut.loop = true;

        return new SDo(location(ctx), block, condition);
    }

    @Override
    public ANode visitFor(final ForContext ctx) {
        final ANode intializer = ctx.initializer() == null ? null : visit(ctx.initializer());
        final AExpression condition = ctx.expression() == null ? null : (AExpression)visit(ctx.expression());
        final AExpression afterthought = ctx.afterthought() == null ? null : (AExpression)visit(ctx.afterthought());
        final AStatement block = ctx.block() == null ? null : (AStatement)visit(ctx.block());

        shortcut.loop = true;

        return new SFor(location(ctx), intializer, condition, afterthought, block);
    }

    @Override
    public ANode visitDecl(final DeclContext ctx) {
        return visit(ctx.declaration());
    }

    @Override
    public ANode visitContinue(final ContinueContext ctx) {
        return new SContinue(location(ctx));
    }

    @Override
    public ANode visitBreak(final BreakContext ctx) {
        return new SBreak(location(ctx));
    }

    @Override
    public ANode visitReturn(final ReturnContext ctx) {
        final AExpression expression = (AExpression)visit(ctx.expression());

        return new SReturn(location(ctx), expression);
    }

    @Override
    public ANode visitTry(final TryContext ctx) {
        final AStatement block = (AStatement)visit(ctx.block());
        final List<STrap> traps = new ArrayList<>();

        for (final TrapContext trap : ctx.trap()) {
            traps.add((STrap)visit(trap));
        }

        return new STry(location(ctx), block, traps);
    }

    @Override
    public ANode visitThrow(final ThrowContext ctx) {
        final AExpression expression = (AExpression)visit(ctx.expression());

        return new SThrow(location(ctx), expression);
    }

    @Override
    public ANode visitExpr(final ExprContext ctx) {
        final AExpression expression = (AExpression)visit(ctx.expression());

        return new SExpression(location(ctx), expression);
    }

    @Override
    public ANode visitMultiple(final MultipleContext ctx) {
        final List<AStatement> statements = new ArrayList<>();

        for (final StatementContext statement : ctx.statement()) {
            statements.add((AStatement)visit(statement));
        }

        return new SBlock(location(ctx), statements);
    }

    @Override
    public ANode visitSingle(final SingleContext ctx) {
        final List<AStatement> statements = new ArrayList<>();
        statements.add((AStatement)visit(ctx.statement()));

        return new SBlock(location(ctx), statements);
    }

    @Override
    public ANode visitEmpty(final EmptyContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public ANode visitEmptyscope(final EmptyscopeContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public ANode visitInitializer(final InitializerContext ctx) {
        if (ctx.declaration() != null) {
            return visit(ctx.declaration());
        } else if (ctx.expression() != null) {
            return visit(ctx.expression());
        }

        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public ANode visitAfterthought(final AfterthoughtContext ctx) {
        return visit(ctx.expression());
    }

    @Override
    public ANode visitDeclaration(final DeclarationContext ctx) {
        final String type = ctx.decltype().getText();
        final List<SDeclaration> declarations = new ArrayList<>();

        for (final DeclvarContext declvar : ctx.declvar()) {
            final String name = declvar.identifier().getText();
            final AExpression expression = declvar.expression() == null ? null : (AExpression)visit(declvar.expression());
            declarations.add(new SDeclaration(location(ctx), type, name, expression));
        }

        return new SDeclBlock(location(ctx), declarations);
    }

    @Override
    public ANode visitDecltype(final DecltypeContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public ANode visitDeclvar(final DeclvarContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public ANode visitTrap(final TrapContext ctx) {
        final String type = ctx.identifier(0).getText();
        final String name = ctx.identifier(1).getText();
        final AStatement block = ctx.block() == null ? null : (AStatement)visit(ctx.block());

        return new STrap(location(ctx), type, name, block);
    }

    @Override
    public ANode visitIdentifier(final IdentifierContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public ANode visitGeneric(final GenericContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public ANode visitPrecedence(final PrecedenceContext ctx) {
        return visit(ctx.expression());
    }

    @Override
    public ANode visitNumeric(final NumericContext ctx) {
        final boolean negate = ctx.parent instanceof UnaryContext && ((UnaryContext)ctx.parent).SUB() != null;

        if (ctx.DECIMAL() != null) {
            return new EDecimal(location(ctx), (negate ? "-" : "") + ctx.DECIMAL().getText());
        } else if (ctx.HEX() != null) {
            return new ENumeric(location(ctx), (negate ? "-" : "") + ctx.HEX().getText().substring(2), 16);
        } else if (ctx.INTEGER() != null) {
            return new ENumeric(location(ctx), (negate ? "-" : "") + ctx.INTEGER().getText(), 10);
        } else if (ctx.OCTAL() != null) {
            return new ENumeric(location(ctx), (negate ? "-" : "") + ctx.OCTAL().getText().substring(1), 8);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }
    }

    @Override
    public ANode visitTrue(final TrueContext ctx) {
        return new EBoolean(location(ctx), true);
    }

    @Override
    public ANode visitFalse(FalseContext ctx) {
        return new EBoolean(location(ctx), false);
    }

    @Override
    public ANode visitNull(final NullContext ctx) {
        return new ENull(location(ctx));
    }

    @Override
    public ANode visitPostinc(final PostincContext ctx) {
        final List<ALink> links = new ArrayList<>();
        final Operation operation;

        visitExtstart(ctx.extstart(), links);

        if (ctx.INCR() != null) {
            operation = Operation.INCR;
        } else if (ctx.DECR() != null) {
            operation = Operation.DECR;
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        return new EChain(location(ctx), links, false, true, operation, null);
    }

    @Override
    public ANode visitPreinc(final PreincContext ctx) {
        final List<ALink> links = new ArrayList<>();
        final Operation operation;

        visitExtstart(ctx.extstart(), links);

        if (ctx.INCR() != null) {
            operation = Operation.INCR;
        } else if (ctx.DECR() != null) {
            operation = Operation.DECR;
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        return new EChain(location(ctx), links, true, false, operation, null);
    }

    @Override
    public ANode visitExternal(final ExternalContext ctx) {
        final List<ALink> links = new ArrayList<>();

        visitExtstart(ctx.extstart(), links);

        return new EChain(location(ctx), links, false, false, null, null);
    }

    @Override
    public ANode visitUnary(final UnaryContext ctx) {
        if (ctx.SUB() != null && ctx.expression() instanceof NumericContext) {
            return visit(ctx.expression());
        } else {
            final Operation operation;

            if (ctx.BOOLNOT() != null) {
                operation = Operation.NOT;
            } else if (ctx.BWNOT() != null) {
                operation = Operation.BWNOT;
            } else if (ctx.ADD() != null) {
                operation = Operation.ADD;
            } else if (ctx.SUB() != null) {
                operation = Operation.SUB;
            } else {
                throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
            }

            return new EUnary(location(ctx), operation, (AExpression)visit(ctx.expression()));
        }
    }

    @Override
    public ANode visitCast(final CastContext ctx) {
        return new EExplicit(location(ctx), ctx.decltype().getText(), (AExpression)visit(ctx.expression()));
    }

    @Override
    public ANode visitBinary(final BinaryContext ctx) {
        final AExpression left = (AExpression)visit(ctx.expression(0));
        final AExpression right = (AExpression)visit(ctx.expression(1));
        final Operation operation;

        if (ctx.MUL() != null) {
            operation = Operation.MUL;
        } else if (ctx.DIV() != null) {
            operation = Operation.DIV;
        } else if (ctx.REM() != null) {
            operation = Operation.REM;
        } else if (ctx.ADD() != null) {
            operation = Operation.ADD;
        } else if (ctx.SUB() != null) {
            operation = Operation.SUB;
        } else if (ctx.LSH() != null) {
            operation = Operation.LSH;
        } else if (ctx.RSH() != null) {
            operation = Operation.RSH;
        } else if (ctx.USH() != null) {
            operation = Operation.USH;
        } else if (ctx.BWAND() != null) {
            operation = Operation.BWAND;
        } else if (ctx.XOR() != null) {
            operation = Operation.XOR;
        } else if (ctx.BWOR() != null) {
            operation = Operation.BWOR;
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        return new EBinary(location(ctx), operation, left, right);
    }

    @Override
    public ANode visitComp(PainlessParser.CompContext ctx) {
        final AExpression left = (AExpression)visit(ctx.expression(0));
        final AExpression right = (AExpression)visit(ctx.expression(1));
        final Operation operation;

        if (ctx.LT() != null) {
            operation = Operation.LT;
        } else if (ctx.LTE() != null) {
            operation = Operation.LTE;
        } else if (ctx.GT() != null) {
            operation = Operation.GT;
        } else if (ctx.GTE() != null) {
            operation = Operation.GTE;
        } else if (ctx.EQ() != null) {
            operation = Operation.EQ;
        } else if (ctx.EQR() != null) {
            operation = Operation.EQR;
        } else if (ctx.NE() != null) {
            operation = Operation.NE;
        } else if (ctx.NER() != null) {
            operation = Operation.NER;
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        return new EComp(location(ctx), operation, left, right);
    }

    @Override
    public ANode visitBool(PainlessParser.BoolContext ctx) {
        final AExpression left = (AExpression)visit(ctx.expression(0));
        final AExpression right = (AExpression)visit(ctx.expression(1));
        final Operation operation;

        if (ctx.BOOLAND() != null) {
            operation = Operation.AND;
        } else if (ctx.BOOLOR() != null) {
            operation = Operation.OR;
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        return new EBool(location(ctx), operation, left, right);
    }


    @Override
    public ANode visitConditional(final ConditionalContext ctx) {
        final AExpression condition = (AExpression)visit(ctx.expression(0));
        final AExpression left = (AExpression)visit(ctx.expression(1));
        final AExpression right = (AExpression)visit(ctx.expression(2));

        return new EConditional(location(ctx), condition, left, right);
    }

    @Override
    public ANode visitAssignment(final AssignmentContext ctx) {
        final List<ALink> links = new ArrayList<>();
        final Operation operation;

        visitExtstart(ctx.extstart(), links);

        if (ctx.AMUL() != null) {
            operation = Operation.MUL;
        } else if (ctx.ADIV() != null) {
            operation = Operation.DIV;
        } else if (ctx.AREM() != null) {
            operation = Operation.REM;
        } else if (ctx.AADD() != null) {
            operation = Operation.ADD;
        } else if (ctx.ASUB() != null) {
            operation = Operation.SUB;
        } else if (ctx.ALSH() != null) {
            operation = Operation.LSH;
        } else if (ctx.ARSH() != null) {
            operation = Operation.RSH;
        } else if (ctx.AUSH() != null) {
            operation = Operation.USH;
        } else if (ctx.AAND() != null) {
            operation = Operation.BWAND;
        } else if (ctx.AXOR() != null) {
            operation = Operation.XOR;
        } else if (ctx.AOR() != null) {
            operation = Operation.BWOR;
        } else {
            operation = null;
        }

        return new EChain(location(ctx), links, false, false, operation, (AExpression)visit(ctx.expression()));
    }

    private void visitExtstart(final ExtstartContext ctx, final List<ALink> links) {
        if (ctx.extprec() != null) {
            visitExtprec(ctx.extprec(), links);
        } else if (ctx.extcast() != null) {
            visitExtcast(ctx.extcast(), links);
        } else if (ctx.extvar() != null) {
            visitExtvar(ctx.extvar(), links);
        } else if (ctx.extnew() != null) {
            visitExtnew(ctx.extnew(), links);
        } else if (ctx.extstring() != null) {
            visitExtstring(ctx.extstring(), links);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }
    }

    @Override
    public ANode visitExtstart(final ExtstartContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtprec(final ExtprecContext ctx, final List<ALink> links) {
        if (ctx.extprec() != null) {
            visitExtprec(ctx.extprec(), links);
        } else if (ctx.extcast() != null) {
            visitExtcast(ctx.extcast(), links);
        } else if (ctx.extvar() != null) {
            visitExtvar(ctx.extvar(), links);
        } else if (ctx.extnew() != null) {
            visitExtnew(ctx.extnew(), links);
        } else if (ctx.extstring() != null) {
            visitExtstring(ctx.extstring(), links);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        if (ctx.extbrace() != null) {
            visitExtbrace(ctx.extbrace(), links);
        } else if (ctx.extdot() != null) {
            visitExtdot(ctx.extdot(), links);
        }
    }

    @Override
    public ANode visitExtprec(final ExtprecContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtcast(final ExtcastContext ctx, final List<ALink> links) {
        if (ctx.extprec() != null) {
            visitExtprec(ctx.extprec(), links);
        } else if (ctx.extcast() != null) {
            visitExtcast(ctx.extcast(), links);
        } else if (ctx.extvar() != null) {
            visitExtvar(ctx.extvar(), links);
        } else if (ctx.extnew() != null) {
            visitExtnew(ctx.extnew(), links);
        } else if (ctx.extstring() != null) {
            visitExtstring(ctx.extstring(), links);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        links.add(new LCast(location(ctx), ctx.decltype().getText()));
    }

    @Override
    public ANode visitExtcast(final ExtcastContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtbrace(final ExtbraceContext ctx, final List<ALink> links) {
        links.add(new LBrace(location(ctx), (AExpression)visit(ctx.expression())));

        if (ctx.extbrace() != null) {
            visitExtbrace(ctx.extbrace(), links);
        } else if (ctx.extdot() != null) {
            visitExtdot(ctx.extdot(), links);
        }
    }

    @Override
    public ANode visitExtbrace(final ExtbraceContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtdot(final ExtdotContext ctx, final List<ALink> links) {
        if (ctx.extcall() != null) {
            visitExtcall(ctx.extcall(), links);
        } else if (ctx.extfield() != null) {
            visitExtfield(ctx.extfield(), links);
        }
    }

    @Override
    public ANode visitExtdot(final ExtdotContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtcall(final ExtcallContext ctx, final List<ALink> links) {
        final List<AExpression> arguments = new ArrayList<>();

        for (final ExpressionContext expression : ctx.arguments().expression()) {
            arguments.add((AExpression)visit(expression));
        }

        links.add(new LCall(location(ctx), ctx.EXTID().getText(), arguments));

        if (ctx.extbrace() != null) {
            visitExtbrace(ctx.extbrace(), links);
        } else if (ctx.extdot() != null) {
            visitExtdot(ctx.extdot(), links);
        }
    }

    @Override
    public ANode visitExtcall(final ExtcallContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtvar(final ExtvarContext ctx, final List<ALink> links) {
        final String name = ctx.identifier().getText();

        if ("score".equals(name)) {
            shortcut.score = true;
        } else if ("doc".equals(name)) {
            shortcut.doc = true;
        }

        links.add(new LVariable(location(ctx), name));

        if (ctx.extbrace() != null) {
            visitExtbrace(ctx.extbrace(), links);
        } else if (ctx.extdot() != null) {
            visitExtdot(ctx.extdot(), links);
        }
    }

    @Override
    public ANode visitExtvar(final ExtvarContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtfield(final ExtfieldContext ctx, final List<ALink> links) {
        final String value;

        if (ctx.EXTID() != null) {
            value = ctx.EXTID().getText();
        } else if (ctx.EXTINTEGER() != null) {
            value = ctx.EXTINTEGER().getText();
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        links.add(new LField(location(ctx), value));

        if (ctx.extbrace() != null) {
            visitExtbrace(ctx.extbrace(), links);
        } else if (ctx.extdot() != null) {
            visitExtdot(ctx.extdot(), links);
        }
    }

    @Override
    public ANode visitExtfield(final ExtfieldContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtnew(final ExtnewContext ctx, final List<ALink> links) {
        final List<AExpression> arguments = new ArrayList<>();

        if (ctx.arguments() != null) {
            for (final ExpressionContext expression : ctx.arguments().expression()) {
                arguments.add((AExpression)visit(expression));
            }

            links.add(new LNewObj(location(ctx), ctx.identifier().getText(), arguments));
        } else if (ctx.expression().size() > 0) {
            for (final ExpressionContext expression : ctx.expression()) {
                arguments.add((AExpression)visit(expression));
            }

            links.add(new LNewArray(location(ctx), ctx.identifier().getText(), arguments));
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        if (ctx.extdot() != null) {
            visitExtdot(ctx.extdot(), links);
        }
    }

    @Override
    public ANode visitExtnew(final ExtnewContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtstring(final ExtstringContext ctx, final List<ALink> links) {
        links.add(new LString(location(ctx), ctx.STRING().getText().substring(1, ctx.STRING().getText().length() - 1)));

        if (ctx.extbrace() != null) {
            visitExtbrace(ctx.extbrace(), links);
        } else if (ctx.extdot() != null) {
            visitExtdot(ctx.extdot(), links);
        }
    }

    @Override
    public ANode visitExtstring(final ExtstringContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public ANode visitArguments(final ArgumentsContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }
}
