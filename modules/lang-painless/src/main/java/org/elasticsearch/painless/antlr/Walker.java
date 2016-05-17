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

package org.elasticsearch.painless.antlr;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.Variables.Reserved;
import org.elasticsearch.painless.antlr.PainlessParser.AfterthoughtContext;
import org.elasticsearch.painless.antlr.PainlessParser.ArgumentsContext;
import org.elasticsearch.painless.antlr.PainlessParser.AssignmentContext;
import org.elasticsearch.painless.antlr.PainlessParser.BinaryContext;
import org.elasticsearch.painless.antlr.PainlessParser.BreakContext;
import org.elasticsearch.painless.antlr.PainlessParser.CastContext;
import org.elasticsearch.painless.antlr.PainlessParser.ConditionalContext;
import org.elasticsearch.painless.antlr.PainlessParser.ContinueContext;
import org.elasticsearch.painless.antlr.PainlessParser.DeclContext;
import org.elasticsearch.painless.antlr.PainlessParser.DeclarationContext;
import org.elasticsearch.painless.antlr.PainlessParser.DecltypeContext;
import org.elasticsearch.painless.antlr.PainlessParser.DeclvarContext;
import org.elasticsearch.painless.antlr.PainlessParser.DoContext;
import org.elasticsearch.painless.antlr.PainlessParser.EmptyContext;
import org.elasticsearch.painless.antlr.PainlessParser.EmptyscopeContext;
import org.elasticsearch.painless.antlr.PainlessParser.ExprContext;
import org.elasticsearch.painless.antlr.PainlessParser.ExpressionContext;
import org.elasticsearch.painless.antlr.PainlessParser.LinkbraceContext;
import org.elasticsearch.painless.antlr.PainlessParser.LinkcallContext;
import org.elasticsearch.painless.antlr.PainlessParser.LinkcastContext;
import org.elasticsearch.painless.antlr.PainlessParser.LinkdotContext;
import org.elasticsearch.painless.antlr.PainlessParser.ReadContext;
import org.elasticsearch.painless.antlr.PainlessParser.LinkfieldContext;
import org.elasticsearch.painless.antlr.PainlessParser.LinknewContext;
import org.elasticsearch.painless.antlr.PainlessParser.LinkprecContext;
import org.elasticsearch.painless.antlr.PainlessParser.ChainContext;
import org.elasticsearch.painless.antlr.PainlessParser.LinkstringContext;
import org.elasticsearch.painless.antlr.PainlessParser.LinkvarContext;
import org.elasticsearch.painless.antlr.PainlessParser.FalseContext;
import org.elasticsearch.painless.antlr.PainlessParser.ForContext;
import org.elasticsearch.painless.antlr.PainlessParser.GenericContext;
import org.elasticsearch.painless.antlr.PainlessParser.IdentifierContext;
import org.elasticsearch.painless.antlr.PainlessParser.IfContext;
import org.elasticsearch.painless.antlr.PainlessParser.IfelseContext;
import org.elasticsearch.painless.antlr.PainlessParser.InitializerContext;
import org.elasticsearch.painless.antlr.PainlessParser.MultipleContext;
import org.elasticsearch.painless.antlr.PainlessParser.NullContext;
import org.elasticsearch.painless.antlr.PainlessParser.NumericContext;
import org.elasticsearch.painless.antlr.PainlessParser.PostincContext;
import org.elasticsearch.painless.antlr.PainlessParser.PrecedenceContext;
import org.elasticsearch.painless.antlr.PainlessParser.PreincContext;
import org.elasticsearch.painless.antlr.PainlessParser.ReturnContext;
import org.elasticsearch.painless.antlr.PainlessParser.SingleContext;
import org.elasticsearch.painless.antlr.PainlessParser.SourceContext;
import org.elasticsearch.painless.antlr.PainlessParser.StatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.ThrowContext;
import org.elasticsearch.painless.antlr.PainlessParser.TrapContext;
import org.elasticsearch.painless.antlr.PainlessParser.TrueContext;
import org.elasticsearch.painless.antlr.PainlessParser.TryContext;
import org.elasticsearch.painless.antlr.PainlessParser.UnaryContext;
import org.elasticsearch.painless.antlr.PainlessParser.WhileContext;
import org.elasticsearch.painless.node.AExpression;
import org.elasticsearch.painless.node.ALink;
import org.elasticsearch.painless.node.ANode;
import org.elasticsearch.painless.node.AStatement;
import org.elasticsearch.painless.node.EBinary;
import org.elasticsearch.painless.node.EBool;
import org.elasticsearch.painless.node.EBoolean;
import org.elasticsearch.painless.node.EExplicit;
import org.elasticsearch.painless.node.EChain;
import org.elasticsearch.painless.node.EComp;
import org.elasticsearch.painless.node.EConditional;
import org.elasticsearch.painless.node.EDecimal;
import org.elasticsearch.painless.node.ENull;
import org.elasticsearch.painless.node.ENumeric;
import org.elasticsearch.painless.node.EUnary;
import org.elasticsearch.painless.node.LBrace;
import org.elasticsearch.painless.node.LCall;
import org.elasticsearch.painless.node.LCast;
import org.elasticsearch.painless.node.LField;
import org.elasticsearch.painless.node.LNewArray;
import org.elasticsearch.painless.node.LNewObj;
import org.elasticsearch.painless.node.LString;
import org.elasticsearch.painless.node.LVariable;
import org.elasticsearch.painless.node.SBlock;
import org.elasticsearch.painless.node.SBreak;
import org.elasticsearch.painless.node.SContinue;
import org.elasticsearch.painless.node.SDeclBlock;
import org.elasticsearch.painless.node.SDeclaration;
import org.elasticsearch.painless.node.SDo;
import org.elasticsearch.painless.node.SExpression;
import org.elasticsearch.painless.node.SFor;
import org.elasticsearch.painless.node.SIfElse;
import org.elasticsearch.painless.node.SReturn;
import org.elasticsearch.painless.node.SSource;
import org.elasticsearch.painless.node.SThrow;
import org.elasticsearch.painless.node.STrap;
import org.elasticsearch.painless.node.STry;
import org.elasticsearch.painless.node.SWhile;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts the ANTLR tree to a Painless tree.
 */
public final class Walker extends PainlessParserBaseVisitor<ANode> {

    public static SSource buildPainlessTree(final String source, final Reserved reserved) {
        return new Walker(source, reserved).source;
    }

    private final Reserved reserved;
    private final SSource source;

    private Walker(final String source, final Reserved reserved) {
        this.reserved = reserved;
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

    private int line(final ParserRuleContext ctx) {
        return ctx.getStart().getLine();
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

        return new SSource(line(ctx), location(ctx), statements);
    }

    @Override
    public ANode visitIfelse(final IfelseContext ctx) {
        final AExpression condition = (AExpression)visit(ctx.expression());
        final AStatement ifblock = (AStatement)visit(ctx.block(0));
        final AStatement elseblock = (AStatement)visit(ctx.block(1));

        return new SIfElse(line(ctx), location(ctx), condition, ifblock, elseblock);
    }

    @Override
    public ANode visitIf(final IfContext ctx) {
        final AExpression condition = (AExpression)visit(ctx.expression());
        final AStatement ifblock = (AStatement)visit(ctx.block());

        return new SIfElse(line(ctx), location(ctx), condition, ifblock, null);
    }

    @Override
    public ANode visitWhile(final WhileContext ctx) {
        final AExpression condition = (AExpression)visit(ctx.expression());
        final AStatement block = ctx.block() == null ? null : (AStatement)visit(ctx.block());

        reserved.usesLoop();

        return new SWhile(line(ctx), location(ctx), condition, block);
    }

    @Override
    public ANode visitDo(final DoContext ctx) {
        final AStatement block = ctx.block() == null ? null : (AStatement)visit(ctx.block());
        final AExpression condition = (AExpression)visit(ctx.expression());

        reserved.usesLoop();

        return new SDo(line(ctx), location(ctx), block, condition);
    }

    @Override
    public ANode visitFor(final ForContext ctx) {
        final ANode intializer = ctx.initializer() == null ? null : visit(ctx.initializer());
        final AExpression condition = ctx.expression() == null ? null : (AExpression)visit(ctx.expression());
        final AExpression afterthought = ctx.afterthought() == null ? null : (AExpression)visit(ctx.afterthought());
        final AStatement block = ctx.block() == null ? null : (AStatement)visit(ctx.block());

        reserved.usesLoop();

        return new SFor(line(ctx), location(ctx), intializer, condition, afterthought, block);
    }

    @Override
    public ANode visitDecl(final DeclContext ctx) {
        return visit(ctx.declaration());
    }

    @Override
    public ANode visitContinue(final ContinueContext ctx) {
        return new SContinue(line(ctx), location(ctx));
    }

    @Override
    public ANode visitBreak(final BreakContext ctx) {
        return new SBreak(line(ctx), location(ctx));
    }

    @Override
    public ANode visitReturn(final ReturnContext ctx) {
        final AExpression expression = (AExpression)visit(ctx.expression());

        return new SReturn(line(ctx), location(ctx), expression);
    }

    @Override
    public ANode visitTry(final TryContext ctx) {
        final AStatement block = (AStatement)visit(ctx.block());
        final List<STrap> traps = new ArrayList<>();

        for (final TrapContext trap : ctx.trap()) {
            traps.add((STrap)visit(trap));
        }

        return new STry(line(ctx), location(ctx), block, traps);
    }

    @Override
    public ANode visitThrow(final ThrowContext ctx) {
        final AExpression expression = (AExpression)visit(ctx.expression());

        return new SThrow(line(ctx), location(ctx), expression);
    }

    @Override
    public ANode visitExpr(final ExprContext ctx) {
        final AExpression expression = (AExpression)visit(ctx.expression());

        return new SExpression(line(ctx), location(ctx), expression);
    }

    @Override
    public ANode visitMultiple(final MultipleContext ctx) {
        final List<AStatement> statements = new ArrayList<>();

        for (final StatementContext statement : ctx.statement()) {
            statements.add((AStatement)visit(statement));
        }

        return new SBlock(line(ctx), location(ctx), statements);
    }

    @Override
    public ANode visitSingle(final SingleContext ctx) {
        final List<AStatement> statements = new ArrayList<>();
        statements.add((AStatement)visit(ctx.statement()));

        return new SBlock(line(ctx), location(ctx), statements);
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
            declarations.add(new SDeclaration(line(ctx), location(ctx), type, name, expression));
        }

        return new SDeclBlock(line(ctx), location(ctx), declarations);
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

        return new STrap(line(ctx), location(ctx), type, name, block);
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
            return new EDecimal(line(ctx), location(ctx), (negate ? "-" : "") + ctx.DECIMAL().getText());
        } else if (ctx.HEX() != null) {
            return new ENumeric(line(ctx), location(ctx), (negate ? "-" : "") + ctx.HEX().getText().substring(2), 16);
        } else if (ctx.INTEGER() != null) {
            return new ENumeric(line(ctx), location(ctx), (negate ? "-" : "") + ctx.INTEGER().getText(), 10);
        } else if (ctx.OCTAL() != null) {
            return new ENumeric(line(ctx), location(ctx), (negate ? "-" : "") + ctx.OCTAL().getText().substring(1), 8);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }
    }

    @Override
    public ANode visitTrue(final TrueContext ctx) {
        return new EBoolean(line(ctx), location(ctx), true);
    }

    @Override
    public ANode visitFalse(FalseContext ctx) {
        return new EBoolean(line(ctx), location(ctx), false);
    }

    @Override
    public ANode visitNull(final NullContext ctx) {
        return new ENull(line(ctx), location(ctx));
    }

    @Override
    public ANode visitPostinc(final PostincContext ctx) {
        final List<ALink> links = new ArrayList<>();
        final Operation operation;

        visitChain(ctx.chain(), links);

        if (ctx.INCR() != null) {
            operation = Operation.INCR;
        } else if (ctx.DECR() != null) {
            operation = Operation.DECR;
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        return new EChain(line(ctx), location(ctx), links, false, true, operation, null);
    }

    @Override
    public ANode visitPreinc(final PreincContext ctx) {
        final List<ALink> links = new ArrayList<>();
        final Operation operation;

        visitChain(ctx.chain(), links);

        if (ctx.INCR() != null) {
            operation = Operation.INCR;
        } else if (ctx.DECR() != null) {
            operation = Operation.DECR;
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        return new EChain(line(ctx), location(ctx), links, true, false, operation, null);
    }

    @Override
    public ANode visitRead(final ReadContext ctx) {
        final List<ALink> links = new ArrayList<>();

        visitChain(ctx.chain(), links);

        return new EChain(line(ctx), location(ctx), links, false, false, null, null);
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

            return new EUnary(line(ctx), location(ctx), operation, (AExpression)visit(ctx.expression()));
        }
    }

    @Override
    public ANode visitCast(final CastContext ctx) {
        return new EExplicit(line(ctx), location(ctx), ctx.decltype().getText(), (AExpression)visit(ctx.expression()));
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

        return new EBinary(line(ctx), location(ctx), operation, left, right);
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

        return new EComp(line(ctx), location(ctx), operation, left, right);
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

        return new EBool(line(ctx), location(ctx), operation, left, right);
    }


    @Override
    public ANode visitConditional(final ConditionalContext ctx) {
        final AExpression condition = (AExpression)visit(ctx.expression(0));
        final AExpression left = (AExpression)visit(ctx.expression(1));
        final AExpression right = (AExpression)visit(ctx.expression(2));

        return new EConditional(line(ctx), location(ctx), condition, left, right);
    }

    @Override
    public ANode visitAssignment(final AssignmentContext ctx) {
        final List<ALink> links = new ArrayList<>();
        final Operation operation;

        visitChain(ctx.chain(), links);

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

        return new EChain(line(ctx), location(ctx), links, false, false, operation, (AExpression)visit(ctx.expression()));
    }

    private void visitChain(final ChainContext ctx, final List<ALink> links) {
        if (ctx.linkprec() != null) {
            visitLinkprec(ctx.linkprec(), links);
        } else if (ctx.linkcast() != null) {
            visitLinkcast(ctx.linkcast(), links);
        } else if (ctx.linkvar() != null) {
            visitLinkvar(ctx.linkvar(), links);
        } else if (ctx.linknew() != null) {
            visitLinknew(ctx.linknew(), links);
        } else if (ctx.linkstring() != null) {
            visitLinkstring(ctx.linkstring(), links);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }
    }

    @Override
    public ANode visitChain(final ChainContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitLinkprec(final LinkprecContext ctx, final List<ALink> links) {
        if (ctx.linkprec() != null) {
            visitLinkprec(ctx.linkprec(), links);
        } else if (ctx.linkcast() != null) {
            visitLinkcast(ctx.linkcast(), links);
        } else if (ctx.linkvar() != null) {
            visitLinkvar(ctx.linkvar(), links);
        } else if (ctx.linknew() != null) {
            visitLinknew(ctx.linknew(), links);
        } else if (ctx.linkstring() != null) {
            visitLinkstring(ctx.linkstring(), links);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        if (ctx.linkbrace() != null) {
            visitLinkbrace(ctx.linkbrace(), links);
        } else if (ctx.linkdot() != null) {
            visitLinkdot(ctx.linkdot(), links);
        }
    }

    @Override
    public ANode visitLinkprec(final LinkprecContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitLinkcast(final LinkcastContext ctx, final List<ALink> links) {
        if (ctx.linkprec() != null) {
            visitLinkprec(ctx.linkprec(), links);
        } else if (ctx.linkcast() != null) {
            visitLinkcast(ctx.linkcast(), links);
        } else if (ctx.linkvar() != null) {
            visitLinkvar(ctx.linkvar(), links);
        } else if (ctx.linknew() != null) {
            visitLinknew(ctx.linknew(), links);
        } else if (ctx.linkstring() != null) {
            visitLinkstring(ctx.linkstring(), links);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        links.add(new LCast(line(ctx), location(ctx), ctx.decltype().getText()));
    }

    @Override
    public ANode visitLinkcast(final LinkcastContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitLinkbrace(final LinkbraceContext ctx, final List<ALink> links) {
        links.add(new LBrace(line(ctx), location(ctx), (AExpression)visit(ctx.expression())));

        if (ctx.linkbrace() != null) {
            visitLinkbrace(ctx.linkbrace(), links);
        } else if (ctx.linkdot() != null) {
            visitLinkdot(ctx.linkdot(), links);
        }
    }

    @Override
    public ANode visitLinkbrace(final LinkbraceContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitLinkdot(final LinkdotContext ctx, final List<ALink> links) {
        if (ctx.linkcall() != null) {
            visitLinkcall(ctx.linkcall(), links);
        } else if (ctx.linkfield() != null) {
            visitLinkfield(ctx.linkfield(), links);
        }
    }

    @Override
    public ANode visitLinkdot(final LinkdotContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitLinkcall(final LinkcallContext ctx, final List<ALink> links) {
        final List<AExpression> arguments = new ArrayList<>();

        for (final ExpressionContext expression : ctx.arguments().expression()) {
            arguments.add((AExpression)visit(expression));
        }

        links.add(new LCall(line(ctx), location(ctx), ctx.EXTID().getText(), arguments));

        if (ctx.linkbrace() != null) {
            visitLinkbrace(ctx.linkbrace(), links);
        } else if (ctx.linkdot() != null) {
            visitLinkdot(ctx.linkdot(), links);
        }
    }

    @Override
    public ANode visitLinkcall(final LinkcallContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitLinkvar(final LinkvarContext ctx, final List<ALink> links) {
        final String name = ctx.identifier().getText();

        reserved.markReserved(name);

        links.add(new LVariable(line(ctx), location(ctx), name));

        if (ctx.linkbrace() != null) {
            visitLinkbrace(ctx.linkbrace(), links);
        } else if (ctx.linkdot() != null) {
            visitLinkdot(ctx.linkdot(), links);
        }
    }

    @Override
    public ANode visitLinkvar(final LinkvarContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitLinkfield(final LinkfieldContext ctx, final List<ALink> links) {
        final String value;

        if (ctx.EXTID() != null) {
            value = ctx.EXTID().getText();
        } else if (ctx.EXTINTEGER() != null) {
            value = ctx.EXTINTEGER().getText();
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        links.add(new LField(line(ctx), location(ctx), value));

        if (ctx.linkbrace() != null) {
            visitLinkbrace(ctx.linkbrace(), links);
        } else if (ctx.linkdot() != null) {
            visitLinkdot(ctx.linkdot(), links);
        }
    }

    @Override
    public ANode visitLinkfield(final LinkfieldContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitLinknew(final LinknewContext ctx, final List<ALink> links) {
        final List<AExpression> arguments = new ArrayList<>();

        if (ctx.arguments() != null) {
            for (final ExpressionContext expression : ctx.arguments().expression()) {
                arguments.add((AExpression)visit(expression));
            }

            links.add(new LNewObj(line(ctx), location(ctx), ctx.identifier().getText(), arguments));
        } else if (ctx.expression().size() > 0) {
            for (final ExpressionContext expression : ctx.expression()) {
                arguments.add((AExpression)visit(expression));
            }

            links.add(new LNewArray(line(ctx), location(ctx), ctx.identifier().getText(), arguments));
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        if (ctx.linkdot() != null) {
            visitLinkdot(ctx.linkdot(), links);
        }
    }

    @Override
    public ANode visitLinknew(final LinknewContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitLinkstring(final LinkstringContext ctx, final List<ALink> links) {
        links.add(new LString(line(ctx), location(ctx), ctx.STRING().getText().substring(1, ctx.STRING().getText().length() - 1)));

        if (ctx.linkbrace() != null) {
            visitLinkbrace(ctx.linkbrace(), links);
        } else if (ctx.linkdot() != null) {
            visitLinkdot(ctx.linkdot(), links);
        }
    }

    @Override
    public ANode visitLinkstring(final LinkstringContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public ANode visitArguments(final ArgumentsContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }
}
