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
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.DiagnosticErrorListener;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.Variables.Reserved;
import org.elasticsearch.painless.antlr.PainlessParser.ArgumentsContext;
import org.elasticsearch.painless.antlr.PainlessParser.AssignmentContext;
import org.elasticsearch.painless.antlr.PainlessParser.BinaryContext;
import org.elasticsearch.painless.antlr.PainlessParser.BreakStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.CastContext;
import org.elasticsearch.painless.antlr.PainlessParser.CatchBlockContext;
import org.elasticsearch.painless.antlr.PainlessParser.ChainContext;
import org.elasticsearch.painless.antlr.PainlessParser.ConditionalContext;
import org.elasticsearch.painless.antlr.PainlessParser.ContinueStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.DeclarationStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.DeclarationTypeContext;
import org.elasticsearch.painless.antlr.PainlessParser.DeclarationVariableContext;
import org.elasticsearch.painless.antlr.PainlessParser.DelimiterContext;
import org.elasticsearch.painless.antlr.PainlessParser.DoStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.EmptyStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.ExpressionContext;
import org.elasticsearch.painless.antlr.PainlessParser.ExpressionStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.FalseContext;
import org.elasticsearch.painless.antlr.PainlessParser.ForAfterthoughtContext;
import org.elasticsearch.painless.antlr.PainlessParser.ForInitializerContext;
import org.elasticsearch.painless.antlr.PainlessParser.LinkbraceContext;
import org.elasticsearch.painless.antlr.PainlessParser.LinkcallContext;
import org.elasticsearch.painless.antlr.PainlessParser.LinkcastContext;
import org.elasticsearch.painless.antlr.PainlessParser.LinkdotContext;
import org.elasticsearch.painless.antlr.PainlessParser.LinkfieldContext;
import org.elasticsearch.painless.antlr.PainlessParser.LinknewContext;
import org.elasticsearch.painless.antlr.PainlessParser.LinkprecContext;
import org.elasticsearch.painless.antlr.PainlessParser.LinkstringContext;
import org.elasticsearch.painless.antlr.PainlessParser.LinkvarContext;
import org.elasticsearch.painless.antlr.PainlessParser.LongForStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.LongIfShortElseStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.LongIfStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.LongStatementBlockContext;
import org.elasticsearch.painless.antlr.PainlessParser.LongStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.LongWhileStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.NoTrailingStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.NullContext;
import org.elasticsearch.painless.antlr.PainlessParser.NumericContext;
import org.elasticsearch.painless.antlr.PainlessParser.PostincContext;
import org.elasticsearch.painless.antlr.PainlessParser.PrecedenceContext;
import org.elasticsearch.painless.antlr.PainlessParser.PreincContext;
import org.elasticsearch.painless.antlr.PainlessParser.ReadContext;
import org.elasticsearch.painless.antlr.PainlessParser.ReturnStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.ShortForStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.ShortIfStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.ShortStatementBlockContext;
import org.elasticsearch.painless.antlr.PainlessParser.ShortStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.ShortWhileStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.SourceBlockContext;
import org.elasticsearch.painless.antlr.PainlessParser.StatementBlockContext;
import org.elasticsearch.painless.antlr.PainlessParser.ThrowStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.TrueContext;
import org.elasticsearch.painless.antlr.PainlessParser.TryStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.UnaryContext;
import org.elasticsearch.painless.node.AExpression;
import org.elasticsearch.painless.node.ALink;
import org.elasticsearch.painless.node.ANode;
import org.elasticsearch.painless.node.AStatement;
import org.elasticsearch.painless.node.EBinary;
import org.elasticsearch.painless.node.EBool;
import org.elasticsearch.painless.node.EBoolean;
import org.elasticsearch.painless.node.EChain;
import org.elasticsearch.painless.node.EComp;
import org.elasticsearch.painless.node.EConditional;
import org.elasticsearch.painless.node.EDecimal;
import org.elasticsearch.painless.node.EExplicit;
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
import org.elasticsearch.painless.node.SCatch;
import org.elasticsearch.painless.node.SContinue;
import org.elasticsearch.painless.node.SDeclBlock;
import org.elasticsearch.painless.node.SDeclaration;
import org.elasticsearch.painless.node.SDo;
import org.elasticsearch.painless.node.SExpression;
import org.elasticsearch.painless.node.SFor;
import org.elasticsearch.painless.node.SIf;
import org.elasticsearch.painless.node.SIfElse;
import org.elasticsearch.painless.node.SReturn;
import org.elasticsearch.painless.node.SSource;
import org.elasticsearch.painless.node.SThrow;
import org.elasticsearch.painless.node.STry;
import org.elasticsearch.painless.node.SWhile;

import java.util.ArrayList;
import java.util.List;

/**
 * Converts the ANTLR tree to a Painless tree.
 */
public final class Walker extends PainlessParserBaseVisitor<ANode> {

    public static SSource buildPainlessTree(final String source, final Reserved reserved, boolean picky) {
        return new Walker(source, reserved, picky).source;
    }

    private final Reserved reserved;
    private final SSource source;

    private Walker(final String source, final Reserved reserved, boolean picky) {
        this.reserved = reserved;
        this.source = (SSource)visit(buildAntlrTree(source, picky));
    }

    private static SourceBlockContext buildAntlrTree(final String source, boolean picky) {
        final ANTLRInputStream stream = new ANTLRInputStream(source);
        final PainlessLexer lexer = new ErrorHandlingLexer(stream);
        final PainlessParser parser = new PainlessParser(new CommonTokenStream(lexer));
        final ParserErrorStrategy strategy = new ParserErrorStrategy();

        lexer.removeErrorListeners();
        parser.removeErrorListeners();

        if (picky) {
            // Diagnostic listener invokes syntaxError on other listeners for ambiguity issues,
            parser.addErrorListener(new DiagnosticErrorListener(true));
            // a second listener to fail the test when the above happens.
            parser.addErrorListener(new BaseErrorListener() {
                @Override
                public void syntaxError(final Recognizer<?,?> recognizer, final Object offendingSymbol, final int line,
                                        final int charPositionInLine, final String msg, final RecognitionException e) {
                    throw new AssertionError("line: " + line + ", offset: " + charPositionInLine +
                                             ", symbol:" + offendingSymbol + " " + msg);
                }
            });

            // Enable exact ambiguity detection (costly). we enable exact since its the default for
            // DiagnosticErrorListener, life is too short to think about what 'inexact ambiguity' might mean.
            parser.getInterpreter().setPredictionMode(PredictionMode.LL_EXACT_AMBIG_DETECTION);
        }

        parser.setErrorHandler(strategy);

        return parser.sourceBlock();
    }

    private int line(final ParserRuleContext ctx) {
        return ctx.getStart().getLine();
    }

    private String location(final ParserRuleContext ctx) {
        return "[ " + ctx.getStart().getLine() + " : " + ctx.getStart().getCharPositionInLine() + " ]";
    }

    @Override
    public ANode visitSourceBlock(final SourceBlockContext ctx) {
        final List<AStatement> statements = new ArrayList<>();

        for (final ShortStatementContext statement : ctx.shortStatement()) {
            statements.add((AStatement)visit(statement));
        }

        return new SSource(line(ctx), location(ctx), statements);
    }

    @Override
    public ANode visitShortStatementBlock(final ShortStatementBlockContext ctx) {
        if (ctx.statementBlock() != null) {
            return visit(ctx.statementBlock());
        } else if (ctx.shortStatement() != null) {
            final List<AStatement> statements = new ArrayList<>();
            statements.add((AStatement)visit(ctx.shortStatement()));

            return new SBlock(line(ctx), location(ctx), statements);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
        }
    }

    @Override
    public ANode visitLongStatementBlock(final LongStatementBlockContext ctx) {
        if (ctx.statementBlock() != null) {
            return visit(ctx.statementBlock());
        } else if (ctx.longStatement() != null) {
            final List<AStatement> statements = new ArrayList<>();
            statements.add((AStatement)visit(ctx.longStatement()));

            return new SBlock(line(ctx), location(ctx), statements);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
        }
    }

    @Override
    public ANode visitStatementBlock(final StatementBlockContext ctx) {
        if (ctx.shortStatement().isEmpty()) {
            return null;
        } else {
            final List<AStatement> statements = new ArrayList<>();

            for (final ShortStatementContext statement : ctx.shortStatement()) {
                statements.add((AStatement)visit(statement));
            }

            return new SBlock(line(ctx), location(ctx), statements);
        }
    }

    @Override
    public ANode visitEmptyStatement(final EmptyStatementContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
    }

    @Override
    public ANode visitShortStatement(final ShortStatementContext ctx) {
        if (ctx.noTrailingStatement() != null) {
            return visit(ctx.noTrailingStatement());
        } else if (ctx.shortIfStatement() != null) {
            return visit(ctx.shortIfStatement());
        } else if (ctx.longIfShortElseStatement() != null) {
            return visit(ctx.longIfShortElseStatement());
        } else if (ctx.shortWhileStatement() != null) {
            return visit(ctx.shortWhileStatement());
        } else if (ctx.shortForStatement() != null) {
            return visit(ctx.shortForStatement());
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
        }
    }

    @Override
    public ANode visitLongStatement(LongStatementContext ctx) {
        if (ctx.noTrailingStatement() != null) {
            return visit(ctx.noTrailingStatement());
        } else if (ctx.longIfStatement() != null) {
            return visit(ctx.longIfStatement());
        } else if (ctx.longWhileStatement() != null) {
            return visit(ctx.longWhileStatement());
        } else if (ctx.longForStatement() != null) {
            return visit(ctx.longForStatement());
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
        }
    }

    @Override
    public ANode visitNoTrailingStatement(final NoTrailingStatementContext ctx) {
        if (ctx.declarationStatement() != null) {
            return visit(ctx.declarationStatement());
        } else if (ctx.doStatement() != null) {
            return visit(ctx.doStatement());
        } else if (ctx.continueStatement() != null) {
            return visit(ctx.continueStatement());
        } else if (ctx.breakStatement() != null) {
            return visit(ctx.breakStatement());
        } else if (ctx.returnStatement() != null) {
            return visit(ctx.returnStatement());
        } else if (ctx.tryStatement() != null) {
            return visit(ctx.tryStatement());
        } else if (ctx.throwStatement() != null) {
            return visit(ctx.throwStatement());
        } else if (ctx.expressionStatement() != null) {
            return visit(ctx.expressionStatement());
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
        }
    }

    @Override
    public ANode visitShortIfStatement(final ShortIfStatementContext ctx) {
        final AExpression condition = (AExpression)visit(ctx.expression());
        final SBlock ifblock = (SBlock)visit(ctx.shortStatementBlock());

        return new SIf(line(ctx), location(ctx), condition, ifblock);
    }

    @Override
    public ANode visitLongIfShortElseStatement(final LongIfShortElseStatementContext ctx) {
        final AExpression condition = (AExpression)visit(ctx.expression());
        final SBlock ifblock = (SBlock)visit(ctx.longStatementBlock());
        final SBlock elseblock = (SBlock)visit(ctx.shortStatementBlock());

        return new SIfElse(line(ctx), location(ctx), condition, ifblock, elseblock);
    }

    @Override
    public ANode visitLongIfStatement(final LongIfStatementContext ctx) {
        final AExpression condition = (AExpression)visit(ctx.expression());
        final SBlock ifblock = (SBlock)visit(ctx.longStatementBlock(0));
        final SBlock elseblock = (SBlock)visit(ctx.longStatementBlock(1));

        return new SIfElse(line(ctx), location(ctx), condition, ifblock, elseblock);
    }

    @Override
    public ANode visitShortWhileStatement(final ShortWhileStatementContext ctx) {
        final AExpression condition = (AExpression)visit(ctx.expression());
        final SBlock block;

        if (ctx.shortStatementBlock() != null) {
            block = (SBlock)visit(ctx.shortStatementBlock());
        } else if (ctx.emptyStatement() != null) {
            block = null;
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
        }

        reserved.usesLoop();

        return new SWhile(line(ctx), location(ctx), condition, block);
    }

    @Override
    public ANode visitLongWhileStatement(final LongWhileStatementContext ctx) {
        final AExpression condition = (AExpression)visit(ctx.expression());
        final SBlock block;

        if (ctx.longStatementBlock() != null) {
            block = (SBlock)visit(ctx.longStatementBlock());
        } else if (ctx.emptyStatement() != null) {
            block = null;
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
        }

        reserved.usesLoop();

        return new SWhile(line(ctx), location(ctx), condition, block);
    }

    @Override
    public ANode visitShortForStatement(final ShortForStatementContext ctx) {
        final ANode intializer = ctx.forInitializer() == null ? null : visit(ctx.forInitializer());
        final AExpression condition = ctx.expression() == null ? null : (AExpression)visit(ctx.expression());
        final AExpression afterthought = ctx.forAfterthought() == null ? null : (AExpression)visit(ctx.forAfterthought());
        final SBlock block;

        if (ctx.shortStatementBlock() != null) {
            block = (SBlock)visit(ctx.shortStatementBlock());
        } else if (ctx.emptyStatement() != null) {
            block = null;
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
        }

        reserved.usesLoop();

        return new SFor(line(ctx), location(ctx), intializer, condition, afterthought, block);
    }

    @Override
    public ANode visitLongForStatement(final LongForStatementContext ctx) {
        final ANode intializer = ctx.forInitializer() == null ? null : visit(ctx.forInitializer());
        final AExpression condition = ctx.expression() == null ? null : (AExpression)visit(ctx.expression());
        final AExpression afterthought = ctx.forAfterthought() == null ? null : (AExpression)visit(ctx.forAfterthought());
        final SBlock block;

        if (ctx.longStatementBlock() != null) {
            block = (SBlock)visit(ctx.longStatementBlock());
        } else if (ctx.emptyStatement() != null) {
            block = null;
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
        }

        reserved.usesLoop();

        return new SFor(line(ctx), location(ctx), intializer, condition, afterthought, block);
    }

    @Override
    public ANode visitDoStatement(final DoStatementContext ctx) {
        final AExpression condition = (AExpression)visit(ctx.expression());
        final SBlock block = (SBlock)visit(ctx.statementBlock());

        reserved.usesLoop();

        return new SDo(line(ctx), location(ctx), block, condition);
    }

    @Override
    public ANode visitDeclarationStatement(final DeclarationStatementContext ctx) {
        final String type = ctx.declarationType().getText();
        final List<SDeclaration> declarations = new ArrayList<>();

        for (final DeclarationVariableContext declarationVariable : ctx.declarationVariable()) {
            final String name = declarationVariable.ID().getText();
            final AExpression expression = declarationVariable.expression() == null ?
                null : (AExpression)visit(declarationVariable.expression());

            declarations.add(new SDeclaration(line(ctx), location(ctx), type, name, expression));
        }

        return new SDeclBlock(line(ctx), location(ctx), declarations);
    }

    @Override
    public ANode visitContinueStatement(final ContinueStatementContext ctx) {
        return new SContinue(line(ctx), location(ctx));
    }

    @Override
    public ANode visitBreakStatement(final BreakStatementContext ctx) {
        return new SBreak(line(ctx), location(ctx));
    }

    @Override
    public ANode visitReturnStatement(final ReturnStatementContext ctx) {
        final AExpression expression = (AExpression)visit(ctx.expression());

        return new SReturn(line(ctx), location(ctx), expression);
    }

    @Override
    public ANode visitTryStatement(final TryStatementContext ctx) {
        final SBlock block = (SBlock)visit(ctx.statementBlock());
        final List<SCatch> traps = new ArrayList<>();

        for (final CatchBlockContext trap : ctx.catchBlock()) {
            traps.add((SCatch)visit(trap));
        }

        return new STry(line(ctx), location(ctx), block, traps);
    }

    @Override
    public ANode visitThrowStatement(final ThrowStatementContext ctx) {
        final AExpression expression = (AExpression)visit(ctx.expression());

        return new SThrow(line(ctx), location(ctx), expression);
    }

    @Override
    public ANode visitExpressionStatement(final ExpressionStatementContext ctx) {
        final AExpression expression = (AExpression)visit(ctx.expression());

        return new SExpression(line(ctx), location(ctx), expression);
    }

    @Override
    public ANode visitForInitializer(final ForInitializerContext ctx) {
        if (ctx.declarationStatement() != null) {
            return visit(ctx.declarationStatement());
        } else if (ctx.expression() != null) {
            return visit(ctx.expression());
        }

        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
    }

    @Override
    public ANode visitForAfterthought(final ForAfterthoughtContext ctx) {
        return visit(ctx.expression());
    }

    @Override
    public ANode visitDeclarationType(final DeclarationTypeContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
    }

    @Override
    public ANode visitDeclarationVariable(final DeclarationVariableContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
    }

    @Override
    public ANode visitCatchBlock(final CatchBlockContext ctx) {
        final String type = ctx.ID(0).getText();
        final String name = ctx.ID(1).getText();
        final SBlock block = (SBlock)visit(ctx.statementBlock());

        return new SCatch(line(ctx), location(ctx), type, name, block);
    }

    @Override
    public ANode visitDelimiter(final DelimiterContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
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
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
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
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
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
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
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
                throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
            }

            return new EUnary(line(ctx), location(ctx), operation, (AExpression)visit(ctx.expression()));
        }
    }

    @Override
    public ANode visitCast(final CastContext ctx) {
        return new EExplicit(line(ctx), location(ctx), ctx.declarationType().getText(), (AExpression)visit(ctx.expression()));
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
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
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
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
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
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
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
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
        }
    }

    @Override
    public ANode visitChain(final ChainContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
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
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
        }

        if (ctx.linkbrace() != null) {
            visitLinkbrace(ctx.linkbrace(), links);
        } else if (ctx.linkdot() != null) {
            visitLinkdot(ctx.linkdot(), links);
        }
    }

    @Override
    public ANode visitLinkprec(final LinkprecContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
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
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
        }

        links.add(new LCast(line(ctx), location(ctx), ctx.declarationType().getText()));
    }

    @Override
    public ANode visitLinkcast(final LinkcastContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
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
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
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
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
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
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
    }

    private void visitLinkvar(final LinkvarContext ctx, final List<ALink> links) {
        final String name = ctx.ID().getText();

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
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
    }

    private void visitLinkfield(final LinkfieldContext ctx, final List<ALink> links) {
        final String value;

        if (ctx.EXTID() != null) {
            value = ctx.EXTID().getText();
        } else if (ctx.EXTINTEGER() != null) {
            value = ctx.EXTINTEGER().getText();
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
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
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
    }

    private void visitLinknew(final LinknewContext ctx, final List<ALink> links) {
        final List<AExpression> arguments = new ArrayList<>();

        if (ctx.arguments() != null) {
            for (final ExpressionContext expression : ctx.arguments().expression()) {
                arguments.add((AExpression)visit(expression));
            }

            links.add(new LNewObj(line(ctx), location(ctx), ctx.ID().getText(), arguments));
        } else if (ctx.expression().size() > 0) {
            for (final ExpressionContext expression : ctx.expression()) {
                arguments.add((AExpression)visit(expression));
            }

            links.add(new LNewArray(line(ctx), location(ctx), ctx.ID().getText(), arguments));
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
        }

        if (ctx.linkdot() != null) {
            visitLinkdot(ctx.linkdot(), links);
        }
    }

    @Override
    public ANode visitLinknew(final LinknewContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
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
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
    }

    @Override
    public ANode visitArguments(final ArgumentsContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
    }
}
