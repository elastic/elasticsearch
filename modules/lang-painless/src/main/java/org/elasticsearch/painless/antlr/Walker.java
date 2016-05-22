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
import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Variables.Reserved;
import org.elasticsearch.painless.antlr.PainlessParser.BreakStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.CatchBlockContext;
import org.elasticsearch.painless.antlr.PainlessParser.ContinueStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.DeclarationStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.DeclarationTypeContext;
import org.elasticsearch.painless.antlr.PainlessParser.DeclarationVariableContext;
import org.elasticsearch.painless.antlr.PainlessParser.DelimiterContext;
import org.elasticsearch.painless.antlr.PainlessParser.DoStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.EmptyStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.ExpressionStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.ForAfterthoughtContext;
import org.elasticsearch.painless.antlr.PainlessParser.ForInitializerContext;
import org.elasticsearch.painless.antlr.PainlessParser.LongForStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.LongIfShortElseStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.LongIfStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.LongStatementBlockContext;
import org.elasticsearch.painless.antlr.PainlessParser.LongStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.LongWhileStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.NoTrailingStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.ReturnStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.ShortForStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.ShortIfStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.ShortStatementBlockContext;
import org.elasticsearch.painless.antlr.PainlessParser.ShortStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.ShortWhileStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.SourceBlockContext;
import org.elasticsearch.painless.antlr.PainlessParser.StatementBlockContext;
import org.elasticsearch.painless.antlr.PainlessParser.ThrowStatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.TryStatementContext;
import org.elasticsearch.painless.node.AExpression;
import org.elasticsearch.painless.node.ANode;
import org.elasticsearch.painless.node.AStatement;
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

    public static SSource buildPainlessTree(String source, Reserved reserved, CompilerSettings settings) {
        return new Walker(source, reserved, settings).source;
    }

    private final Reserved reserved;
    private final SSource source;
    private final CompilerSettings settings;

    private Walker(String source, Reserved reserved, CompilerSettings settings) {
        this.reserved = reserved;
        this.settings = settings;
        this.source = (SSource)visit(buildAntlrTree(source));
    }

    private SourceBlockContext buildAntlrTree(String source) {
        final ANTLRInputStream stream = new ANTLRInputStream(source);
        final PainlessLexer lexer = new ErrorHandlingLexer(stream);
        final PainlessParser parser = new PainlessParser(new CommonTokenStream(lexer));
        final ParserErrorStrategy strategy = new ParserErrorStrategy();

        lexer.removeErrorListeners();
        parser.removeErrorListeners();

        if (settings.isPicky()) {
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

        return new SWhile(line(ctx), location(ctx), condition, block, settings.getMaxLoopCounter());
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

        return new SWhile(line(ctx), location(ctx), condition, block, settings.getMaxLoopCounter());
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

        return new SFor(line(ctx), location(ctx), intializer, condition, afterthought, block, settings.getMaxLoopCounter());
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

        return new SFor(line(ctx), location(ctx), intializer, condition, afterthought, block, settings.getMaxLoopCounter());
    }

    @Override
    public ANode visitDoStatement(final DoStatementContext ctx) {
        final AExpression condition = (AExpression)visit(ctx.expression());
        final SBlock block = (SBlock)visit(ctx.statementBlock());

        reserved.usesLoop();

        return new SDo(line(ctx), location(ctx), block, condition, settings.getMaxLoopCounter());
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
    public ANode visitType(PainlessParser.TypeContext ctx) {
        return null;
    }

    @Override
    public ANode visitDeclarationVariable(final DeclarationVariableContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
    }

    @Override
    public ANode visitCatchBlock(final CatchBlockContext ctx) {
        final String type = ctx.type().getText();
        final String name = ctx.ID().getText();
        final SBlock block = (SBlock)visit(ctx.statementBlock());

        return new SCatch(line(ctx), location(ctx), type, name, block);
    }

    @Override
    public ANode visitDelimiter(final DelimiterContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
    }

    @Override
    public ANode visitSingle(PainlessParser.SingleContext ctx) {
        return null;
    }

    @Override
    public ANode visitComp(PainlessParser.CompContext ctx) {
        return null;
    }

    @Override
    public ANode visitBool(PainlessParser.BoolContext ctx) {
        return null;
    }

    @Override
    public ANode visitConditional(PainlessParser.ConditionalContext ctx) {
        return null;
    }

    @Override
    public ANode visitAssignment(PainlessParser.AssignmentContext ctx) {
        return null;
    }

    @Override
    public ANode visitBinary(PainlessParser.BinaryContext ctx) {
        return null;
    }

    @Override
    public ANode visitPre(PainlessParser.PreContext ctx) {
        return null;
    }

    @Override
    public ANode visitPost(PainlessParser.PostContext ctx) {
        return null;
    }

    @Override
    public ANode visitRead(PainlessParser.ReadContext ctx) {
        return null;
    }

    @Override
    public ANode visitNumeric(PainlessParser.NumericContext ctx) {
        return null;
    }

    @Override
    public ANode visitTrue(PainlessParser.TrueContext ctx) {
        return null;
    }

    @Override
    public ANode visitFalse(PainlessParser.FalseContext ctx) {
        return null;
    }

    @Override
    public ANode visitOperator(PainlessParser.OperatorContext ctx) {
        return null;
    }

    @Override
    public ANode visitCast(PainlessParser.CastContext ctx) {
        return null;
    }

    @Override
    public ANode visitVariableprimary(PainlessParser.VariableprimaryContext ctx) {
        return null;
    }

    @Override
    public ANode visitTypeprimary(PainlessParser.TypeprimaryContext ctx) {
        return null;
    }

    @Override
    public ANode visitArraycreation(PainlessParser.ArraycreationContext ctx) {
        return null;
    }

    @Override
    public ANode visitPrecedence(PainlessParser.PrecedenceContext ctx) {
        return null;
    }

    @Override
    public ANode visitString(PainlessParser.StringContext ctx) {
        return null;
    }

    @Override
    public ANode visitVariable(PainlessParser.VariableContext ctx) {
        return null;
    }

    @Override
    public ANode visitNewobject(PainlessParser.NewobjectContext ctx) {
        return null;
    }

    @Override
    public ANode visitNewarray(PainlessParser.NewarrayContext ctx) {
        return null;
    }

    @Override
    public ANode visitCallinvoke(PainlessParser.CallinvokeContext ctx) {
        return null;
    }

    @Override
    public ANode visitFieldaccess(PainlessParser.FieldaccessContext ctx) {
        return null;
    }

    @Override
    public ANode visitArrayaccess(PainlessParser.ArrayaccessContext ctx) {
        return null;
    }

    @Override
    public ANode visitArguments(PainlessParser.ArgumentsContext ctx) {
        return null;
    }
}
