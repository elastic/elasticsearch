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
import org.elasticsearch.painless.antlr.PainlessParser.TypeContext;
import org.elasticsearch.painless.antlr.PainlessParser.SingleContext;
import org.elasticsearch.painless.node.AExpression;
import org.elasticsearch.painless.node.ALink;
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
        ANTLRInputStream stream = new ANTLRInputStream(source);
        PainlessLexer lexer = new ErrorHandlingLexer(stream);
        PainlessParser parser = new PainlessParser(new CommonTokenStream(lexer));
        ParserErrorStrategy strategy = new ParserErrorStrategy();

        lexer.removeErrorListeners();
        parser.removeErrorListeners();

        if (settings.isPicky()) {
            setupPicky(parser);
        }

        parser.setErrorHandler(strategy);

        return parser.sourceBlock();
    }

    private void setupPicky(PainlessParser parser) {
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

    private int line(ParserRuleContext ctx) {
        return ctx.getStart().getLine();
    }

    private int offset(ParserRuleContext ctx) {
        return ctx.getStart().getStartIndex();
    }

    private String location(ParserRuleContext ctx) {
        return "[ " + ctx.getStart().getLine() + " : " + ctx.getStart().getCharPositionInLine() + " ]";
    }


    @Override
    public ANode visitSourceBlock(SourceBlockContext ctx) {
        List<AStatement> statements = new ArrayList<>();

        for (ShortStatementContext statement : ctx.shortStatement()) {
            statements.add((AStatement)visit(statement));
        }

        return new SSource(line(ctx), offset(ctx), location(ctx), statements);
     }

    @Override
    public ANode visitShortStatementBlock(ShortStatementBlockContext ctx) {
        if (ctx.statementBlock() != null) {
            return visit(ctx.statementBlock());
        } else if (ctx.shortStatement() != null) {
            List<AStatement> statements = new ArrayList<>();
            statements.add((AStatement)visit(ctx.shortStatement()));

            return new SBlock(line(ctx), offset(ctx), location(ctx), statements);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
        }
    }

    @Override
    public ANode visitLongStatementBlock(LongStatementBlockContext ctx) {
        if (ctx.statementBlock() != null) {
            return visit(ctx.statementBlock());
        } else if (ctx.longStatement() != null) {
            List<AStatement> statements = new ArrayList<>();
            statements.add((AStatement)visit(ctx.longStatement()));

            return new SBlock(line(ctx), offset(ctx), location(ctx), statements);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
        }
    }

    @Override
    public ANode visitStatementBlock(StatementBlockContext ctx) {
        if (ctx.shortStatement().isEmpty()) {
            return null;
        } else {
            List<AStatement> statements = new ArrayList<>();

            for (ShortStatementContext statement : ctx.shortStatement()) {
                statements.add((AStatement)visit(statement));
            }

            return new SBlock(line(ctx), offset(ctx), location(ctx), statements);
        }
    }

    @Override
    public ANode visitEmptyStatement(EmptyStatementContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
    }

    @Override
    public ANode visitShortStatement(ShortStatementContext ctx) {
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
    public ANode visitNoTrailingStatement(NoTrailingStatementContext ctx) {
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
    public ANode visitShortIfStatement(ShortIfStatementContext ctx) {
        AExpression expression = (AExpression)visit(ctx.expression());
        SBlock block = (SBlock)visit(ctx.shortStatementBlock());

        return new SIf(line(ctx), offset(ctx), location(ctx), expression, block);
    }

    @Override
    public ANode visitLongIfShortElseStatement(LongIfShortElseStatementContext ctx) {
        AExpression expression = (AExpression)visit(ctx.expression());
        SBlock ifblock = (SBlock)visit(ctx.longStatementBlock());
        SBlock elseblock = (SBlock)visit(ctx.shortStatementBlock());

        return new SIfElse(line(ctx), offset(ctx), location(ctx), expression, ifblock, elseblock);
    }

    @Override
    public ANode visitLongIfStatement(LongIfStatementContext ctx) {
        AExpression expression = (AExpression)visit(ctx.expression());
        SBlock ifblock = (SBlock)visit(ctx.longStatementBlock(0));
        SBlock elseblock = (SBlock)visit(ctx.longStatementBlock(1));

        return new SIfElse(line(ctx), offset(ctx), location(ctx), expression, ifblock, elseblock);
    }

    @Override
    public ANode visitShortWhileStatement(ShortWhileStatementContext ctx) {
        if (settings.getMaxLoopCounter() > 0) {
            reserved.usesLoop();
        }

        AExpression expression = (AExpression)visit(ctx.expression());
        final SBlock block;

        if (ctx.shortStatementBlock() != null) {
            block = (SBlock)visit(ctx.shortStatementBlock());
        } else if (ctx.emptyStatement() != null) {
            block = null;
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
        }

        return new SWhile(line(ctx), offset(ctx), location(ctx), settings.getMaxLoopCounter(), expression, block);
    }

    @Override
    public ANode visitLongWhileStatement(LongWhileStatementContext ctx) {
        if (settings.getMaxLoopCounter() > 0) {
            reserved.usesLoop();
        }

        AExpression expression = (AExpression)visit(ctx.expression());
        final SBlock block;

        if (ctx.longStatementBlock() != null) {
            block = (SBlock)visit(ctx.longStatementBlock());
        } else if (ctx.emptyStatement() != null) {
            block = null;
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
        }

        return new SWhile(line(ctx), offset(ctx), location(ctx), settings.getMaxLoopCounter(), expression, block);
    }

    @Override
    public ANode visitShortForStatement(ShortForStatementContext ctx) {
        if (settings.getMaxLoopCounter() > 0) {
            reserved.usesLoop();
        }

        ANode initializer = ctx.forInitializer() == null ? null : visit(ctx.forInitializer());
        AExpression expression = ctx.expression() == null ? null : (AExpression)visit(ctx.expression());
        AExpression afterthought = ctx.forInitializer() == null ? null : (AExpression)visit(ctx.forInitializer());
        final SBlock block;

        if (ctx.shortStatementBlock() != null) {
            block = (SBlock)visit(ctx.shortStatementBlock());
        } else if (ctx.emptyStatement() != null) {
            block = null;
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
        }

        return new SFor(line(ctx), offset(ctx), location(ctx), settings.getMaxLoopCounter(),
            initializer, expression, afterthought, block);
    }

    @Override
    public ANode visitLongForStatement(LongForStatementContext ctx) {
        if (settings.getMaxLoopCounter() > 0) {
            reserved.usesLoop();
        }

        ANode initializer = ctx.forInitializer() == null ? null : visit(ctx.forInitializer());
        AExpression expression = ctx.expression() == null ? null : (AExpression)visit(ctx.expression());
        AExpression afterthought = ctx.forInitializer() == null ? null : (AExpression)visit(ctx.forInitializer());
        final SBlock block;

        if (ctx.longStatementBlock() != null) {
            block = (SBlock)visit(ctx.longStatementBlock());
        } else if (ctx.emptyStatement() != null) {
            block = null;
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
        }

        return new SFor(line(ctx), offset(ctx), location(ctx), settings.getMaxLoopCounter(),
            initializer, expression, afterthought, block);
    }

    @Override
    public ANode visitDoStatement(DoStatementContext ctx) {
        if (settings.getMaxLoopCounter() > 0) {
            reserved.usesLoop();
        }

        SBlock block = (SBlock)visit(ctx.statementBlock());
        AExpression expression = (AExpression)visit(ctx.expression());

        return new SDo(line(ctx), offset(ctx), location(ctx), settings.getMaxLoopCounter(), block, expression);
    }

    @Override
    public ANode visitDeclarationStatement(DeclarationStatementContext ctx) {
        String type = ctx.declarationType().getText();
        List<SDeclaration> declarations = new ArrayList<>();

        for (DeclarationVariableContext declaration : ctx.declarationVariable()) {
            String name = declaration.ID().getText();
            AExpression expression = declaration.expression() == null ? null : (AExpression)visit(declaration.expression());

            declarations.add(new SDeclaration(line(ctx), offset(ctx), location(ctx), type, name, expression));
        }

        return new SDeclBlock(line(ctx), offset(ctx), location(ctx), declarations);
    }

    @Override
    public ANode visitContinueStatement(ContinueStatementContext ctx) {
        return new SContinue(line(ctx), offset(ctx), location(ctx));
    }

    @Override
    public ANode visitBreakStatement(BreakStatementContext ctx) {
        return new SBreak(line(ctx), offset(ctx), location(ctx));
    }

    @Override
    public ANode visitReturnStatement(ReturnStatementContext ctx) {
        AExpression expression = (AExpression)visit(ctx.expression());

        return new SReturn(line(ctx), offset(ctx), location(ctx), expression);
    }

    @Override
    public ANode visitTryStatement(TryStatementContext ctx) {
        SBlock block = (SBlock)visit(ctx.statementBlock());
        List<SCatch> catches = new ArrayList<>();

        for (CatchBlockContext catc : ctx.catchBlock()) {
            catches.add((SCatch)visit(catc));
        }

        return new STry(line(ctx), offset(ctx), location(ctx), block, catches);
    }

    @Override
    public ANode visitThrowStatement(ThrowStatementContext ctx) {
        AExpression expression = (AExpression)visit(ctx.expression());

        return new SThrow(line(ctx), offset(ctx), location(ctx), expression);
    }

    @Override
    public ANode visitExpressionStatement(ExpressionStatementContext ctx) {
        AExpression expression = (AExpression)visit(ctx.expression());

        return new SExpression(line(ctx), offset(ctx), location(ctx), expression);
    }

    @Override
    public ANode visitForInitializer(ForInitializerContext ctx) {
        if (ctx.declarationStatement() != null) {
            return visit(ctx.declarationStatement());
        } else if (ctx.expression() != null) {
            return visit(ctx.expression());
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
        }
    }

    @Override
    public ANode visitForAfterthought(ForAfterthoughtContext ctx) {
        return visit(ctx.expression());
    }

    @Override
    public ANode visitDeclarationType(DeclarationTypeContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
    }

    @Override
    public ANode visitType(TypeContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
    }

    @Override
    public ANode visitDeclarationVariable(DeclarationVariableContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
    }

    @Override
    public ANode visitCatchBlock(CatchBlockContext ctx) {
        String type = ctx.type().getText();
        String name = ctx.ID().getText();
        SBlock block = (SBlock)visit(ctx.statementBlock());

        return new SCatch(line(ctx), offset(ctx), location(ctx), type, name, block);
    }

    @Override
    public ANode visitDelimiter(DelimiterContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Illegal tree structure.");
    }

    @Override
    public ANode visitSingle(SingleContext ctx) {
        return visit(ctx.unary());
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
    public ANode visitNull(PainlessParser.NullContext ctx) {
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
    public ANode visitDynamicprimary(PainlessParser.DynamicprimaryContext ctx) {
        return null;
    }

    @Override
    public ANode visitStaticprimary(PainlessParser.StaticprimaryContext ctx) {
        return null;
    }

    @Override
    public ANode visitNewarray(PainlessParser.NewarrayContext ctx) {
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
    public ANode visitSecondary(PainlessParser.SecondaryContext ctx) {
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
    public ANode visitBraceaccess(PainlessParser.BraceaccessContext ctx) {
        return null;
    }

    @Override
    public ANode visitArguments(PainlessParser.ArgumentsContext ctx) {
        return null;
    }
}
