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
import org.elasticsearch.painless.antlr.PainlessParser.AfterthoughtContext;
import org.elasticsearch.painless.antlr.PainlessParser.BlockContext;
import org.elasticsearch.painless.antlr.PainlessParser.BraceaccessContext;
import org.elasticsearch.painless.antlr.PainlessParser.BreakContext;
import org.elasticsearch.painless.antlr.PainlessParser.CallinvokeContext;
import org.elasticsearch.painless.antlr.PainlessParser.ChainprecContext;
import org.elasticsearch.painless.antlr.PainlessParser.ContinueContext;
import org.elasticsearch.painless.antlr.PainlessParser.DeclContext;
import org.elasticsearch.painless.antlr.PainlessParser.DeclarationContext;
import org.elasticsearch.painless.antlr.PainlessParser.DecltypeContext;
import org.elasticsearch.painless.antlr.PainlessParser.DeclvarContext;
import org.elasticsearch.painless.antlr.PainlessParser.DelimiterContext;
import org.elasticsearch.painless.antlr.PainlessParser.DoContext;
import org.elasticsearch.painless.antlr.PainlessParser.DynamicContext;
import org.elasticsearch.painless.antlr.PainlessParser.EmptyContext;
import org.elasticsearch.painless.antlr.PainlessParser.ExprContext;
import org.elasticsearch.painless.antlr.PainlessParser.ExpressionContext;
import org.elasticsearch.painless.antlr.PainlessParser.ExprprecContext;
import org.elasticsearch.painless.antlr.PainlessParser.FieldaccessContext;
import org.elasticsearch.painless.antlr.PainlessParser.ForContext;
import org.elasticsearch.painless.antlr.PainlessParser.IfContext;
import org.elasticsearch.painless.antlr.PainlessParser.InitializerContext;
import org.elasticsearch.painless.antlr.PainlessParser.NewobjectContext;
import org.elasticsearch.painless.antlr.PainlessParser.ReturnContext;
import org.elasticsearch.painless.antlr.PainlessParser.SecondaryContext;
import org.elasticsearch.painless.antlr.PainlessParser.SourceContext;
import org.elasticsearch.painless.antlr.PainlessParser.StatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.StringContext;
import org.elasticsearch.painless.antlr.PainlessParser.ThrowContext;
import org.elasticsearch.painless.antlr.PainlessParser.TrailerContext;
import org.elasticsearch.painless.antlr.PainlessParser.TrapContext;
import org.elasticsearch.painless.antlr.PainlessParser.TryContext;
import org.elasticsearch.painless.antlr.PainlessParser.TypeContext;
import org.elasticsearch.painless.antlr.PainlessParser.VariableContext;
import org.elasticsearch.painless.antlr.PainlessParser.WhileContext;
import org.elasticsearch.painless.node.AExpression;
import org.elasticsearch.painless.node.ALink;
import org.elasticsearch.painless.node.ANode;
import org.elasticsearch.painless.node.AStatement;
import org.elasticsearch.painless.node.LBrace;
import org.elasticsearch.painless.node.LCall;
import org.elasticsearch.painless.node.LField;
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

    private SourceContext buildAntlrTree(String source) {
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

        return parser.source();
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
    public ANode visitSource(SourceContext ctx) {
        List<AStatement> statements = new ArrayList<>();

        for (StatementContext statement : ctx.statement()) {
            statements.add((AStatement)visit(statement));
        }

        return new SSource(line(ctx), offset(ctx), location(ctx), statements);
    }

    @Override
    public ANode visitIf(IfContext ctx) {
        AExpression expression = (AExpression)visit(ctx.expression());
        SBlock ifblock = (SBlock)visit(ctx.trailer(0));

        if (ctx.trailer().size() > 1) {
            SBlock elseblock = (SBlock)visit(ctx.trailer(1));

            return new SIfElse(line(ctx), offset(ctx), location(ctx), expression, ifblock, elseblock);
        } else {
            return new SIf(line(ctx), offset(ctx), location(ctx), expression, ifblock);
        }
    }

    @Override
    public ANode visitWhile(WhileContext ctx) {
        if (settings.getMaxLoopCounter() > 0) {
            reserved.usesLoop();
        }

        AExpression expression = (AExpression)visit(ctx.expression());

        if (ctx.trailer() != null) {
            SBlock block = (SBlock)visit(ctx.trailer());

            return new SWhile(line(ctx), offset(ctx), location(ctx), settings.getMaxLoopCounter(), expression, block);
        } else if (ctx.empty() != null) {
            return new SWhile(line(ctx), offset(ctx), location(ctx), settings.getMaxLoopCounter(), expression, null);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + " Illegal tree structure.");
        }
    }

    @Override
    public ANode visitDo(DoContext ctx) {
        if (settings.getMaxLoopCounter() > 0) {
            reserved.usesLoop();
        }

        AExpression expression = (AExpression)visit(ctx.expression());
        SBlock block = (SBlock)visit(ctx.block());

        return new SDo(line(ctx), offset(ctx), location(ctx), settings.getMaxLoopCounter(), block, expression);
    }

    @Override
    public ANode visitFor(ForContext ctx) {
        if (settings.getMaxLoopCounter() > 0) {
            reserved.usesLoop();
        }

        ANode initializer = ctx.initializer() == null ? null : visit(ctx.initializer());
        AExpression expression = ctx.expression() == null ? null : (AExpression)visit(ctx.expression());
        AExpression afterthought = ctx.afterthought() == null ? null : (AExpression)visit(ctx.afterthought());

        if (ctx.trailer() != null) {
            SBlock block = (SBlock)visit(ctx.trailer());

            return new SFor(line(ctx), offset(ctx), location(ctx),
                settings.getMaxLoopCounter(), initializer, expression, afterthought, block);
        } else if (ctx.empty() != null) {
            return new SFor(line(ctx), offset(ctx), location(ctx),
                settings.getMaxLoopCounter(), initializer, expression, afterthought, null);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + " Illegal tree structure.");
        }
    }

    @Override
    public ANode visitDecl(DeclContext ctx) {
        return visit(ctx.declaration());
    }

    @Override
    public ANode visitContinue(ContinueContext ctx) {
        return new SContinue(line(ctx), offset(ctx), location(ctx));
    }

    @Override
    public ANode visitBreak(BreakContext ctx) {
        return new SBreak(line(ctx), offset(ctx), location(ctx));
    }

    @Override
    public ANode visitReturn(ReturnContext ctx) {
        AExpression expression = (AExpression)visit(ctx.expression());

        return new SReturn(line(ctx), offset(ctx), location(ctx), expression);
    }

    @Override
    public ANode visitTry(TryContext ctx) {
        SBlock block = (SBlock)visit(ctx.block());
        List<SCatch> catches = new ArrayList<>();

        for (TrapContext trap : ctx.trap()) {
            catches.add((SCatch)visit(trap));
        }

        return new STry(line(ctx), offset(ctx), location(ctx), block, catches);
    }

    @Override
    public ANode visitThrow(ThrowContext ctx) {
        AExpression expression = (AExpression)visit(ctx.expression());

        return new SThrow(line(ctx), offset(ctx), location(ctx), expression);
    }

    @Override
    public ANode visitExpr(ExprContext ctx) {
        AExpression expression = (AExpression)visit(ctx.expression());

        return new SExpression(line(ctx), offset(ctx), location(ctx), expression);
    }

    @Override
    public ANode visitTrailer(TrailerContext ctx) {
        if (ctx.block() != null) {
            return visit(ctx.block());
        } else if (ctx.statement() != null) {
            List<AStatement> statements = new ArrayList<>();
            statements.add((AStatement)visit(ctx.statement()));

            return new SBlock(line(ctx), offset(ctx), location(ctx), statements);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + " Illegal tree structure.");
        }
    }

    @Override
    public ANode visitBlock(BlockContext ctx) {
        if (ctx.statement().isEmpty()) {
            return null;
        } else {
            List<AStatement> statements = new ArrayList<>();

            for (StatementContext statement : ctx.statement()) {
                statements.add((AStatement)visit(statement));
            }

            return new SBlock(line(ctx), offset(ctx), location(ctx), statements);
        }
    }

    @Override
    public ANode visitEmpty(EmptyContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + " Illegal tree structure.");
    }

    @Override
    public ANode visitInitializer(InitializerContext ctx) {
        if (ctx.declaration() != null) {
            return visit(ctx.declaration());
        } else if (ctx.expression() != null) {
            return visit(ctx.expression());
        } else {
            throw new IllegalStateException("Error " + location(ctx) + " Illegal tree structure.");
        }
    }

    @Override
    public ANode visitAfterthought(AfterthoughtContext ctx) {
        return visit(ctx.expression());
    }

    @Override
    public ANode visitDeclaration(DeclarationContext ctx) {
        String type = ctx.decltype().getText();
        List<SDeclaration> declarations = new ArrayList<>();

        for (DeclvarContext declvar : ctx.declvar()) {
            String name = declvar.ID().getText();
            AExpression expression = declvar.expression() == null ? null : (AExpression)visit(declvar.expression());

            declarations.add(new SDeclaration(line(ctx), offset(ctx), location(ctx), type, name, expression));
        }

        return new SDeclBlock(line(ctx), offset(ctx), location(ctx), declarations);
    }

    @Override
    public ANode visitDecltype(DecltypeContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + " Illegal tree structure.");
    }

    @Override
    public ANode visitType(TypeContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + " Illegal tree structure.");
    }

    @Override
    public ANode visitDeclvar(DeclvarContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + " Illegal tree structure.");
    }

    @Override
    public ANode visitTrap(TrapContext ctx) {
        String type = ctx.type().getText();
        String name = ctx.ID().getText();
        SBlock block = (SBlock)visit(ctx.block());

        return new SCatch(line(ctx), offset(ctx), location(ctx), type, name, block);
    }

    @Override
    public ANode visitDelimiter(DelimiterContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + " Illegal tree structure.");
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
    public ANode visitDynamic(DynamicContext ctx) {

    }

    @Override
    public ANode visitStatic(PainlessParser.StaticContext ctx) {
        return null;
    }

    @Override
    public ANode visitNewarray(PainlessParser.NewarrayContext ctx) {
        return null;
    }

    @Override
    public ANode visitExprprec(ExprprecContext ctx) {
        return visit(ctx.expression());
    }

    @Override
    public ANode visitChainprec(ChainprecContext ctx) {
        return visit(ctx.unary());
    }

    @Override
    public ANode visitString(StringContext ctx) {
        String string = ctx.STRING().getText();

        return new LString(line(ctx), offset(ctx), location(ctx), string);
    }

    @Override
    public ANode visitVariable(VariableContext ctx) {
        String name = ctx.ID().getText();

        return new LVariable(line(ctx), offset(ctx), location(ctx), name);
    }

    @Override
    public ANode visitNewobject(NewobjectContext ctx) {
        String type = ctx.type().getText();
        List<AExpression> arguments = new ArrayList<>();

        for (ExpressionContext expression : ctx.arguments().expression()) {
            arguments.add((AExpression)visit(expression));
        }

        return new LNewObj(line(ctx), offset(ctx), location(ctx), type, arguments);
    }

    @Override
    public ANode visitSecondary(SecondaryContext ctx) {
        if (ctx.dot() != null) {
            return visit(ctx.dot());
        } else if (ctx.brace() != null) {
            return visit(ctx.brace());
        } else {
            throw new IllegalStateException("Error " + location(ctx) + " Illegal tree structure.");
        }
    }

    @Override
    public ANode visitCallinvoke(CallinvokeContext ctx) {
        String name = ctx.DOTID().getText();
        List<AExpression> arguments = new ArrayList<>();

        for (ExpressionContext expression : ctx.arguments().expression()) {
            arguments.add((AExpression)visit(expression));
        }

        return new LCall(line(ctx), offset(ctx), location(ctx), name, arguments);
    }

    @Override
    public ANode visitFieldaccess(FieldaccessContext ctx) {
        final String value;

        if (ctx.DOTID() != null) {
            value = ctx.DOTID().getText();
        } else if (ctx.DOTINTEGER() != null) {
            value = ctx.DOTINTEGER().getText();
        } else {
            throw new IllegalStateException("Error " + location(ctx) + " Illegal tree structure.");
        }

        return new LField(line(ctx), offset(ctx), location(ctx), value);
    }

    @Override
    public ANode visitBraceaccess(BraceaccessContext ctx) {
        AExpression expression = (AExpression)visit(ctx.expression());

        return new LBrace(line(ctx), offset(ctx), location(ctx), expression);
    }

    @Override
    public ANode visitArguments(PainlessParser.ArgumentsContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + " Illegal tree structure.");
    }
}
