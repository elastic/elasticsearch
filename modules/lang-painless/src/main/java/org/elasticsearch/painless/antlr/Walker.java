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
import org.antlr.v4.runtime.tree.TerminalNode;
import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Locals.ExecuteReserved;
import org.elasticsearch.painless.Locals.FunctionReserved;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.Locals.Reserved;
import org.elasticsearch.painless.antlr.PainlessParser.AfterthoughtContext;
import org.elasticsearch.painless.antlr.PainlessParser.ArgumentContext;
import org.elasticsearch.painless.antlr.PainlessParser.ArgumentsContext;
import org.elasticsearch.painless.antlr.PainlessParser.AssignmentContext;
import org.elasticsearch.painless.antlr.PainlessParser.BinaryContext;
import org.elasticsearch.painless.antlr.PainlessParser.BlockContext;
import org.elasticsearch.painless.antlr.PainlessParser.BoolContext;
import org.elasticsearch.painless.antlr.PainlessParser.BraceaccessContext;
import org.elasticsearch.painless.antlr.PainlessParser.BreakContext;
import org.elasticsearch.painless.antlr.PainlessParser.CallinvokeContext;
import org.elasticsearch.painless.antlr.PainlessParser.CalllocalContext;
import org.elasticsearch.painless.antlr.PainlessParser.CapturingFuncrefContext;
import org.elasticsearch.painless.antlr.PainlessParser.CastContext;
import org.elasticsearch.painless.antlr.PainlessParser.ChainprecContext;
import org.elasticsearch.painless.antlr.PainlessParser.ClassFuncrefContext;
import org.elasticsearch.painless.antlr.PainlessParser.CompContext;
import org.elasticsearch.painless.antlr.PainlessParser.ConditionalContext;
import org.elasticsearch.painless.antlr.PainlessParser.ConstructorFuncrefContext;
import org.elasticsearch.painless.antlr.PainlessParser.ContinueContext;
import org.elasticsearch.painless.antlr.PainlessParser.DeclContext;
import org.elasticsearch.painless.antlr.PainlessParser.DeclarationContext;
import org.elasticsearch.painless.antlr.PainlessParser.DecltypeContext;
import org.elasticsearch.painless.antlr.PainlessParser.DeclvarContext;
import org.elasticsearch.painless.antlr.PainlessParser.DelimiterContext;
import org.elasticsearch.painless.antlr.PainlessParser.DoContext;
import org.elasticsearch.painless.antlr.PainlessParser.DynamicContext;
import org.elasticsearch.painless.antlr.PainlessParser.EachContext;
import org.elasticsearch.painless.antlr.PainlessParser.EmptyContext;
import org.elasticsearch.painless.antlr.PainlessParser.ExprContext;
import org.elasticsearch.painless.antlr.PainlessParser.ExpressionContext;
import org.elasticsearch.painless.antlr.PainlessParser.ExprprecContext;
import org.elasticsearch.painless.antlr.PainlessParser.FalseContext;
import org.elasticsearch.painless.antlr.PainlessParser.FieldaccessContext;
import org.elasticsearch.painless.antlr.PainlessParser.ForContext;
import org.elasticsearch.painless.antlr.PainlessParser.FuncrefContext;
import org.elasticsearch.painless.antlr.PainlessParser.FunctionContext;
import org.elasticsearch.painless.antlr.PainlessParser.IfContext;
import org.elasticsearch.painless.antlr.PainlessParser.InitializerContext;
import org.elasticsearch.painless.antlr.PainlessParser.LambdaContext;
import org.elasticsearch.painless.antlr.PainlessParser.LamtypeContext;
import org.elasticsearch.painless.antlr.PainlessParser.LocalFuncrefContext;
import org.elasticsearch.painless.antlr.PainlessParser.NewarrayContext;
import org.elasticsearch.painless.antlr.PainlessParser.NewobjectContext;
import org.elasticsearch.painless.antlr.PainlessParser.NullContext;
import org.elasticsearch.painless.antlr.PainlessParser.NumericContext;
import org.elasticsearch.painless.antlr.PainlessParser.OperatorContext;
import org.elasticsearch.painless.antlr.PainlessParser.ParametersContext;
import org.elasticsearch.painless.antlr.PainlessParser.PostContext;
import org.elasticsearch.painless.antlr.PainlessParser.PreContext;
import org.elasticsearch.painless.antlr.PainlessParser.ReadContext;
import org.elasticsearch.painless.antlr.PainlessParser.RegexContext;
import org.elasticsearch.painless.antlr.PainlessParser.ReturnContext;
import org.elasticsearch.painless.antlr.PainlessParser.SecondaryContext;
import org.elasticsearch.painless.antlr.PainlessParser.SingleContext;
import org.elasticsearch.painless.antlr.PainlessParser.SourceContext;
import org.elasticsearch.painless.antlr.PainlessParser.StatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.StaticContext;
import org.elasticsearch.painless.antlr.PainlessParser.StringContext;
import org.elasticsearch.painless.antlr.PainlessParser.ThrowContext;
import org.elasticsearch.painless.antlr.PainlessParser.TrailerContext;
import org.elasticsearch.painless.antlr.PainlessParser.TrapContext;
import org.elasticsearch.painless.antlr.PainlessParser.TrueContext;
import org.elasticsearch.painless.antlr.PainlessParser.TryContext;
import org.elasticsearch.painless.antlr.PainlessParser.UnaryContext;
import org.elasticsearch.painless.antlr.PainlessParser.VariableContext;
import org.elasticsearch.painless.antlr.PainlessParser.WhileContext;
import org.elasticsearch.painless.node.AExpression;
import org.elasticsearch.painless.node.ALink;
import org.elasticsearch.painless.node.ANode;
import org.elasticsearch.painless.node.AStatement;
import org.elasticsearch.painless.node.EBinary;
import org.elasticsearch.painless.node.EBool;
import org.elasticsearch.painless.node.EBoolean;
import org.elasticsearch.painless.node.ECapturingFunctionRef;
import org.elasticsearch.painless.node.EChain;
import org.elasticsearch.painless.node.EComp;
import org.elasticsearch.painless.node.EConditional;
import org.elasticsearch.painless.node.EDecimal;
import org.elasticsearch.painless.node.EExplicit;
import org.elasticsearch.painless.node.EFunctionRef;
import org.elasticsearch.painless.node.ENull;
import org.elasticsearch.painless.node.ENumeric;
import org.elasticsearch.painless.node.EUnary;
import org.elasticsearch.painless.node.LBrace;
import org.elasticsearch.painless.node.LCallInvoke;
import org.elasticsearch.painless.node.LCallLocal;
import org.elasticsearch.painless.node.LCast;
import org.elasticsearch.painless.node.LField;
import org.elasticsearch.painless.node.LNewArray;
import org.elasticsearch.painless.node.LNewObj;
import org.elasticsearch.painless.node.LRegex;
import org.elasticsearch.painless.node.LStatic;
import org.elasticsearch.painless.node.LString;
import org.elasticsearch.painless.node.LVariable;
import org.elasticsearch.painless.node.SBlock;
import org.elasticsearch.painless.node.SBreak;
import org.elasticsearch.painless.node.SCatch;
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
import org.elasticsearch.painless.node.SSource;
import org.elasticsearch.painless.node.SThrow;
import org.elasticsearch.painless.node.STry;
import org.elasticsearch.painless.node.SWhile;
import org.objectweb.asm.util.Printer;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;

/**
 * Converts the ANTLR tree to a Painless tree.
 */
public final class Walker extends PainlessParserBaseVisitor<Object> {

    public static SSource buildPainlessTree(String sourceName, String sourceText, CompilerSettings settings, Printer debugStream) {
        return new Walker(sourceName, sourceText, settings, debugStream).source;
    }

    private final SSource source;
    private final CompilerSettings settings;
    private final Printer debugStream;
    private final String sourceName;
    private final String sourceText;

    private final Deque<Reserved> reserved = new ArrayDeque<>();
    private final List<SFunction> synthetic = new ArrayList<>();
    private int syntheticCounter = 0;

    private Walker(String sourceName, String sourceText, CompilerSettings settings, Printer debugStream) {
        this.debugStream = debugStream;
        this.settings = settings;
        this.sourceName = Location.computeSourceName(sourceName, sourceText);
        this.sourceText = sourceText;
        this.source = (SSource)visit(buildAntlrTree(sourceText));
    }

    private SourceContext buildAntlrTree(String source) {
        ANTLRInputStream stream = new ANTLRInputStream(source);
        PainlessLexer lexer = new EnhancedPainlessLexer(stream, sourceName);
        PainlessParser parser = new PainlessParser(new CommonTokenStream(lexer));
        ParserErrorStrategy strategy = new ParserErrorStrategy(sourceName);

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

    private Location location(ParserRuleContext ctx) {
        return new Location(sourceName, ctx.getStart().getStartIndex());
    }

    @Override
    public Object visitSource(SourceContext ctx) {
        reserved.push(new ExecuteReserved());

        List<SFunction> functions = new ArrayList<>();

        for (FunctionContext function : ctx.function()) {
            functions.add((SFunction)visit(function));
        }

        List<AStatement> statements = new ArrayList<>();

        for (StatementContext statement : ctx.statement()) {
            statements.add((AStatement)visit(statement));
        }
        
        functions.addAll(synthetic);

        return new SSource(sourceName, sourceText, debugStream, 
                          (ExecuteReserved)reserved.pop(), location(ctx), functions, statements);
    }

    @Override
    public Object visitFunction(FunctionContext ctx) {
        reserved.push(new FunctionReserved());

        String rtnType = ctx.decltype().getText();
        String name = ctx.ID().getText();
        List<String> paramTypes = new ArrayList<>();
        List<String> paramNames = new ArrayList<>();
        List<AStatement> statements = new ArrayList<>();

        for (DecltypeContext decltype : ctx.parameters().decltype()) {
            paramTypes.add(decltype.getText());
        }

        for (TerminalNode id : ctx.parameters().ID()) {
            paramNames.add(id.getText());
        }

        for (StatementContext statement : ctx.block().statement()) {
            statements.add((AStatement)visit(statement));
        }

        return new SFunction((FunctionReserved)reserved.pop(), location(ctx), rtnType, name, paramTypes, paramNames, statements, false);
    }

    @Override
    public Object visitParameters(ParametersContext ctx) {
        throw location(ctx).createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    public Object visitIf(IfContext ctx) {
        AExpression expression = (AExpression)visitExpression(ctx.expression());
        SBlock ifblock = (SBlock)visit(ctx.trailer(0));

        if (ctx.trailer().size() > 1) {
            SBlock elseblock = (SBlock)visit(ctx.trailer(1));

            return new SIfElse(location(ctx), expression, ifblock, elseblock);
        } else {
            return new SIf(location(ctx), expression, ifblock);
        }
    }

    @Override
    public Object visitWhile(WhileContext ctx) {
        reserved.peek().setMaxLoopCounter(settings.getMaxLoopCounter());

        AExpression expression = (AExpression)visitExpression(ctx.expression());

        if (ctx.trailer() != null) {
            SBlock block = (SBlock)visit(ctx.trailer());

            return new SWhile(location(ctx), expression, block);
        } else if (ctx.empty() != null) {
            return new SWhile(location(ctx), expression, null);
        } else {
            throw location(ctx).createError(new IllegalStateException(" Illegal tree structure."));
        }
    }

    @Override
    public Object visitDo(DoContext ctx) {
        reserved.peek().setMaxLoopCounter(settings.getMaxLoopCounter());

        AExpression expression = (AExpression)visitExpression(ctx.expression());
        SBlock block = (SBlock)visit(ctx.block());

        return new SDo(location(ctx), block, expression);
    }

    @Override
    public Object visitFor(ForContext ctx) {
        reserved.peek().setMaxLoopCounter(settings.getMaxLoopCounter());

        ANode initializer = ctx.initializer() == null ? null : (ANode)visit(ctx.initializer());
        AExpression expression = ctx.expression() == null ? null : (AExpression)visitExpression(ctx.expression());
        AExpression afterthought = ctx.afterthought() == null ? null : (AExpression)visit(ctx.afterthought());

        if (ctx.trailer() != null) {
            SBlock block = (SBlock)visit(ctx.trailer());

            return new SFor(location(ctx), initializer, expression, afterthought, block);
        } else if (ctx.empty() != null) {
            return new SFor(location(ctx), initializer, expression, afterthought, null);
        } else {
            throw location(ctx).createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    @Override
    public Object visitEach(EachContext ctx) {
        reserved.peek().setMaxLoopCounter(settings.getMaxLoopCounter());

        String type = ctx.decltype().getText();
        String name = ctx.ID().getText();
        AExpression expression = (AExpression)visitExpression(ctx.expression());
        SBlock block = (SBlock)visit(ctx.trailer());

        return new SEach(location(ctx), type, name, expression, block);
    }

    @Override
    public Object visitDecl(DeclContext ctx) {
        return visit(ctx.declaration());
    }

    @Override
    public Object visitContinue(ContinueContext ctx) {
        return new SContinue(location(ctx));
    }

    @Override
    public Object visitBreak(BreakContext ctx) {
        return new SBreak(location(ctx));
    }

    @Override
    public Object visitReturn(ReturnContext ctx) {
        AExpression expression = (AExpression)visitExpression(ctx.expression());

        return new SReturn(location(ctx), expression);
    }

    @Override
    public Object visitTry(TryContext ctx) {
        SBlock block = (SBlock)visit(ctx.block());
        List<SCatch> catches = new ArrayList<>();

        for (TrapContext trap : ctx.trap()) {
            catches.add((SCatch)visit(trap));
        }

        return new STry(location(ctx), block, catches);
    }

    @Override
    public Object visitThrow(ThrowContext ctx) {
        AExpression expression = (AExpression)visitExpression(ctx.expression());

        return new SThrow(location(ctx), expression);
    }

    @Override
    public Object visitExpr(ExprContext ctx) {
        AExpression expression = (AExpression)visitExpression(ctx.expression());

        return new SExpression(location(ctx), expression);
    }

    @Override
    public Object visitTrailer(TrailerContext ctx) {
        if (ctx.block() != null) {
            return visit(ctx.block());
        } else if (ctx.statement() != null) {
            List<AStatement> statements = new ArrayList<>();
            statements.add((AStatement)visit(ctx.statement()));

            return new SBlock(location(ctx), statements);
        } else {
            throw location(ctx).createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    @Override
    public Object visitBlock(BlockContext ctx) {
        if (ctx.statement().isEmpty()) {
            return null;
        } else {
            List<AStatement> statements = new ArrayList<>();

            for (StatementContext statement : ctx.statement()) {
                statements.add((AStatement)visit(statement));
            }

            return new SBlock(location(ctx), statements);
        }
    }

    @Override
    public Object visitEmpty(EmptyContext ctx) {
        throw location(ctx).createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    public Object visitInitializer(InitializerContext ctx) {
        if (ctx.declaration() != null) {
            return visit(ctx.declaration());
        } else if (ctx.expression() != null) {
            return visitExpression(ctx.expression());
        } else {
            throw location(ctx).createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    @Override
    public Object visitAfterthought(AfterthoughtContext ctx) {
        return visitExpression(ctx.expression());
    }

    @Override
    public Object visitDeclaration(DeclarationContext ctx) {
        String type = ctx.decltype().getText();
        List<SDeclaration> declarations = new ArrayList<>();

        for (DeclvarContext declvar : ctx.declvar()) {
            String name = declvar.ID().getText();
            AExpression expression = declvar.expression() == null ? null : (AExpression)visitExpression(declvar.expression());

            declarations.add(new SDeclaration(location(declvar), type, name, expression));
        }

        return new SDeclBlock(location(ctx), declarations);
    }

    @Override
    public Object visitDecltype(DecltypeContext ctx) {
        throw location(ctx).createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    public Object visitDeclvar(DeclvarContext ctx) {
        throw location(ctx).createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    public Object visitTrap(TrapContext ctx) {
        String type = ctx.TYPE().getText();
        String name = ctx.ID().getText();
        SBlock block = (SBlock)visit(ctx.block());

        return new SCatch(location(ctx), type, name, block);
    }

    @Override
    public Object visitDelimiter(DelimiterContext ctx) {
        throw location(ctx).createError(new IllegalStateException("Illegal tree structure."));
    }

    private Object visitExpression(ExpressionContext ctx) {
        Object expression = visit(ctx);

        if (expression instanceof List) {
            @SuppressWarnings("unchecked")
            List<ALink> links = (List<ALink>)expression;

            return new EChain(location(ctx), links, false, false, null, null);
        } else {
            return expression;
        }
    }

    @Override
    public Object visitSingle(SingleContext ctx) {
        return visit(ctx.unary());
    }

    @Override
    public Object visitBinary(BinaryContext ctx) {
        AExpression left = (AExpression)visitExpression(ctx.expression(0));
        AExpression right = (AExpression)visitExpression(ctx.expression(1));
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
        } else if (ctx.FIND() != null) {
            operation = Operation.FIND;
        } else if (ctx.MATCH() != null) {
            operation = Operation.MATCH;
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
            throw location(ctx).createError(new IllegalStateException("Unexpected state."));
        }

        return new EBinary(location(ctx), operation, left, right);
    }

    @Override
    public Object visitComp(CompContext ctx) {
        AExpression left = (AExpression)visitExpression(ctx.expression(0));
        AExpression right = (AExpression)visitExpression(ctx.expression(1));
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
            throw location(ctx).createError(new IllegalStateException("Unexpected state."));
        }

        return new EComp(location(ctx), operation, left, right);
    }

    @Override
    public Object visitBool(BoolContext ctx) {
        AExpression left = (AExpression)visitExpression(ctx.expression(0));
        AExpression right = (AExpression)visitExpression(ctx.expression(1));
        final Operation operation;

        if (ctx.BOOLAND() != null) {
            operation = Operation.AND;
        } else if (ctx.BOOLOR() != null) {
            operation = Operation.OR;
        } else {
            throw location(ctx).createError(new IllegalStateException("Unexpected state."));
        }

        return new EBool(location(ctx), operation, left, right);
    }

    @Override
    public Object visitConditional(ConditionalContext ctx) {
        AExpression condition = (AExpression)visitExpression(ctx.expression(0));
        AExpression left = (AExpression)visitExpression(ctx.expression(1));
        AExpression right = (AExpression)visitExpression(ctx.expression(2));

        return new EConditional(location(ctx), condition, left, right);
    }

    @Override
    public Object visitAssignment(AssignmentContext ctx) {
        @SuppressWarnings("unchecked")
        List<ALink> links = (List<ALink>)visit(ctx.chain());
        final Operation operation;

        if (ctx.ASSIGN() != null) {
            operation = null;
        } else if (ctx.AMUL() != null) {
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
            throw location(ctx).createError(new IllegalStateException("Illegal tree structure."));
        }

        AExpression expression = (AExpression)visitExpression(ctx.expression());

        return new EChain(location(ctx), links, false, false, operation, expression);
    }

    private Object visitUnary(UnaryContext ctx) {
        Object expression = visit(ctx);

        if (expression instanceof List) {
            @SuppressWarnings("unchecked")
            List<ALink> links = (List<ALink>)expression;

            return new EChain(location(ctx), links, false, false, null, null);
        } else {
            return expression;
        }
    }

    @Override
    public Object visitPre(PreContext ctx) {
        @SuppressWarnings("unchecked")
        List<ALink> links = (List<ALink>)visit(ctx.chain());
        final Operation operation;

        if (ctx.INCR() != null) {
            operation = Operation.INCR;
        } else if (ctx.DECR() != null) {
            operation = Operation.DECR;
        } else {
            throw location(ctx).createError(new IllegalStateException("Illegal tree structure."));
        }

        return new EChain(location(ctx), links, true, false, operation, null);
    }

    @Override
    public Object visitPost(PostContext ctx) {
        @SuppressWarnings("unchecked")
        List<ALink> links = (List<ALink>)visit(ctx.chain());
        final Operation operation;

        if (ctx.INCR() != null) {
            operation = Operation.INCR;
        } else if (ctx.DECR() != null) {
            operation = Operation.DECR;
        } else {
            throw location(ctx).createError(new IllegalStateException("Illegal tree structure."));
        }

        return new EChain(location(ctx), links, false, true, operation, null);
    }

    @Override
    public Object visitRead(ReadContext ctx) {
        return visit(ctx.chain());
    }

    @Override
    public Object visitNumeric(NumericContext ctx) {
        final boolean negate = ctx.parent instanceof OperatorContext && ((OperatorContext)ctx.parent).SUB() != null;

        if (ctx.DECIMAL() != null) {
            return new EDecimal(location(ctx), (negate ? "-" : "") + ctx.DECIMAL().getText());
        } else if (ctx.HEX() != null) {
            return new ENumeric(location(ctx), (negate ? "-" : "") + ctx.HEX().getText().substring(2), 16);
        } else if (ctx.INTEGER() != null) {
            return new ENumeric(location(ctx), (negate ? "-" : "") + ctx.INTEGER().getText(), 10);
        } else if (ctx.OCTAL() != null) {
            return new ENumeric(location(ctx), (negate ? "-" : "") + ctx.OCTAL().getText().substring(1), 8);
        } else {
            throw location(ctx).createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    @Override
    public Object visitTrue(TrueContext ctx) {
        return new EBoolean(location(ctx), true);
    }

    @Override
    public Object visitFalse(FalseContext ctx) {
        return new EBoolean(location(ctx), false);
    }

    @Override
    public Object visitNull(NullContext ctx) {
        return new ENull(location(ctx));
    }

    @Override
    public Object visitOperator(OperatorContext ctx) {
        if (ctx.SUB() != null && ctx.unary() instanceof NumericContext) {
            return visit(ctx.unary());
        } else {
            AExpression expression = (AExpression)visitUnary(ctx.unary());
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
                throw location(ctx).createError(new IllegalStateException("Illegal tree structure."));
            }

            return new EUnary(location(ctx), operation, expression);
        }
    }

    @Override
    public Object visitCast(CastContext ctx) {
        String type = ctx.decltype().getText();
        Object child = visit(ctx.unary());

        if (child instanceof List) {
            @SuppressWarnings("unchecked")
            List<ALink> links = (List<ALink>)child;
            links.add(new LCast(location(ctx), type));

            return links;
        } else {
            return new EExplicit(location(ctx), type, (AExpression)child);
        }
    }

    @Override
    public Object visitDynamic(DynamicContext ctx) {
        Object child = visit(ctx.primary());

        if (child instanceof List) {
            @SuppressWarnings("unchecked")
            List<ALink> links = (List<ALink>)child;

            for (SecondaryContext secondary : ctx.secondary()) {
                links.add((ALink)visit(secondary));
            }

            return links;
        } else if (!ctx.secondary().isEmpty()) {
            throw location(ctx).createError(new IllegalStateException("Illegal tree structure."));
        } else {
            return child;
        }
    }

    @Override
    public Object visitStatic(StaticContext ctx) {
        String type = ctx.decltype().getText();
        List<ALink> links = new ArrayList<>();

        links.add(new LStatic(location(ctx), type));
        links.add((ALink)visit(ctx.dot()));

        for (SecondaryContext secondary : ctx.secondary()) {
            links.add((ALink)visit(secondary));
        }

        return links;
    }

    @Override
    public Object visitNewarray(NewarrayContext ctx) {
        String type = ctx.TYPE().getText();
        List<AExpression> expressions = new ArrayList<>();

        for (ExpressionContext expression : ctx.expression()) {
            expressions.add((AExpression)visitExpression(expression));
        }

        List<ALink> links = new ArrayList<>();
        links.add(new LNewArray(location(ctx), type, expressions));

        if (ctx.dot() != null) {
            links.add((ALink)visit(ctx.dot()));

            for (SecondaryContext secondary : ctx.secondary()) {
                links.add((ALink)visit(secondary));
            }
        } else if (!ctx.secondary().isEmpty()) {
            throw location(ctx).createError(new IllegalStateException("Illegal tree structure."));
        }

        return links;
    }

    @Override
    public Object visitExprprec(ExprprecContext ctx) {
        return visit(ctx.expression());
    }

    @Override
    public Object visitChainprec(ChainprecContext ctx) {
        return visit(ctx.unary());
    }

    @Override
    public Object visitString(StringContext ctx) {
        String string = ctx.STRING().getText().substring(1, ctx.STRING().getText().length() - 1);
        List<ALink> links = new ArrayList<>();
        links.add(new LString(location(ctx), string));

        return links;
    }

    @Override
    public Object visitRegex(RegexContext ctx) {
        String text = ctx.REGEX().getText();
        int lastSlash = text.lastIndexOf('/');
        String pattern = text.substring(1, lastSlash);
        String flags = text.substring(lastSlash + 1);
        List<ALink> links = new ArrayList<>();
        links.add(new LRegex(location(ctx), pattern, flags));

        return links;
    }

    @Override
    public Object visitVariable(VariableContext ctx) {
        String name = ctx.ID().getText();
        List<ALink> links = new ArrayList<>();
        links.add(new LVariable(location(ctx), name));

        reserved.peek().markReserved(name);

        return links;
    }

    @Override
    public Object visitCalllocal(CalllocalContext ctx) {
        String name = ctx.ID().getText();
        @SuppressWarnings("unchecked")
        List<AExpression> arguments = (List<AExpression>)visit(ctx.arguments());
        List<ALink> links = new ArrayList<>();
        links.add(new LCallLocal(location(ctx), name, arguments));

        return links;
    }

    @Override
    public Object visitNewobject(NewobjectContext ctx) {
        String type = ctx.TYPE().getText();
        @SuppressWarnings("unchecked")
        List<AExpression> arguments = (List<AExpression>)visit(ctx.arguments());

        List<ALink> links = new ArrayList<>();
        links.add(new LNewObj(location(ctx), type, arguments));

        return links;
    }

    @Override
    public Object visitSecondary(SecondaryContext ctx) {
        if (ctx.dot() != null) {
            return visit(ctx.dot());
        } else if (ctx.brace() != null) {
            return visit(ctx.brace());
        } else {
            throw location(ctx).createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    @Override
    public Object visitCallinvoke(CallinvokeContext ctx) {
        String name = ctx.DOTID().getText();
        @SuppressWarnings("unchecked")
        List<AExpression> arguments = (List<AExpression>)visit(ctx.arguments());

        return new LCallInvoke(location(ctx), name, arguments);
    }

    @Override
    public Object visitFieldaccess(FieldaccessContext ctx) {
        final String value;

        if (ctx.DOTID() != null) {
            value = ctx.DOTID().getText();
        } else if (ctx.DOTINTEGER() != null) {
            value = ctx.DOTINTEGER().getText();
        } else {
            throw location(ctx).createError(new IllegalStateException("Illegal tree structure."));
        }

        return new LField(location(ctx), value);
    }

    @Override
    public Object visitBraceaccess(BraceaccessContext ctx) {
        AExpression expression = (AExpression)visitExpression(ctx.expression());

        return new LBrace(location(ctx), expression);
    }

    @Override
    public Object visitArguments(ArgumentsContext ctx) {
        List<AExpression> arguments = new ArrayList<>();

        for (ArgumentContext argument : ctx.argument()) {
            arguments.add((AExpression)visit(argument));
        }

        return arguments;
    }

    @Override
    public Object visitArgument(ArgumentContext ctx) {
        if (ctx.expression() != null) {
            return visitExpression(ctx.expression());
        } else if (ctx.lambda() != null) {
            return visit(ctx.lambda());
        } else if (ctx.funcref() != null) {
            return visit(ctx.funcref());
        } else {
            throw location(ctx).createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    @Override
    public Object visitLambda(LambdaContext ctx) {
        reserved.push(new FunctionReserved());

        List<String> paramTypes = new ArrayList<>();
        List<String> paramNames = new ArrayList<>();
        List<AStatement> statements = new ArrayList<>();

        for (LamtypeContext lamtype : ctx.lamtype()) {
            if (lamtype.decltype() == null) {
                paramTypes.add("def");
            } else {
                paramTypes.add(lamtype.decltype().getText());
            }

            paramNames.add(lamtype.ID().getText());
        }

        if (ctx.expression() != null) {
            // single expression
            AExpression expression = (AExpression) visitExpression(ctx.expression());
            statements.add(new SReturn(location(ctx), expression));
        } else {
            for (StatementContext statement : ctx.block().statement()) {
                statements.add((AStatement)visit(statement));
            }
        }
        
        String name = nextLambda();
        synthetic.add(new SFunction((FunctionReserved)reserved.pop(), location(ctx), "def", name, 
                      paramTypes, paramNames, statements, true));
        return new EFunctionRef(location(ctx), "this", name);
        // TODO: use a real node for captures and shit
    }

    @Override
    public Object visitLamtype(LamtypeContext ctx) {
        throw location(ctx).createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    public Object visitFuncref(FuncrefContext ctx) {
        if (ctx.classFuncref() != null) {
            return visit(ctx.classFuncref());
        } else if (ctx.constructorFuncref() != null) {
            return visit(ctx.constructorFuncref());
        } else if (ctx.capturingFuncref() != null) {
            return visit(ctx.capturingFuncref());
        } else if (ctx.localFuncref() != null) {
            return visit(ctx.localFuncref());
        } else {
            throw location(ctx).createError(new IllegalStateException("Illegal tree structure."));
        }
    }

    @Override
    public Object visitClassFuncref(ClassFuncrefContext ctx) {
        return new EFunctionRef(location(ctx), ctx.TYPE().getText(), ctx.ID().getText());
    }

    @Override
    public Object visitConstructorFuncref(ConstructorFuncrefContext ctx) {
        if (!ctx.decltype().LBRACE().isEmpty()) {
            // array constructors are special: we need to make a synthetic method
            // taking integer as argument and returning a new instance, and return a ref to that.
            Location location = location(ctx);
            String arrayType = ctx.decltype().getText();
            SReturn code = new SReturn(location, 
                           new EChain(location,
                           new LNewArray(location, arrayType, Arrays.asList(
                           new EChain(location, 
                           new LVariable(location, "size"))))));
            String name = nextLambda();
            synthetic.add(new SFunction(new FunctionReserved(), location, arrayType, name, 
                          Arrays.asList("int"), Arrays.asList("size"), Arrays.asList(code), true));
            return new EFunctionRef(location(ctx), "this", name);
        }
        return new EFunctionRef(location(ctx), ctx.decltype().getText(), ctx.NEW().getText());
    }

    @Override
    public Object visitCapturingFuncref(CapturingFuncrefContext ctx) {
        return new ECapturingFunctionRef(location(ctx), ctx.ID(0).getText(), ctx.ID(1).getText());
    }

    @Override
    public Object visitLocalFuncref(LocalFuncrefContext ctx) {
        return new EFunctionRef(location(ctx), ctx.THIS().getText(), ctx.ID().getText());
    }
    
    /** Returns name of next lambda */
    private String nextLambda() {
        return "lambda$" + syntheticCounter++;
    }
}
