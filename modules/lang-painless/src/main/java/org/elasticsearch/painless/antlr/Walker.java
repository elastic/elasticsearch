/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.antlr.PainlessParser.AddsubContext;
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
import org.elasticsearch.painless.antlr.PainlessParser.CastContext;
import org.elasticsearch.painless.antlr.PainlessParser.ClassfuncrefContext;
import org.elasticsearch.painless.antlr.PainlessParser.CompContext;
import org.elasticsearch.painless.antlr.PainlessParser.ConditionalContext;
import org.elasticsearch.painless.antlr.PainlessParser.ConstructorfuncrefContext;
import org.elasticsearch.painless.antlr.PainlessParser.ContinueContext;
import org.elasticsearch.painless.antlr.PainlessParser.DeclContext;
import org.elasticsearch.painless.antlr.PainlessParser.DeclarationContext;
import org.elasticsearch.painless.antlr.PainlessParser.DecltypeContext;
import org.elasticsearch.painless.antlr.PainlessParser.DeclvarContext;
import org.elasticsearch.painless.antlr.PainlessParser.DoContext;
import org.elasticsearch.painless.antlr.PainlessParser.DynamicContext;
import org.elasticsearch.painless.antlr.PainlessParser.EachContext;
import org.elasticsearch.painless.antlr.PainlessParser.ElvisContext;
import org.elasticsearch.painless.antlr.PainlessParser.EmptyContext;
import org.elasticsearch.painless.antlr.PainlessParser.ExprContext;
import org.elasticsearch.painless.antlr.PainlessParser.ExpressionContext;
import org.elasticsearch.painless.antlr.PainlessParser.FalseContext;
import org.elasticsearch.painless.antlr.PainlessParser.FieldaccessContext;
import org.elasticsearch.painless.antlr.PainlessParser.ForContext;
import org.elasticsearch.painless.antlr.PainlessParser.FunctionContext;
import org.elasticsearch.painless.antlr.PainlessParser.IfContext;
import org.elasticsearch.painless.antlr.PainlessParser.IneachContext;
import org.elasticsearch.painless.antlr.PainlessParser.InitializerContext;
import org.elasticsearch.painless.antlr.PainlessParser.InstanceofContext;
import org.elasticsearch.painless.antlr.PainlessParser.LambdaContext;
import org.elasticsearch.painless.antlr.PainlessParser.LamtypeContext;
import org.elasticsearch.painless.antlr.PainlessParser.ListinitContext;
import org.elasticsearch.painless.antlr.PainlessParser.ListinitializerContext;
import org.elasticsearch.painless.antlr.PainlessParser.LocalfuncrefContext;
import org.elasticsearch.painless.antlr.PainlessParser.MapinitContext;
import org.elasticsearch.painless.antlr.PainlessParser.MapinitializerContext;
import org.elasticsearch.painless.antlr.PainlessParser.MaptokenContext;
import org.elasticsearch.painless.antlr.PainlessParser.NewarrayContext;
import org.elasticsearch.painless.antlr.PainlessParser.NewinitializedarrayContext;
import org.elasticsearch.painless.antlr.PainlessParser.NewobjectContext;
import org.elasticsearch.painless.antlr.PainlessParser.NewstandardarrayContext;
import org.elasticsearch.painless.antlr.PainlessParser.NonconditionalContext;
import org.elasticsearch.painless.antlr.PainlessParser.NotContext;
import org.elasticsearch.painless.antlr.PainlessParser.NotaddsubContext;
import org.elasticsearch.painless.antlr.PainlessParser.NullContext;
import org.elasticsearch.painless.antlr.PainlessParser.NumericContext;
import org.elasticsearch.painless.antlr.PainlessParser.ParametersContext;
import org.elasticsearch.painless.antlr.PainlessParser.PostContext;
import org.elasticsearch.painless.antlr.PainlessParser.PostdotContext;
import org.elasticsearch.painless.antlr.PainlessParser.PostfixContext;
import org.elasticsearch.painless.antlr.PainlessParser.PreContext;
import org.elasticsearch.painless.antlr.PainlessParser.PrecedenceContext;
import org.elasticsearch.painless.antlr.PainlessParser.ReadContext;
import org.elasticsearch.painless.antlr.PainlessParser.RegexContext;
import org.elasticsearch.painless.antlr.PainlessParser.ReturnContext;
import org.elasticsearch.painless.antlr.PainlessParser.SingleContext;
import org.elasticsearch.painless.antlr.PainlessParser.SourceContext;
import org.elasticsearch.painless.antlr.PainlessParser.StatementContext;
import org.elasticsearch.painless.antlr.PainlessParser.StringContext;
import org.elasticsearch.painless.antlr.PainlessParser.ThrowContext;
import org.elasticsearch.painless.antlr.PainlessParser.TrailerContext;
import org.elasticsearch.painless.antlr.PainlessParser.TrapContext;
import org.elasticsearch.painless.antlr.PainlessParser.TrueContext;
import org.elasticsearch.painless.antlr.PainlessParser.TryContext;
import org.elasticsearch.painless.antlr.PainlessParser.TypeContext;
import org.elasticsearch.painless.antlr.PainlessParser.VariableContext;
import org.elasticsearch.painless.antlr.PainlessParser.WhileContext;
import org.elasticsearch.painless.node.AExpression;
import org.elasticsearch.painless.node.ANode;
import org.elasticsearch.painless.node.AStatement;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Collections.emptyList;

/**
 * Converts the ANTLR tree to a Painless tree.
 */
public final class Walker extends PainlessParserBaseVisitor<ANode> {

    public static SClass buildPainlessTree(String sourceName, String sourceText, CompilerSettings settings) {
        return new Walker(sourceName, sourceText, settings).source;
    }

    private final CompilerSettings settings;
    private final String sourceName;

    private int identifier;

    private final SClass source;

    private Walker(String sourceName, String sourceText, CompilerSettings settings) {
        this.settings = settings;
        this.sourceName = sourceName;

        this.identifier = 0;

        this.source = (SClass)visit(buildAntlrTree(sourceText));
    }

    private int nextIdentifier() {
        return identifier++;
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

    private Location location(TerminalNode tn) {
        return new Location(sourceName, tn.getSymbol().getStartIndex());
    }

    @Override
    public ANode visitSource(SourceContext ctx) {
        List<SFunction> functions = new ArrayList<>();

        for (FunctionContext function : ctx.function()) {
            functions.add((SFunction)visit(function));
        }

        // handle the code to generate the execute method here
        // because the statements come loose from the grammar as
        // part of the overall class
        List<AStatement> statements = new ArrayList<>();

        for (StatementContext statement : ctx.statement()) {
            statements.add((AStatement)visit(statement));
        }

        // generate the execute method from the collected statements and parameters
        SFunction execute = new SFunction(nextIdentifier(), location(ctx), "<internal>", "execute", emptyList(), emptyList(),
                new SBlock(nextIdentifier(), location(ctx), statements), false, false, false, false);
        functions.add(execute);

        return new SClass(nextIdentifier(), location(ctx), functions);
    }

    @Override
    public ANode visitFunction(FunctionContext ctx) {
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

        if (ctx.block().dstatement() != null) {
            statements.add((AStatement)visit(ctx.block().dstatement()));
        }

        return new SFunction(nextIdentifier(), location(ctx),
                rtnType, name, paramTypes, paramNames, new SBlock(nextIdentifier(), location(ctx), statements), false, false, false, false);
    }

    @Override
    public ANode visitParameters(ParametersContext ctx) {
        throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
    }

    @Override
    public ANode visitStatement(StatementContext ctx) {
        if (ctx.rstatement() != null) {
            return visit(ctx.rstatement());
        } else if (ctx.dstatement() != null) {
            return visit(ctx.dstatement());
        } else {
            throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
        }
    }

    @Override
    public ANode visitIf(IfContext ctx) {
        AExpression expression = (AExpression)visit(ctx.expression());
        SBlock ifblock = (SBlock)visit(ctx.trailer(0));

        if (ctx.trailer().size() > 1) {
            SBlock elseblock = (SBlock)visit(ctx.trailer(1));

            return new SIfElse(nextIdentifier(), location(ctx), expression, ifblock, elseblock);
        } else {
            return new SIf(nextIdentifier(), location(ctx), expression, ifblock);
        }
    }

    @Override
    public ANode visitWhile(WhileContext ctx) {
        AExpression expression = (AExpression)visit(ctx.expression());

        if (ctx.trailer() != null) {
            SBlock block = (SBlock)visit(ctx.trailer());

            return new SWhile(nextIdentifier(), location(ctx), expression, block);
        } else if (ctx.empty() != null) {
            return new SWhile(nextIdentifier(), location(ctx), expression, null);
        } else {
            throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
        }
    }

    @Override
    public ANode visitDo(DoContext ctx) {
        AExpression expression = (AExpression)visit(ctx.expression());
        SBlock block = (SBlock)visit(ctx.block());

        return new SDo(nextIdentifier(), location(ctx), expression, block);
    }

    @Override
    public ANode visitFor(ForContext ctx) {
        ANode initializer = ctx.initializer() == null ? null : visit(ctx.initializer());
        AExpression expression = ctx.expression() == null ? null : (AExpression)visit(ctx.expression());
        AExpression afterthought = ctx.afterthought() == null ? null : (AExpression)visit(ctx.afterthought());

        if (ctx.trailer() != null) {
            SBlock block = (SBlock)visit(ctx.trailer());

            return new SFor(nextIdentifier(), location(ctx), initializer, expression, afterthought, block);
        } else if (ctx.empty() != null) {
            return new SFor(nextIdentifier(), location(ctx), initializer, expression, afterthought, null);
        } else {
            throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
        }
    }

    @Override
    public ANode visitEach(EachContext ctx) {
        String type = ctx.decltype().getText();
        String name = ctx.ID().getText();
        AExpression expression = (AExpression)visit(ctx.expression());
        SBlock block = (SBlock)visit(ctx.trailer());

        return new SEach(nextIdentifier(), location(ctx), type, name, expression, block);
    }

    @Override
    public ANode visitIneach(IneachContext ctx) {
        String name = ctx.ID().getText();
        AExpression expression = (AExpression)visit(ctx.expression());
        SBlock block = (SBlock)visit(ctx.trailer());

        return new SEach(nextIdentifier(), location(ctx), "def", name, expression, block);
    }

    @Override
    public ANode visitDecl(DeclContext ctx) {
        return visit(ctx.declaration());
    }

    @Override
    public ANode visitContinue(ContinueContext ctx) {
        return new SContinue(nextIdentifier(), location(ctx));
    }

    @Override
    public ANode visitBreak(BreakContext ctx) {
        return new SBreak(nextIdentifier(), location(ctx));
    }

    @Override
    public ANode visitReturn(ReturnContext ctx) {
        AExpression expression = null;

        if (ctx.expression() != null) {
            expression = (AExpression) visit(ctx.expression());
        }

        return new SReturn(nextIdentifier(), location(ctx), expression);
    }

    @Override
    public ANode visitTry(TryContext ctx) {
        SBlock block = (SBlock)visit(ctx.block());
        List<SCatch> catches = new ArrayList<>();

        for (TrapContext trap : ctx.trap()) {
            catches.add((SCatch)visit(trap));
        }

        return new STry(nextIdentifier(), location(ctx), block, catches);
    }

    @Override
    public ANode visitThrow(ThrowContext ctx) {
        AExpression expression = (AExpression)visit(ctx.expression());

        return new SThrow(nextIdentifier(), location(ctx), expression);
    }

    @Override
    public ANode visitExpr(ExprContext ctx) {
        AExpression expression = (AExpression)visit(ctx.expression());

        return new SExpression(nextIdentifier(), location(ctx), expression);
    }

    @Override
    public ANode visitTrailer(TrailerContext ctx) {
        if (ctx.block() != null) {
            return visit(ctx.block());
        } else if (ctx.statement() != null) {
            List<AStatement> statements = new ArrayList<>();
            statements.add((AStatement)visit(ctx.statement()));

            return new SBlock(nextIdentifier(), location(ctx), statements);
        } else {
            throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
        }
    }

    @Override
    public ANode visitBlock(BlockContext ctx) {
        if (ctx.statement().isEmpty() && ctx.dstatement() == null) {
            return null;
        } else {
            List<AStatement> statements = new ArrayList<>();

            for (StatementContext statement : ctx.statement()) {
                statements.add((AStatement)visit(statement));
            }

            if (ctx.dstatement() != null) {
                statements.add((AStatement)visit(ctx.dstatement()));
            }

            return new SBlock(nextIdentifier(), location(ctx), statements);
        }
    }

    @Override
    public ANode visitEmpty(EmptyContext ctx) {
        throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
    }

    @Override
    public ANode visitInitializer(InitializerContext ctx) {
        if (ctx.declaration() != null) {
            return visit(ctx.declaration());
        } else if (ctx.expression() != null) {
            return visit(ctx.expression());
        } else {
            throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
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
            declarations.add(new SDeclaration(nextIdentifier(), location(declvar), type, name, expression));
        }

        return new SDeclBlock(nextIdentifier(), location(ctx), declarations);
    }

    @Override
    public ANode visitDecltype(DecltypeContext ctx) {
        throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
    }

    @Override
    public ANode visitType(TypeContext ctx) {
        throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
    }

    @Override
    public ANode visitDeclvar(DeclvarContext ctx) {
        throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
    }

    @Override
    public ANode visitTrap(TrapContext ctx) {
        String type = ctx.type().getText();
        String name = ctx.ID().getText();
        SBlock block = (SBlock)visit(ctx.block());

        return new SCatch(nextIdentifier(), location(ctx), Exception.class, type, name, block);
    }

    @Override
    public ANode visitSingle(SingleContext ctx) {
        return visit(ctx.unary());
    }

    @Override
    public ANode visitBinary(BinaryContext ctx) {
        AExpression left = (AExpression)visit(ctx.noncondexpression(0));
        AExpression right = (AExpression)visit(ctx.noncondexpression(1));
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
            throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
        }

        return new EBinary(nextIdentifier(), location(ctx), left, right, operation);
    }

    @Override
    public ANode visitComp(CompContext ctx) {
        AExpression left = (AExpression)visit(ctx.noncondexpression(0));
        AExpression right = (AExpression)visit(ctx.noncondexpression(1));
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
            throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
        }

        return new EComp(nextIdentifier(), location(ctx), left, right, operation);
    }

    @Override
    public ANode visitInstanceof(InstanceofContext ctx) {
        AExpression expr = (AExpression)visit(ctx.noncondexpression());
        String type = ctx.decltype().getText();

        return new EInstanceof(nextIdentifier(), location(ctx), expr, type);
    }

    @Override
    public ANode visitBool(BoolContext ctx) {
        AExpression left = (AExpression)visit(ctx.noncondexpression(0));
        AExpression right = (AExpression)visit(ctx.noncondexpression(1));
        final Operation operation;

        if (ctx.BOOLAND() != null) {
            operation = Operation.AND;
        } else if (ctx.BOOLOR() != null) {
            operation = Operation.OR;
        } else {
            throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
        }

        return new EBooleanComp(nextIdentifier(), location(ctx), left, right, operation);
    }

    @Override
    public ANode visitElvis(ElvisContext ctx) {
        AExpression left = (AExpression)visit(ctx.noncondexpression(0));
        AExpression right = (AExpression)visit(ctx.noncondexpression(1));

        return new EElvis(nextIdentifier(), location(ctx), left, right);
    }

    @Override
    public ANode visitNonconditional(NonconditionalContext ctx) {
        return visit(ctx.noncondexpression());
    }

    @Override
    public ANode visitConditional(ConditionalContext ctx) {
        AExpression condition = (AExpression)visit(ctx.noncondexpression());
        AExpression left = (AExpression)visit(ctx.expression(0));
        AExpression right = (AExpression)visit(ctx.expression(1));

        return new EConditional(nextIdentifier(), location(ctx), condition, left, right);
    }

    @Override
    public ANode visitAssignment(AssignmentContext ctx) {
        AExpression lhs = (AExpression)visit(ctx.noncondexpression());
        AExpression rhs = (AExpression)visit(ctx.expression());

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
            throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
        }

        return new EAssignment(nextIdentifier(), location(ctx), lhs, rhs, false, operation);
    }

    @Override
    public ANode visitPre(PreContext ctx) {
        AExpression expression = (AExpression)visit(ctx.chain());

        final Operation operation;

        if (ctx.INCR() != null) {
            operation = Operation.ADD;
        } else if (ctx.DECR() != null) {
            operation = Operation.SUB;
        } else {
            throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
        }

        return new EAssignment(nextIdentifier(), location(ctx), expression,
                new ENumeric(nextIdentifier(), location(ctx), "1", 10), false, operation);
    }

    @Override
    public ANode visitAddsub(AddsubContext ctx) {
        AExpression expression = (AExpression)visit(ctx.unary());

        final Operation operation;

        if (ctx.ADD() != null) {
            operation = Operation.ADD;
        } else if (ctx.SUB() != null) {
            operation = Operation.SUB;
        } else {
            throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
        }

        return new EUnary(nextIdentifier(), location(ctx), expression, operation);
    }

    @Override
    public ANode visitNotaddsub(NotaddsubContext ctx) {
        return visit(ctx.unarynotaddsub());
    }

    @Override
    public ANode visitRead(ReadContext ctx) {
        return visit(ctx.chain());
    }

    @Override
    public ANode visitPost(PostContext ctx) {
        AExpression expression = (AExpression)visit(ctx.chain());

        final Operation operation;

        if (ctx.INCR() != null) {
            operation = Operation.ADD;
        } else if (ctx.DECR() != null) {
            operation = Operation.SUB;
        } else {
            throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
        }

        return new EAssignment(nextIdentifier(), location(ctx), expression,
                new ENumeric(nextIdentifier(), location(ctx), "1", 10), true, operation);
    }

    @Override
    public ANode visitNot(NotContext ctx) {
        AExpression expression = (AExpression)visit(ctx.unary());

        final Operation operation;

        if (ctx.BOOLNOT() != null) {
            operation = Operation.NOT;
        } else if (ctx.BWNOT() != null) {
            operation = Operation.BWNOT;
        } else {
            throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
        }

        return new EUnary(nextIdentifier(), location(ctx), expression, operation);
    }

    @Override
    public ANode visitCast(CastContext ctx) {
        return visit(ctx.castexpression());
    }

    @Override
    public ANode visitPrimordefcast(PainlessParser.PrimordefcastContext ctx) {
        String type = ctx.primordefcasttype().getText();
        AExpression child = (AExpression)visit(ctx.unary());

        return new EExplicit(nextIdentifier(), location(ctx), type, child);
    }

    @Override
    public ANode visitRefcast(PainlessParser.RefcastContext ctx) {
        String type = ctx.refcasttype().getText();
        AExpression child = (AExpression)visit(ctx.unarynotaddsub());

        return new EExplicit(nextIdentifier(), location(ctx), type, child);
    }

    @Override
    public ANode visitPrimordefcasttype(PainlessParser.PrimordefcasttypeContext ctx) {
        throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
    }

    @Override
    public ANode visitRefcasttype(PainlessParser.RefcasttypeContext ctx) {
        throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
    }

    @Override
    public ANode visitDynamic(DynamicContext ctx) {
        AExpression primary = (AExpression)visit(ctx.primary());

        return buildPostfixChain(primary, null, ctx.postfix());
    }

    @Override
    public ANode visitNewarray(NewarrayContext ctx) {
        return visit(ctx.arrayinitializer());
    }

    @Override
    public ANode visitPrecedence(PrecedenceContext ctx) {
        return visit(ctx.expression());
    }

    @Override
    public ANode visitNumeric(NumericContext ctx) {
        if (ctx.DECIMAL() != null) {
            return new EDecimal(nextIdentifier(), location(ctx), ctx.DECIMAL().getText());
        } else if (ctx.HEX() != null) {
            return new ENumeric(nextIdentifier(), location(ctx), ctx.HEX().getText().substring(2), 16);
        } else if (ctx.INTEGER() != null) {
            return new ENumeric(nextIdentifier(), location(ctx), ctx.INTEGER().getText(), 10);
        } else if (ctx.OCTAL() != null) {
            return new ENumeric(nextIdentifier(), location(ctx), ctx.OCTAL().getText().substring(1), 8);
        } else {
            throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
        }
    }

    @Override
    public ANode visitTrue(TrueContext ctx) {
        return new EBooleanConstant(nextIdentifier(), location(ctx), true);
    }

    @Override
    public ANode visitFalse(FalseContext ctx) {
        return new EBooleanConstant(nextIdentifier(), location(ctx), false);
    }

    @Override
    public ANode visitNull(NullContext ctx) {
        return new ENull(nextIdentifier(), location(ctx));
    }

    @Override
    public ANode visitString(StringContext ctx) {
        StringBuilder string = new StringBuilder(ctx.STRING().getText());

        // Strip the leading and trailing quotes and replace the escape sequences with their literal equivalents
        int src = 1;
        int dest = 0;
        int end = string.length() - 1;
        assert string.charAt(0) == '"' || string.charAt(0) == '\'' : "expected string to start with a quote but was [" + string + "]";
        assert string.charAt(end) == '"' || string.charAt(end) == '\'' : "expected string to end with a quote was [" + string + "]";
        while (src < end) {
            char current = string.charAt(src);
            if (current == '\\') {
                src++;
                current = string.charAt(src);
            }
            string.setCharAt(dest, current);
            src++;
            dest++;
        }
        string.setLength(dest);

        return new EString(nextIdentifier(), location(ctx), string.toString());
    }

    @Override
    public ANode visitRegex(RegexContext ctx) {
        String text = ctx.REGEX().getText();
        int lastSlash = text.lastIndexOf('/');
        String pattern = text.substring(1, lastSlash);
        String flags = text.substring(lastSlash + 1);

        return new ERegex(nextIdentifier(), location(ctx), pattern, flags);
    }

    @Override
    public ANode visitListinit(ListinitContext ctx) {
        return visit(ctx.listinitializer());
    }

    @Override
    public ANode visitMapinit(MapinitContext ctx) {
        return visit(ctx.mapinitializer());
    }

    @Override
    public ANode visitVariable(VariableContext ctx) {
        String name = ctx.ID().getText();

        return new ESymbol(nextIdentifier(), location(ctx), name);
    }

    @Override
    public ANode visitCalllocal(CalllocalContext ctx) {
        String name = ctx.ID().getText();
        List<AExpression> arguments = collectArguments(ctx.arguments());

        return new ECallLocal(nextIdentifier(), location(ctx), name, arguments);
    }

    @Override
    public ANode visitNewobject(NewobjectContext ctx) {
        String type = ctx.type().getText();
        List<AExpression> arguments = collectArguments(ctx.arguments());

        return new ENewObj(nextIdentifier(), location(ctx), type, arguments);
    }

    private AExpression buildPostfixChain(AExpression primary, PostdotContext postdot, List<PostfixContext> postfixes) {
        AExpression prefix = primary;

        if (postdot != null) {
            prefix = visitPostdot(postdot, prefix);
        }

        for (PostfixContext postfix : postfixes) {
            prefix = visitPostfix(postfix, prefix);
        }

        return prefix;
    }

    @Override
    public ANode visitPostfix(PostfixContext ctx) {
        throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
    }

    public AExpression visitPostfix(PostfixContext ctx, AExpression prefix) {
        if (ctx.callinvoke() != null) {
            return visitCallinvoke(ctx.callinvoke(), prefix);
        } else if (ctx.fieldaccess() != null) {
            return visitFieldaccess(ctx.fieldaccess(), prefix);
        } else if (ctx.braceaccess() != null) {
            return visitBraceaccess(ctx.braceaccess(), prefix);
        } else {
            throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
        }
    }

    @Override
    public ANode visitPostdot(PostdotContext ctx) {
        throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
    }

    public AExpression visitPostdot(PostdotContext ctx, AExpression prefix) {
        if (ctx.callinvoke() != null) {
            return visitCallinvoke(ctx.callinvoke(), prefix);
        } else if (ctx.fieldaccess() != null) {
            return visitFieldaccess(ctx.fieldaccess(), prefix);
        } else {
            throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
        }
    }

    @Override
    public ANode visitCallinvoke(CallinvokeContext ctx) {
        throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
    }

    public AExpression visitCallinvoke(CallinvokeContext ctx, AExpression prefix) {
        String name = ctx.DOTID().getText();
        List<AExpression> arguments = collectArguments(ctx.arguments());

        return new ECall(nextIdentifier(), location(ctx), prefix, name, arguments, ctx.NSDOT() != null);
    }

    @Override
    public ANode visitFieldaccess(FieldaccessContext ctx) {
        throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
    }

    public AExpression visitFieldaccess(FieldaccessContext ctx, AExpression prefix) {
        final String value;

        if (ctx.DOTID() != null) {
            value = ctx.DOTID().getText();
        } else if (ctx.DOTINTEGER() != null) {
            value = ctx.DOTINTEGER().getText();
        } else {
            throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
        }

        return new EDot(nextIdentifier(), location(ctx), prefix, value, ctx.NSDOT() != null);
    }

    @Override
    public ANode visitBraceaccess(BraceaccessContext ctx) {
        throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
    }

    public AExpression visitBraceaccess(BraceaccessContext ctx, AExpression prefix) {
        AExpression expression = (AExpression)visit(ctx.expression());

        return new EBrace(nextIdentifier(), location(ctx), prefix, expression);
    }

    @Override
    public ANode visitNewstandardarray(NewstandardarrayContext ctx) {
        StringBuilder type = new StringBuilder(ctx.type().getText());
        List<AExpression> expressions = new ArrayList<>();

        for (ExpressionContext expression : ctx.expression()) {
            type.append("[]");
            expressions.add((AExpression)visit(expression));
        }

        return buildPostfixChain(
                new ENewArray(nextIdentifier(), location(ctx), type.toString(), expressions, false), ctx.postdot(), ctx.postfix());
    }

    @Override
    public ANode visitNewinitializedarray(NewinitializedarrayContext ctx) {
        String type = ctx.type().getText() + "[]";
        List<AExpression> expressions = new ArrayList<>();

        for (ExpressionContext expression : ctx.expression()) {
            expressions.add((AExpression)visit(expression));
        }

        return buildPostfixChain(new ENewArray(nextIdentifier(), location(ctx), type, expressions, true), null, ctx.postfix());
    }

    @Override
    public ANode visitListinitializer(ListinitializerContext ctx) {
        List<AExpression> values = new ArrayList<>();

        for (ExpressionContext expression : ctx.expression()) {
            values.add((AExpression)visit(expression));
        }

        return new EListInit(nextIdentifier(), location(ctx), values);
    }

    @Override
    public ANode visitMapinitializer(MapinitializerContext ctx) {
        List<AExpression> keys = new ArrayList<>();
        List<AExpression> values = new ArrayList<>();

        for (MaptokenContext maptoken : ctx.maptoken()) {
            keys.add((AExpression)visit(maptoken.expression(0)));
            values.add((AExpression)visit(maptoken.expression(1)));
        }

        return new EMapInit(nextIdentifier(), location(ctx), keys, values);
    }

    @Override
    public ANode visitMaptoken(MaptokenContext ctx) {
        throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
    }

    @Override
    public ANode visitArguments(ArgumentsContext ctx) {
        throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
    }

    private List<AExpression> collectArguments(ArgumentsContext ctx) {
        List<AExpression> arguments = new ArrayList<>();

        for (ArgumentContext argument : ctx.argument()) {
            arguments.add((AExpression)visit(argument));
        }

        return arguments;
    }

    @Override
    public ANode visitArgument(ArgumentContext ctx) {
        if (ctx.expression() != null) {
            return visit(ctx.expression());
        } else if (ctx.lambda() != null) {
            return visit(ctx.lambda());
        } else if (ctx.funcref() != null) {
            return visit(ctx.funcref());
        } else {
            throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
        }
    }

    @Override
    public ANode visitLambda(LambdaContext ctx) {
        List<String> paramTypes = new ArrayList<>();
        List<String> paramNames = new ArrayList<>();
        SBlock block;

        for (LamtypeContext lamtype : ctx.lamtype()) {
            if (lamtype.decltype() == null) {
                paramTypes.add(null);
            } else {
                paramTypes.add(lamtype.decltype().getText());
            }

            paramNames.add(lamtype.ID().getText());
        }

        if (ctx.expression() != null) {
            // single expression
            AExpression expression = (AExpression)visit(ctx.expression());
            block = new SBlock(nextIdentifier(), location(ctx),
                    Collections.singletonList(new SReturn(nextIdentifier(), location(ctx), expression)));
        } else {
            block = (SBlock)visit(ctx.block());
        }

        return new ELambda(nextIdentifier(), location(ctx), paramTypes, paramNames, block);
    }

    @Override
    public ANode visitLamtype(LamtypeContext ctx) {
        throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
    }

    @Override
    public ANode visitClassfuncref(ClassfuncrefContext ctx) {
        return new EFunctionRef(nextIdentifier(), location(ctx), ctx.decltype().getText(), ctx.ID().getText());
    }

    @Override
    public ANode visitConstructorfuncref(ConstructorfuncrefContext ctx) {
        return ctx.decltype().LBRACE().isEmpty() ?
                new EFunctionRef(nextIdentifier(), location(ctx), ctx.decltype().getText(), ctx.NEW().getText()) :
                new ENewArrayFunctionRef(nextIdentifier(), location(ctx), ctx.decltype().getText());
    }

    @Override
    public ANode visitLocalfuncref(LocalfuncrefContext ctx) {
        return new EFunctionRef(nextIdentifier(), location(ctx), ctx.THIS().getText(), ctx.ID().getText());
    }
}
