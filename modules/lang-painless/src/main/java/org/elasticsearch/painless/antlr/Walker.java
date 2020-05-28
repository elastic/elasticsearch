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
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.Operation;
import org.elasticsearch.painless.ScriptClassInfo;
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
import org.elasticsearch.painless.lookup.PainlessLookupUtility;
import org.elasticsearch.painless.node.AExpression;
import org.elasticsearch.painless.node.ANode;
import org.elasticsearch.painless.node.AStatement;
import org.elasticsearch.painless.node.DResolvedType;
import org.elasticsearch.painless.node.DUnresolvedType;
import org.elasticsearch.painless.node.EAssignment;
import org.elasticsearch.painless.node.EBinary;
import org.elasticsearch.painless.node.EBool;
import org.elasticsearch.painless.node.EBoolean;
import org.elasticsearch.painless.node.EBrace;
import org.elasticsearch.painless.node.ECall;
import org.elasticsearch.painless.node.ECallLocal;
import org.elasticsearch.painless.node.EComp;
import org.elasticsearch.painless.node.EConditional;
import org.elasticsearch.painless.node.EConstant;
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
import org.elasticsearch.painless.node.EUnary;
import org.elasticsearch.painless.node.EVariable;
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

/**
 * Converts the ANTLR tree to a Painless tree.
 */
public final class Walker extends PainlessParserBaseVisitor<ANode> {

    public static SClass buildPainlessTree(ScriptClassInfo mainMethod, String sourceName, String sourceText, CompilerSettings settings) {
        return new Walker(mainMethod, sourceName, sourceText, settings).source;
    }

    private final ScriptClassInfo scriptClassInfo;
    private final SClass source;
    private final CompilerSettings settings;
    private final String sourceName;

    private Walker(ScriptClassInfo scriptClassInfo, String sourceName, String sourceText, CompilerSettings settings) {
        this.scriptClassInfo = scriptClassInfo;
        this.settings = settings;
        this.sourceName = sourceName;
        this.source = (SClass)visit(buildAntlrTree(sourceText));
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

        // add gets methods as declarations available for the user as variables
        for (int index = 0; index < scriptClassInfo.getGetMethods().size(); ++index) {
            org.objectweb.asm.commons.Method method = scriptClassInfo.getGetMethods().get(index);
            String name = method.getName().substring(3);
            name = Character.toLowerCase(name.charAt(0)) + name.substring(1);

            statements.add(new SDeclaration(location(ctx),
                    new DResolvedType(location(ctx), scriptClassInfo.getGetReturns().get(index), false), name, false, null));
        }

        for (StatementContext statement : ctx.statement()) {
            statements.add((AStatement)visit(statement));
        }

        String returnCanonicalTypeName = PainlessLookupUtility.typeToCanonicalTypeName(scriptClassInfo.getExecuteMethodReturnType());
        List<String> paramTypes = new ArrayList<>();
        List<String> paramNames = new ArrayList<>();

        for (ScriptClassInfo.MethodArgument argument : scriptClassInfo.getExecuteArguments()) {
            paramTypes.add(PainlessLookupUtility.typeToCanonicalTypeName(argument.getClazz()));
            paramNames.add(argument.getName());
        }

        // generate the execute method from the collected statements and parameters
        SFunction execute = new SFunction(location(ctx), returnCanonicalTypeName, "execute", paramTypes, paramNames, new SBlock(
                location(ctx), statements), true, false, false, true);
        functions.add(execute);

        return new SClass(location(ctx), functions);
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

        return new SFunction(
                location(ctx), rtnType, name, paramTypes, paramNames, new SBlock(location(ctx), statements), false, true, false, false);
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

            return new SIfElse(location(ctx), expression, ifblock, elseblock);
        } else {
            return new SIf(location(ctx), expression, ifblock);
        }
    }

    @Override
    public ANode visitWhile(WhileContext ctx) {
        AExpression expression = (AExpression)visit(ctx.expression());

        if (ctx.trailer() != null) {
            SBlock block = (SBlock)visit(ctx.trailer());

            return new SWhile(location(ctx), expression, block);
        } else if (ctx.empty() != null) {
            return new SWhile(location(ctx), expression, null);
        } else {
            throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
        }
    }

    @Override
    public ANode visitDo(DoContext ctx) {
        AExpression expression = (AExpression)visit(ctx.expression());
        SBlock block = (SBlock)visit(ctx.block());

        return new SDo(location(ctx), block, expression);
    }

    @Override
    public ANode visitFor(ForContext ctx) {
        ANode initializer = ctx.initializer() == null ? null : visit(ctx.initializer());
        AExpression expression = ctx.expression() == null ? null : (AExpression)visit(ctx.expression());
        AExpression afterthought = ctx.afterthought() == null ? null : (AExpression)visit(ctx.afterthought());

        if (ctx.trailer() != null) {
            SBlock block = (SBlock)visit(ctx.trailer());

            return new SFor(location(ctx), initializer, expression, afterthought, block);
        } else if (ctx.empty() != null) {
            return new SFor(location(ctx), initializer, expression, afterthought, null);
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

        return new SEach(location(ctx), type, name, expression, block);
    }

    @Override
    public ANode visitIneach(IneachContext ctx) {
        String name = ctx.ID().getText();
        AExpression expression = (AExpression)visit(ctx.expression());
        SBlock block = (SBlock)visit(ctx.trailer());

        return new SEach(location(ctx), "def", name, expression, block);
    }

    @Override
    public ANode visitDecl(DeclContext ctx) {
        return visit(ctx.declaration());
    }

    @Override
    public ANode visitContinue(ContinueContext ctx) {
        return new SContinue(location(ctx));
    }

    @Override
    public ANode visitBreak(BreakContext ctx) {
        return new SBreak(location(ctx));
    }

    @Override
    public ANode visitReturn(ReturnContext ctx) {
        AExpression expression = null;

        if (ctx.expression() != null) {
            expression = (AExpression) visit(ctx.expression());
        }

        return new SReturn(location(ctx), expression);
    }

    @Override
    public ANode visitTry(TryContext ctx) {
        SBlock block = (SBlock)visit(ctx.block());
        List<SCatch> catches = new ArrayList<>();

        for (TrapContext trap : ctx.trap()) {
            catches.add((SCatch)visit(trap));
        }

        return new STry(location(ctx), block, catches);
    }

    @Override
    public ANode visitThrow(ThrowContext ctx) {
        AExpression expression = (AExpression)visit(ctx.expression());

        return new SThrow(location(ctx), expression);
    }

    @Override
    public ANode visitExpr(ExprContext ctx) {
        AExpression expression = (AExpression)visit(ctx.expression());

        return new SExpression(location(ctx), expression);
    }

    @Override
    public ANode visitTrailer(TrailerContext ctx) {
        if (ctx.block() != null) {
            return visit(ctx.block());
        } else if (ctx.statement() != null) {
            List<AStatement> statements = new ArrayList<>();
            statements.add((AStatement)visit(ctx.statement()));

            return new SBlock(location(ctx), statements);
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

            return new SBlock(location(ctx), statements);
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
            DUnresolvedType unresolvedType = new DUnresolvedType(location(declvar), type);

            declarations.add(new SDeclaration(location(declvar), unresolvedType, name, true, expression));
        }

        return new SDeclBlock(location(ctx), declarations);
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

        return new SCatch(location(ctx), new DResolvedType(location(ctx), Exception.class),
                new SDeclaration(location(ctx.type()), new DUnresolvedType(location(ctx.type()), type), name, false, null), block);
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

        return new EBinary(location(ctx), operation, left, right);
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

        return new EComp(location(ctx), operation, left, right);
    }

    @Override
    public ANode visitInstanceof(InstanceofContext ctx) {
        AExpression expr = (AExpression)visit(ctx.noncondexpression());
        String type = ctx.decltype().getText();

        return new EInstanceof(location(ctx), expr, type);
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

        return new EBool(location(ctx), operation, left, right);
    }

    @Override
    public ANode visitElvis(ElvisContext ctx) {
        AExpression left = (AExpression)visit(ctx.noncondexpression(0));
        AExpression right = (AExpression)visit(ctx.noncondexpression(1));

        return new EElvis(location(ctx), left, right);
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

        return new EConditional(location(ctx), condition, left, right);
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

        return new EAssignment(location(ctx), lhs, rhs, false, operation);
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

        return new EAssignment(location(ctx), expression, new EConstant(location(ctx), 1), false, operation);
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

        return new EUnary(location(ctx), operation, expression);
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

        return new EAssignment(location(ctx), expression, new EConstant(location(ctx), 1), true, operation);
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

        return new EUnary(location(ctx), operation, expression);
    }

    @Override
    public ANode visitCast(CastContext ctx) {
        return visit(ctx.castexpression());
    }

    @Override
    public ANode visitPrimordefcast(PainlessParser.PrimordefcastContext ctx) {
        String type = ctx.primordefcasttype().getText();
        AExpression child = (AExpression)visit(ctx.unary());

        return new EExplicit(location(ctx), new DUnresolvedType(location(ctx.primordefcasttype()), type), child);
    }

    @Override
    public ANode visitRefcast(PainlessParser.RefcastContext ctx) {
        String type = ctx.refcasttype().getText();
        AExpression child = (AExpression)visit(ctx.unarynotaddsub());

        return new EExplicit(location(ctx), new DUnresolvedType(location(ctx.refcasttype()), type), child);
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
            return new EDecimal(location(ctx), ctx.DECIMAL().getText());
        } else if (ctx.HEX() != null) {
            return new ENumeric(location(ctx), ctx.HEX().getText().substring(2), 16);
        } else if (ctx.INTEGER() != null) {
            return new ENumeric(location(ctx), ctx.INTEGER().getText(), 10);
        } else if (ctx.OCTAL() != null) {
            return new ENumeric(location(ctx), ctx.OCTAL().getText().substring(1), 8);
        } else {
            throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
        }
    }

    @Override
    public ANode visitTrue(TrueContext ctx) {
        return new EBoolean(location(ctx), true);
    }

    @Override
    public ANode visitFalse(FalseContext ctx) {
        return new EBoolean(location(ctx), false);
    }

    @Override
    public ANode visitNull(NullContext ctx) {
        return new ENull(location(ctx));
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

        return new EString(location(ctx), string.toString());
    }

    @Override
    public ANode visitRegex(RegexContext ctx) {
        String text = ctx.REGEX().getText();
        int lastSlash = text.lastIndexOf('/');
        String pattern = text.substring(1, lastSlash);
        String flags = text.substring(lastSlash + 1);

        return new ERegex(location(ctx), pattern, flags);
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

        return new EVariable(location(ctx), name);
    }

    @Override
    public ANode visitCalllocal(CalllocalContext ctx) {
        String name = ctx.ID().getText();
        List<AExpression> arguments = collectArguments(ctx.arguments());

        return new ECallLocal(location(ctx), name, arguments);
    }

    @Override
    public ANode visitNewobject(NewobjectContext ctx) {
        String type = ctx.type().getText();
        List<AExpression> arguments = collectArguments(ctx.arguments());

        return new ENewObj(location(ctx), type, arguments);
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

        return new ECall(location(ctx), prefix, name, arguments, ctx.NSDOT() != null);
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

        return new EDot(location(ctx), prefix, ctx.NSDOT() != null, value);
    }

    @Override
    public ANode visitBraceaccess(BraceaccessContext ctx) {
        throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
    }

    public AExpression visitBraceaccess(BraceaccessContext ctx, AExpression prefix) {
        AExpression expression = (AExpression)visit(ctx.expression());

        return new EBrace(location(ctx), prefix, expression);
    }

    @Override
    public ANode visitNewstandardarray(NewstandardarrayContext ctx) {
        StringBuilder type = new StringBuilder(ctx.type().getText());
        List<AExpression> expressions = new ArrayList<>();

        for (ExpressionContext expression : ctx.expression()) {
            type.append("[]");
            expressions.add((AExpression)visit(expression));
        }

        return buildPostfixChain(new ENewArray(location(ctx), type.toString(), expressions, false), ctx.postdot(), ctx.postfix());
    }

    @Override
    public ANode visitNewinitializedarray(NewinitializedarrayContext ctx) {
        String type = ctx.type().getText() + "[]";
        List<AExpression> expressions = new ArrayList<>();

        for (ExpressionContext expression : ctx.expression()) {
            expressions.add((AExpression)visit(expression));
        }

        return buildPostfixChain(new ENewArray(location(ctx), type, expressions, true), null, ctx.postfix());
    }

    @Override
    public ANode visitListinitializer(ListinitializerContext ctx) {
        List<AExpression> values = new ArrayList<>();

        for (ExpressionContext expression : ctx.expression()) {
            values.add((AExpression)visit(expression));
        }

        return new EListInit(location(ctx), values);
    }

    @Override
    public ANode visitMapinitializer(MapinitializerContext ctx) {
        List<AExpression> keys = new ArrayList<>();
        List<AExpression> values = new ArrayList<>();

        for (MaptokenContext maptoken : ctx.maptoken()) {
            keys.add((AExpression)visit(maptoken.expression(0)));
            values.add((AExpression)visit(maptoken.expression(1)));
        }

        return new EMapInit(location(ctx), keys, values);
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
            block = new SBlock(location(ctx),
                    Collections.singletonList(new SReturn(location(ctx), expression)));
        } else {
            block = (SBlock)visit(ctx.block());
        }

        return new ELambda(location(ctx), paramTypes, paramNames, block);
    }

    @Override
    public ANode visitLamtype(LamtypeContext ctx) {
        throw location(ctx).createError(new IllegalStateException("illegal tree structure"));
    }

    @Override
    public ANode visitClassfuncref(ClassfuncrefContext ctx) {
        return new EFunctionRef(location(ctx), ctx.decltype().getText(), ctx.ID().getText());
    }

    @Override
    public ANode visitConstructorfuncref(ConstructorfuncrefContext ctx) {
        return ctx.decltype().LBRACE().isEmpty() ?
                new EFunctionRef(location(ctx), ctx.decltype().getText(), ctx.NEW().getText()) :
                new ENewArrayFunctionRef(location(ctx), ctx.decltype().getText());
    }

    @Override
    public ANode visitLocalfuncref(LocalfuncrefContext ctx) {
        return new EFunctionRef(location(ctx), ctx.THIS().getText(), ctx.ID().getText());
    }
}
