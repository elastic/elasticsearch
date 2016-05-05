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
import org.elasticsearch.painless.tree.analyzer.Operation;
import org.elasticsearch.painless.tree.analyzer.Variables.Special;
import org.elasticsearch.painless.input.antlr.PainlessParser.AfterthoughtContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.ArgumentsContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.AssignmentContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.BinaryContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.BreakContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.CastContext;
import org.elasticsearch.painless.input.antlr.PainlessParser.CharContext;
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
import org.elasticsearch.painless.tree.node.BNode;
import org.elasticsearch.painless.tree.node.AStatement;
import org.elasticsearch.painless.tree.node.EBoolean;
import org.elasticsearch.painless.tree.node.EChar;
import org.elasticsearch.painless.tree.node.EDecimal;
import org.elasticsearch.painless.tree.node.ENull;
import org.elasticsearch.painless.tree.node.ENumeric;
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

import static org.elasticsearch.painless.tree.analyzer.Operation.ADD;
import static org.elasticsearch.painless.tree.analyzer.Operation.AND;
import static org.elasticsearch.painless.tree.analyzer.Operation.BWNOT;
import static org.elasticsearch.painless.tree.analyzer.Operation.DIV;
import static org.elasticsearch.painless.tree.analyzer.Operation.LSH;
import static org.elasticsearch.painless.tree.analyzer.Operation.MUL;
import static org.elasticsearch.painless.tree.analyzer.Operation.NOT;
import static org.elasticsearch.painless.tree.analyzer.Operation.OR;
import static org.elasticsearch.painless.tree.analyzer.Operation.REM;
import static org.elasticsearch.painless.tree.analyzer.Operation.RSH;
import static org.elasticsearch.painless.tree.analyzer.Operation.SUB;
import static org.elasticsearch.painless.tree.analyzer.Operation.USH;
import static org.elasticsearch.painless.tree.analyzer.Operation.XOR;

public class Walker extends PainlessParserBaseVisitor<BNode> {
    public static BNode buildPainlessTree(final String source, final Special special) {
        return new Walker(source, special).source;
    }

    private Special special;
    private BNode source;

    private Walker(final String source, final Special special) {
        this.special = special;
        this.source = visit(buildAntlrTree(source));
    }

    private SourceContext buildAntlrTree(final String source) {
        final ANTLRInputStream stream = new ANTLRInputStream(source);
        final PainlessLexer lexer = new PainlessLexer(stream);
        final PainlessParser parser = new PainlessParser(new CommonTokenStream(lexer));

        return parser.source();
    }

    private String location(final ParserRuleContext ctx) {
        return "[ " + ctx.getStart().getLine() + " : " + ctx.getStart().getCharPositionInLine() + " ]";
    }

    @Override
    public BNode visitSource(final SourceContext ctx) {
        final List<AStatement> statements = new ArrayList<>();

        for (final StatementContext statement : ctx.statement()) {
            statements.add((AStatement)visit(statement));
        }

        return new SSource(location(ctx), statements);
    }

    @Override
    public BNode visitIf(final IfContext ctx) {
        final AExpression condition = (AExpression)visit(ctx.expression());
        final AStatement ifblock = (AStatement)visit(ctx.block(0));
        final AStatement elseblock = ctx.block(1) == null ? null : (AStatement)visit(ctx.block(1));

        return new SIfElse(location(ctx), condition, ifblock, elseblock);
    }

    @Override
    public BNode visitWhile(final WhileContext ctx) {
        final AExpression condition = (AExpression)visit(ctx.expression());
        final AStatement block = ctx.block() == null ? null : (AStatement)visit(ctx.block());

        return new SWhile(location(ctx), condition, block);
    }

    @Override
    public BNode visitDo(final DoContext ctx) {
        final AStatement block = ctx.block() == null ? null : (AStatement)visit(ctx.block());
        final AExpression condition = (AExpression)visit(ctx.expression());

        return new SDo(location(ctx), block, condition);
    }

    @Override
    public BNode visitFor(final ForContext ctx) {
        final BNode intializer = ctx.initializer() == null ? null : (BNode)visit(ctx.initializer());
        final AExpression condition = ctx.expression() == null ? null : (AExpression)visit(ctx.expression());
        final AExpression afterthought = ctx.afterthought() == null ? null : (AExpression)visit(ctx.afterthought());
        final AStatement block = ctx.block() == null ? null : (AStatement)visit(ctx.block());

        return new SFor(location(ctx), intializer, condition, afterthought, block);
    }

    @Override
    public BNode visitDecl(final DeclContext ctx) {
        return visit(ctx.declaration());
    }

    @Override
    public BNode visitContinue(final ContinueContext ctx) {
        return new SContinue(location(ctx));
    }

    @Override
    public BNode visitBreak(final BreakContext ctx) {
        return new SBreak(location(ctx));
    }

    @Override
    public BNode visitReturn(final ReturnContext ctx) {
        final AExpression expression = (AExpression)visit(ctx.expression());

        return new SReturn(location(ctx), expression);
    }

    @Override
    public BNode visitTry(final TryContext ctx) {
        final AStatement block = (AStatement)visit(ctx.block());
        final List<STrap> traps = new ArrayList<>();

        for (final TrapContext trap : ctx.trap()) {
            traps.add((STrap)visit(trap));
        }

        return new STry(location(ctx), block, traps);
    }

    @Override
    public BNode visitThrow(final ThrowContext ctx) {
        final AExpression expression = (AExpression)visit(ctx.expression());

        return new SThrow(location(ctx), expression);
    }

    @Override
    public BNode visitExpr(final ExprContext ctx) {
        final AExpression expression = (AExpression)visit(ctx.expression());

        return new SExpression(location(ctx), expression);
    }

    @Override
    public BNode visitMultiple(final MultipleContext ctx) {
        final List<AStatement> statements = new ArrayList<>();

        for (final StatementContext statement : ctx.statement()) {
            statements.add((AStatement)visit(statement));
        }

        return new SBlock(location(ctx), statements);
    }

    @Override
    public BNode visitSingle(final SingleContext ctx) {
        final List<AStatement> statements = new ArrayList<>();
        statements.add((AStatement)visit(ctx.statement()));

        return new SBlock(location(ctx), statements);
    }

    @Override
    public BNode visitEmpty(final EmptyContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public BNode visitEmptyscope(final EmptyscopeContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public BNode visitInitializer(final InitializerContext ctx) {
        if (ctx.declaration() != null) {
            return visit(ctx.declaration());
        } else if (ctx.expression() != null) {
            return visit(ctx.expression());
        }

        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public BNode visitAfterthought(final AfterthoughtContext ctx) {
        return visit(ctx.expression());
    }

    @Override
    public BNode visitDeclaration(final DeclarationContext ctx) {
        final String type = ctx.decltype().identifier().getText();
        final List<SDeclaration> declarations = new ArrayList<>();

        for (final DeclvarContext declvar : ctx.declvar()) {
            final String name = declvar.identifier().getText();
            final AExpression expression = declvar.expression() == null ? null : (AExpression)visit(declvar.expression());
            declarations.add(new SDeclaration(location(ctx), type, name, expression));
        }

        return new SDeclBlock(location(ctx), declarations);
    }

    @Override
    public BNode visitDecltype(final DecltypeContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public BNode visitDeclvar(final DeclvarContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public BNode visitTrap(final TrapContext ctx) {
        final String type = ctx.identifier(0).getText();
        final String name = ctx.identifier(1).getText();
        final AStatement block = ctx.block() == null ? null : (AStatement)visit(ctx.block());

        return new STrap(location(ctx), type, name, block);
    }

    @Override
    public BNode visitIdentifier(final IdentifierContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public BNode visitGeneric(final GenericContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public BNode visitPrecedence(final PrecedenceContext ctx) {
        return visit(ctx.expression());
    }

    @Override
    public BNode visitNumeric(final NumericContext ctx) {
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
    public BNode visitChar(final CharContext ctx) {
        return new EChar(location(ctx), ctx.CHAR().getText().charAt(1));
    }

    @Override
    public BNode visitTrue(final TrueContext ctx) {
        return new EBoolean(location(ctx), true);
    }

    @Override
    public BNode visitFalse(FalseContext ctx) {
        return new EBoolean(location(ctx), false);
    }

    @Override
    public BNode visitNull(final NullContext ctx) {
        return new ENull(location(ctx));
    }

    @Override
    public BNode visitPostinc(final PostincContext ctx) {
        final List<ALink> links = new ArrayList<>();

        if (ctx.INCR() != null) {
            node.data.put("operation", ADD);
        } else if (ctx.DECR() != null) {
            node.data.put("operation", SUB);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        visitExtstart(ctx.extstart(), node);

        return new EChain;
    }

    @Override
    public BNode visitPreinc(final PreincContext ctx) {
        final BNode node = new BNode(location(ctx), PRE);

        if (ctx.INCR() != null) {
            node.data.put("operation", ADD);
        } else if (ctx.DECR() != null) {
            node.data.put("operation", SUB);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        visitExtstart(ctx.extstart(), node);

        return node;
    }

    @Override
    public BNode visitExternal(final ExternalContext ctx) {
        final BNode node = new BNode(location(ctx), EXTERNAL);

        visitExtstart(ctx.extstart(), node);

        return node;
    }

    @Override
    public BNode visitUnary(final UnaryContext ctx) {
        if (ctx.SUB() != null && ctx.expression() instanceof NumericContext) {
            return visit(ctx.expression());
        } else {
            final BNode node = new BNode(location(ctx), UNARY);

            if (ctx.BOOLNOT() != null) {
                node.data.put("operation", NOT);
            } else if (ctx.BWNOT() != null) {
                node.data.put("operation", BWNOT);
            } else if (ctx.ADD() != null) {
                node.data.put("operation", ADD);
            } else if (ctx.SUB() != null) {
                node.data.put("operation", SUB);
            } else {
                throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
            }

            node.children.add(visit(ctx.expression()));

            return node;
        }
    }

    @Override
    public BNode visitCast(final CastContext ctx) {
        final BNode node = new BNode(location(ctx), CAST);

        node.data.put("type", ctx.decltype().identifier().getText());
        node.children.add(visit(ctx.expression()));

        return node;
    }

    @Override
    public BNode visitBinary(final BinaryContext ctx) {
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
        } else if (ctx.LT() != null) {
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
        } else if (ctx.LT() != null) {
            operation = Operation.NER;
        } else if (ctx.BWAND() != null) {
            operation = Operation.BWAND;
        } else if (ctx.XOR() != null) {
            operation = Operation.XOR;
        } else if (ctx.BWOR() != null) {
            operation = Operation.BWOR;
        } else if (ctx.BOOLAND() != null) {
            operation = Operation.AND;
        } else if (ctx.BWOR() != null) {
            operation = Operation.OR;
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        node.children.add(visit(ctx.expression(0)));
        node.children.add(visit(ctx.expression(1)));

        return node;
    }

    @Override
    public BNode visitConditional(final ConditionalContext ctx) {
        final BNode node = new BNode(location(ctx), CONDITIONAL);

        node.children.add(visit(ctx.expression(0)));
        node.children.add(visit(ctx.expression(1)));
        node.children.add(visit(ctx.expression(2)));

        return node;
    }

    @Override
    public BNode visitAssignment(final AssignmentContext ctx) {
        final BNode node = new BNode(location(ctx), ctx.ASSIGN() == null ? COMPOUND : ASSIGNMENT);

        if (node.type == COMPOUND) {
            if (ctx.AMUL() != null) {
                node.data.put("operation", MUL);
            } else if (ctx.ADIV() != null) {
                node.data.put("operation", DIV);
            } else if (ctx.AREM() != null) {
                node.data.put("operation", REM);
            } else if (ctx.AADD() != null) {
                node.data.put("operation", ADD);
            } else if (ctx.ASUB() != null) {
                node.data.put("operation", SUB);
            } else if (ctx.ALSH() != null) {
                node.data.put("operation", LSH);
            } else if (ctx.ARSH() != null) {
                node.data.put("operation", RSH);
            } else if (ctx.AUSH() != null) {
                node.data.put("operation", USH);
            } else if (ctx.AAND() != null) {
                node.data.put("operation", AND);
            } else if (ctx.AXOR() != null) {
                node.data.put("operation", XOR);
            } else if (ctx.AAND() != null) {
                node.data.put("operation", OR);
            } else {
                throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
            }
        }

        node.children.add(visit(ctx.expression()));
        visitExtstart(ctx.extstart(), node);

        return node;
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
    public BNode visitExtstart(final ExtstartContext ctx) {
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
    public BNode visitExtprec(final ExtprecContext ctx) {
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

        final BNode node = new BNode(location(ctx), CAST);

        node.data.put("type", ctx.decltype().identifier().getText());

        links.children.add(node);
    }

    @Override
    public BNode visitExtcast(final ExtcastContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtbrace(final ExtbraceContext ctx, final List<ALink> links) {
        final BNode node = new BNode(location(ctx), BRACE);

        node.children.add(visit(ctx.expression()));

        links.children.add(node);

        if (ctx.extbrace() != null) {
            visitExtbrace(ctx.extbrace(), links);
        } else if (ctx.extdot() != null) {
            visitExtdot(ctx.extdot(), links);
        }
    }

    @Override
    public BNode visitExtbrace(final ExtbraceContext ctx) {
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
    public BNode visitExtdot(final ExtdotContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtcall(final ExtcallContext ctx, final List<ALink> links) {
        final BNode node = new BNode(location(ctx), CALL);

        node.data.put("symbol", ctx.EXTID().getText());

        for (final ExpressionContext expression : ctx.arguments().expression()) {
            node.children.add(visit(expression));
        }

        links.children.add(node);

        if (ctx.extbrace() != null) {
            visitExtbrace(ctx.extbrace(), links);
        } else if (ctx.extdot() != null) {
            visitExtdot(ctx.extdot(), links);
        }
    }

    @Override
    public BNode visitExtcall(final ExtcallContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtvar(final ExtvarContext ctx, final List<ALink> links) {
        final String name = ctx.identifier().getText();

        links.add(new LVariable(location(ctx), name));

        if (ctx.extbrace() != null) {
            visitExtbrace(ctx.extbrace(), links);
        } else if (ctx.extdot() != null) {
            visitExtdot(ctx.extdot(), links);
        }
    }

    @Override
    public BNode visitExtvar(final ExtvarContext ctx) {
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
    public BNode visitExtfield(final ExtfieldContext ctx) {
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
    public BNode visitExtnew(final ExtnewContext ctx) {
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
    public BNode visitExtstring(final ExtstringContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public BNode visitArguments(final ArgumentsContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }
}
