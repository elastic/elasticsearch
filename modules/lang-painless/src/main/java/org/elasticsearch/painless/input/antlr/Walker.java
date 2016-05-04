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
import org.elasticsearch.painless.tree.analyzer.Special;
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
import org.elasticsearch.painless.tree.node.ANode;

import static org.elasticsearch.painless.tree.analyzer.Operation.ADD;
import static org.elasticsearch.painless.tree.analyzer.Operation.AND;
import static org.elasticsearch.painless.tree.analyzer.Operation.BWAND;
import static org.elasticsearch.painless.tree.analyzer.Operation.BWNOT;
import static org.elasticsearch.painless.tree.analyzer.Operation.BWOR;
import static org.elasticsearch.painless.tree.analyzer.Operation.DIV;
import static org.elasticsearch.painless.tree.analyzer.Operation.EQ;
import static org.elasticsearch.painless.tree.analyzer.Operation.EQR;
import static org.elasticsearch.painless.tree.analyzer.Operation.GT;
import static org.elasticsearch.painless.tree.analyzer.Operation.GTE;
import static org.elasticsearch.painless.tree.analyzer.Operation.LSH;
import static org.elasticsearch.painless.tree.analyzer.Operation.LT;
import static org.elasticsearch.painless.tree.analyzer.Operation.LTE;
import static org.elasticsearch.painless.tree.analyzer.Operation.MUL;
import static org.elasticsearch.painless.tree.analyzer.Operation.NE;
import static org.elasticsearch.painless.tree.analyzer.Operation.NER;
import static org.elasticsearch.painless.tree.analyzer.Operation.NOT;
import static org.elasticsearch.painless.tree.analyzer.Operation.OR;
import static org.elasticsearch.painless.tree.analyzer.Operation.REM;
import static org.elasticsearch.painless.tree.analyzer.Operation.RSH;
import static org.elasticsearch.painless.tree.analyzer.Operation.SUB;
import static org.elasticsearch.painless.tree.analyzer.Operation.USH;
import static org.elasticsearch.painless.tree.analyzer.Operation.XOR;
import static org.elasticsearch.painless.tree.analyzer.Type.ASSIGNMENT;
import static org.elasticsearch.painless.tree.analyzer.Type.BINARY;
import static org.elasticsearch.painless.tree.analyzer.Type.BLOCK;
import static org.elasticsearch.painless.tree.analyzer.Type.BRACE;
import static org.elasticsearch.painless.tree.analyzer.Type.BREAK;
import static org.elasticsearch.painless.tree.analyzer.Type.CALL;
import static org.elasticsearch.painless.tree.analyzer.Type.CAST;
import static org.elasticsearch.painless.tree.analyzer.Type.CHAR;
import static org.elasticsearch.painless.tree.analyzer.Type.COMPOUND;
import static org.elasticsearch.painless.tree.analyzer.Type.CONDITIONAL;
import static org.elasticsearch.painless.tree.analyzer.Type.CONTINUE;
import static org.elasticsearch.painless.tree.analyzer.Type.DECLARATION;
import static org.elasticsearch.painless.tree.analyzer.Type.DECLVAR;
import static org.elasticsearch.painless.tree.analyzer.Type.DO;
import static org.elasticsearch.painless.tree.analyzer.Type.EXPRESSION;
import static org.elasticsearch.painless.tree.analyzer.Type.EXTERNAL;
import static org.elasticsearch.painless.tree.analyzer.Type.FALSE;
import static org.elasticsearch.painless.tree.analyzer.Type.FIELD;
import static org.elasticsearch.painless.tree.analyzer.Type.FOR;
import static org.elasticsearch.painless.tree.analyzer.Type.IF;
import static org.elasticsearch.painless.tree.analyzer.Type.NEWARRAY;
import static org.elasticsearch.painless.tree.analyzer.Type.NEWOBJ;
import static org.elasticsearch.painless.tree.analyzer.Type.NULL;
import static org.elasticsearch.painless.tree.analyzer.Type.NUMERIC;
import static org.elasticsearch.painless.tree.analyzer.Type.POST;
import static org.elasticsearch.painless.tree.analyzer.Type.PRE;
import static org.elasticsearch.painless.tree.analyzer.Type.RETURN;
import static org.elasticsearch.painless.tree.analyzer.Type.SOURCE;
import static org.elasticsearch.painless.tree.analyzer.Type.STRING;
import static org.elasticsearch.painless.tree.analyzer.Type.THROW;
import static org.elasticsearch.painless.tree.analyzer.Type.TRAP;
import static org.elasticsearch.painless.tree.analyzer.Type.TRUE;
import static org.elasticsearch.painless.tree.analyzer.Type.TRY;
import static org.elasticsearch.painless.tree.analyzer.Type.UNARY;
import static org.elasticsearch.painless.tree.analyzer.Type.VAR;
import static org.elasticsearch.painless.tree.analyzer.Type.WHILE;

public class Walker extends PainlessParserBaseVisitor<ANode> {
    public static ANode buildPainlessTree(final String source, final Special special) {
        return new Walker(source, special).source;
    }

    private Special special;
    private ANode source;

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
    public ANode visitSource(final SourceContext ctx) {
        final ANode node = new ANode(location(ctx), SOURCE);

        for (final StatementContext statement : ctx.statement()) {
            node.children.add(visit(statement));
        }

        return node;
    }

    @Override
    public ANode visitIf(final IfContext ctx) {
        final ANode node = new ANode(location(ctx), IF);

        node.children.add(visit(ctx.expression()));
        node.children.add(visit(ctx.block(0)));
        node.children.add(ctx.block(1) == null ? null : visit(ctx.block(1)));

        return node;
    }

    @Override
    public ANode visitWhile(final WhileContext ctx) {
        final ANode node = new ANode(location(ctx), WHILE);

        node.children.add(visit(ctx.expression()));
        node.children.add(ctx.block() == null ? null : visit(ctx.block()));

        return node;
    }

    @Override
    public ANode visitDo(final DoContext ctx) {
        final ANode node = new ANode(location(ctx), DO);

        node.children.add(visit(ctx.block()));
        node.children.add(visit(ctx.expression()));

        return node;
    }

    @Override
    public ANode visitFor(final ForContext ctx) {
        final ANode node = new ANode(location(ctx), FOR);

        node.children.add(ctx.initializer() == null ? null : visit(ctx.initializer()));
        node.children.add(ctx.expression() == null ? null : visit(ctx.expression()));
        node.children.add(ctx.afterthought() == null ? null : visit(ctx.afterthought()));
        node.children.add(ctx.block() == null ? null : visit(ctx.block()));

        return node;
    }

    @Override
    public ANode visitDecl(final DeclContext ctx) {
        return visit(ctx.declaration());
    }

    @Override
    public ANode visitContinue(final ContinueContext ctx) {
        return new ANode(location(ctx), CONTINUE);
    }

    @Override
    public ANode visitBreak(final BreakContext ctx) {
        return new ANode(location(ctx), BREAK);
    }

    @Override
    public ANode visitReturn(final ReturnContext ctx) {
        final ANode node = new ANode(location(ctx), RETURN);

        node.children.add(visit(ctx.expression()));

        return node;
    }

    @Override
    public ANode visitTry(final TryContext ctx) {
        final ANode node = new ANode(location(ctx), TRY);

        node.children.add(visit(ctx.block()));

        for (final TrapContext trap : ctx.trap()) {
            node.children.add(visit(trap));
        }

        return node;
    }

    @Override
    public ANode visitThrow(final ThrowContext ctx) {
        final ANode node = new ANode(location(ctx), THROW);

        node.children.add(visit(ctx.expression()));

        return node;
    }

    @Override
    public ANode visitExpr(final ExprContext ctx) {
        final ANode node = new ANode(location(ctx), EXPRESSION);

        node.children.add(visit(ctx.expression()));

        return node;
    }

    @Override
    public ANode visitMultiple(final MultipleContext ctx) {
        final ANode node = new ANode(location(ctx), BLOCK);

        for (final StatementContext statement : ctx.statement()) {
            node.children.add(visit(statement));
        }

        return node;
    }

    @Override
    public ANode visitSingle(final SingleContext ctx) {
        final ANode node = new ANode(location(ctx), BLOCK);

        node.children.add(visit(ctx.statement()));

        return node;
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
        final ANode node = new ANode(location(ctx), DECLARATION);

        final String type = ctx.decltype().identifier().getText();

        for (final DeclvarContext declvar : ctx.declvar()) {
            final ANode var = visit(declvar);

            var.data.put("type", type);

            node.children.add(var);
        }

        return node;
    }

    @Override
    public ANode visitDecltype(final DecltypeContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public ANode visitDeclvar(final DeclvarContext ctx) {
        final ANode node = new ANode(location(ctx), DECLVAR);

        final String symbol = ctx.identifier().getText();
        node.data.put("symbol", symbol);

        node.children.add(ctx.expression() == null ? null : visit(ctx.expression()));

        return node;
    }

    @Override
    public ANode visitTrap(final TrapContext ctx) {
        final ANode node = new ANode(location(ctx), TRAP);

        final String type = ctx.identifier(0).getText();
        final String symbol = ctx.identifier(1).getText();

        node.data.put("type", type);
        node.data.put("symbol", symbol);

        node.children.add(ctx.block() == null ? null : visit(ctx.block()));

        return node;
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
        final ANode node = new ANode(location(ctx), NUMERIC);

        final boolean negate = ctx.parent instanceof UnaryContext && ((UnaryContext)ctx.parent).SUB() != null;

        if (ctx.DECIMAL() != null) {
            node.data.put("decimal", (negate ? "-" : "") + ctx.DECIMAL().getText());
        } else if (ctx.HEX() != null) {
            node.data.put("hex", (negate ? "-" : "") + ctx.HEX().getText().substring(2));
        } else if (ctx.INTEGER() != null) {
            node.data.put("integer", (negate ? "-" : "") + ctx.INTEGER().getText());
        } else if (ctx.OCTAL() != null) {
            node.data.put("octal", (negate ? "-" : "") + ctx.OCTAL().getText().substring(1));
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        return node;
    }

    @Override
    public ANode visitChar(final CharContext ctx) {
        final ANode node = new ANode(location(ctx), CHAR);

        node.data.put("char", ctx.CHAR().getText().charAt(1));

        return node;
    }

    @Override
    public ANode visitTrue(final TrueContext ctx) {
        return new ANode(location(ctx), TRUE);
    }

    @Override
    public ANode visitFalse(FalseContext ctx) {
        return new ANode(location(ctx), FALSE);
    }

    @Override
    public ANode visitNull(final NullContext ctx) {
        return new ANode(location(ctx), NULL);
    }

    @Override
    public ANode visitPostinc(final PostincContext ctx) {
        final ANode node = new ANode(location(ctx), POST);

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
    public ANode visitPreinc(final PreincContext ctx) {
        final ANode node = new ANode(location(ctx), PRE);

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
    public ANode visitExternal(final ExternalContext ctx) {
        final ANode node = new ANode(location(ctx), EXTERNAL);

        visitExtstart(ctx.extstart(), node);

        return node;
    }

    @Override
    public ANode visitUnary(final UnaryContext ctx) {
        if (ctx.SUB() != null && ctx.expression() instanceof NumericContext) {
            return visit(ctx.expression());
        } else {
            final ANode node = new ANode(location(ctx), UNARY);

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
    public ANode visitCast(final CastContext ctx) {
        final ANode node = new ANode(location(ctx), CAST);

        node.data.put("type", ctx.decltype().identifier().getText());
        node.children.add(visit(ctx.expression()));

        return node;
    }

    @Override
    public ANode visitBinary(final BinaryContext ctx) {
        final ANode node = new ANode(location(ctx), BINARY);

        if (ctx.MUL() != null) {
            node.data.put("operation", MUL);
        } else if (ctx.DIV() != null) {
            node.data.put("operation", DIV);
        } else if (ctx.REM() != null) {
            node.data.put("operation", REM);
        } else if (ctx.ADD() != null) {
            node.data.put("operation", ADD);
        } else if (ctx.SUB() != null) {
            node.data.put("operation", SUB);
        } else if (ctx.LSH() != null) {
            node.data.put("operation", LSH);
        } else if (ctx.RSH() != null) {
            node.data.put("operation", RSH);
        } else if (ctx.USH() != null) {
            node.data.put("operation", USH);
        } else if (ctx.LT() != null) {
            node.data.put("operation", LT);
        } else if (ctx.LTE() != null) {
            node.data.put("operation", LTE);
        } else if (ctx.GT() != null) {
            node.data.put("operation", GT);
        } else if (ctx.GTE() != null) {
            node.data.put("operation", GTE);
        } else if (ctx.EQ() != null) {
            node.data.put("operation", EQ);
        } else if (ctx.EQR() != null) {
            node.data.put("operation", EQR);
        } else if (ctx.NE() != null) {
            node.data.put("operation", NE);
        } else if (ctx.LT() != null) {
            node.data.put("operation", NER);
        } else if (ctx.BWAND() != null) {
            node.data.put("operation", BWAND);
        } else if (ctx.XOR() != null) {
            node.data.put("operation", XOR);
        } else if (ctx.BWOR() != null) {
            node.data.put("operation", BWOR);
        } else if (ctx.BOOLAND() != null) {
            node.data.put("operation", AND);
        } else if (ctx.BWOR() != null) {
            node.data.put("operation", OR);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        node.children.add(visit(ctx.expression(0)));
        node.children.add(visit(ctx.expression(1)));

        return node;
    }

    @Override
    public ANode visitConditional(final ConditionalContext ctx) {
        final ANode node = new ANode(location(ctx), CONDITIONAL);

        node.children.add(visit(ctx.expression(0)));
        node.children.add(visit(ctx.expression(1)));
        node.children.add(visit(ctx.expression(2)));

        return node;
    }

    @Override
    public ANode visitAssignment(final AssignmentContext ctx) {
        final ANode node = new ANode(location(ctx), ctx.ASSIGN() == null ? COMPOUND : ASSIGNMENT);

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

    private void visitExtstart(final ExtstartContext ctx, final ANode parent) {
        if (ctx.extprec() != null) {
            visitExtprec(ctx.extprec(), parent);
        } else if (ctx.extcast() != null) {
            visitExtcast(ctx.extcast(), parent);
        } else if (ctx.extvar() != null) {
            visitExtvar(ctx.extvar(), parent);
        } else if (ctx.extnew() != null) {
            visitExtnew(ctx.extnew(), parent);
        } else if (ctx.extstring() != null) {
            visitExtstring(ctx.extstring(), parent);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }
    }

    @Override
    public ANode visitExtstart(final ExtstartContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtprec(final ExtprecContext ctx, final ANode parent) {
        if (ctx.extprec() != null) {
            visitExtprec(ctx.extprec(), parent);
        } else if (ctx.extcast() != null) {
            visitExtcast(ctx.extcast(), parent);
        } else if (ctx.extvar() != null) {
            visitExtvar(ctx.extvar(), parent);
        } else if (ctx.extnew() != null) {
            visitExtnew(ctx.extnew(), parent);
        } else if (ctx.extstring() != null) {
            visitExtstring(ctx.extstring(), parent);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        if (ctx.extbrace() != null) {
            visitExtbrace(ctx.extbrace(), parent);
        } else if (ctx.extdot() != null) {
            visitExtdot(ctx.extdot(), parent);
        }
    }

    @Override
    public ANode visitExtprec(final ExtprecContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtcast(final ExtcastContext ctx, final ANode parent) {
        if (ctx.extprec() != null) {
            visitExtprec(ctx.extprec(), parent);
        } else if (ctx.extcast() != null) {
            visitExtcast(ctx.extcast(), parent);
        } else if (ctx.extvar() != null) {
            visitExtvar(ctx.extvar(), parent);
        } else if (ctx.extnew() != null) {
            visitExtnew(ctx.extnew(), parent);
        } else if (ctx.extstring() != null) {
            visitExtstring(ctx.extstring(), parent);
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }
        long x = 0;
        int y = 0;
        long z = 1;

        final ANode node = new ANode(location(ctx), CAST);

        node.data.put("type", ctx.decltype().identifier().getText());

        parent.children.add(node);
    }

    @Override
    public ANode visitExtcast(final ExtcastContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtbrace(final ExtbraceContext ctx, final ANode parent) {
        final ANode node = new ANode(location(ctx), BRACE);

        node.children.add(visit(ctx.expression()));

        parent.children.add(node);

        if (ctx.extbrace() != null) {
            visitExtbrace(ctx.extbrace(), parent);
        } else if (ctx.extdot() != null) {
            visitExtdot(ctx.extdot(), parent);
        }
    }

    @Override
    public ANode visitExtbrace(final ExtbraceContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtdot(final ExtdotContext ctx, final ANode parent) {
        if (ctx.extcall() != null) {
            visitExtcall(ctx.extcall(), parent);
        } else if (ctx.extfield() != null) {
            visitExtfield(ctx.extfield(), parent);
        }
    }

    @Override
    public ANode visitExtdot(final ExtdotContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtcall(final ExtcallContext ctx, final ANode parent) {
        final ANode node = new ANode(location(ctx), CALL);

        node.data.put("symbol", ctx.EXTID().getText());

        for (final ExpressionContext expression : ctx.arguments().expression()) {
            node.children.add(visit(expression));
        }

        parent.children.add(node);

        if (ctx.extbrace() != null) {
            visitExtbrace(ctx.extbrace(), parent);
        } else if (ctx.extdot() != null) {
            visitExtdot(ctx.extdot(), parent);
        }
    }

    @Override
    public ANode visitExtcall(final ExtcallContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtvar(final ExtvarContext ctx, final ANode parent) {
        final ANode node = new ANode(location(ctx), VAR);

        node.data.put("symbol", ctx.identifier().getText());

        parent.children.add(node);

        if (ctx.extbrace() != null) {
            visitExtbrace(ctx.extbrace(), parent);
        } else if (ctx.extdot() != null) {
            visitExtdot(ctx.extdot(), parent);
        }
    }

    @Override
    public ANode visitExtvar(final ExtvarContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtfield(final ExtfieldContext ctx, final ANode parent) {
        final ANode node = new ANode(location(ctx), FIELD);

        if (ctx.EXTID() != null) {
            node.data.put("symbol", ctx.EXTID());
        } else if (ctx.EXTINTEGER() != null) {
            node.data.put("symbol", ctx.EXTINTEGER());
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        parent.children.add(node);

        if (ctx.extbrace() != null) {
            visitExtbrace(ctx.extbrace(), parent);
        } else if (ctx.extdot() != null) {
            visitExtdot(ctx.extdot(), parent);
        }
    }

    @Override
    public ANode visitExtfield(final ExtfieldContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtnew(final ExtnewContext ctx, final ANode parent) {
        final ANode node;

        if (ctx.arguments() != null) {
            node = new ANode(location(ctx), NEWOBJ);

            for (final ExpressionContext expression : ctx.arguments().expression()) {
                node.children.add(visit(expression));
            }
        } else if (ctx.expression().size() > 0) {
            node = new ANode(location(ctx), NEWARRAY);

            for (final ExpressionContext expression : ctx.expression()) {
                node.children.add(visit(expression));
            }
        } else {
            throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
        }

        node.data.put("symbol", ctx.identifier().getText());

        parent.children.add(node);

        if (ctx.extdot() != null) {
            visitExtdot(ctx.extdot(), parent);
        }
    }

    @Override
    public ANode visitExtnew(final ExtnewContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtstring(final ExtstringContext ctx, final ANode parent) {
        final ANode node = new ANode(location(ctx), STRING);

        node.data.put("string", ctx.getText().substring(1, ctx.getText().length() - 1));

        parent.children.add(node);

        if (ctx.extbrace() != null) {
            visitExtbrace(ctx.extbrace(), parent);
        } else if (ctx.extdot() != null) {
            visitExtdot(ctx.extdot(), parent);
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
