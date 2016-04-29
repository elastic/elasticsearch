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
import org.elasticsearch.painless.tree.node.Node;

import static org.elasticsearch.painless.tree.node.Operation.ADD;
import static org.elasticsearch.painless.tree.node.Operation.AND;
import static org.elasticsearch.painless.tree.node.Operation.BWAND;
import static org.elasticsearch.painless.tree.node.Operation.BWNOT;
import static org.elasticsearch.painless.tree.node.Operation.BWOR;
import static org.elasticsearch.painless.tree.node.Operation.DIV;
import static org.elasticsearch.painless.tree.node.Operation.EQ;
import static org.elasticsearch.painless.tree.node.Operation.EQR;
import static org.elasticsearch.painless.tree.node.Operation.GT;
import static org.elasticsearch.painless.tree.node.Operation.GTE;
import static org.elasticsearch.painless.tree.node.Operation.LSH;
import static org.elasticsearch.painless.tree.node.Operation.LT;
import static org.elasticsearch.painless.tree.node.Operation.LTE;
import static org.elasticsearch.painless.tree.node.Operation.MUL;
import static org.elasticsearch.painless.tree.node.Operation.NE;
import static org.elasticsearch.painless.tree.node.Operation.NER;
import static org.elasticsearch.painless.tree.node.Operation.NOT;
import static org.elasticsearch.painless.tree.node.Operation.OR;
import static org.elasticsearch.painless.tree.node.Operation.REM;
import static org.elasticsearch.painless.tree.node.Operation.RSH;
import static org.elasticsearch.painless.tree.node.Operation.SUB;
import static org.elasticsearch.painless.tree.node.Operation.USH;
import static org.elasticsearch.painless.tree.node.Operation.XOR;
import static org.elasticsearch.painless.tree.node.Type.ASSIGNMENT;
import static org.elasticsearch.painless.tree.node.Type.BINARY;
import static org.elasticsearch.painless.tree.node.Type.BLOCK;
import static org.elasticsearch.painless.tree.node.Type.BRACE;
import static org.elasticsearch.painless.tree.node.Type.BREAK;
import static org.elasticsearch.painless.tree.node.Type.CALL;
import static org.elasticsearch.painless.tree.node.Type.CAST;
import static org.elasticsearch.painless.tree.node.Type.CHAR;
import static org.elasticsearch.painless.tree.node.Type.COMPOUND;
import static org.elasticsearch.painless.tree.node.Type.CONDITIONAL;
import static org.elasticsearch.painless.tree.node.Type.CONTINUE;
import static org.elasticsearch.painless.tree.node.Type.DECLARATION;
import static org.elasticsearch.painless.tree.node.Type.DECLVAR;
import static org.elasticsearch.painless.tree.node.Type.DO;
import static org.elasticsearch.painless.tree.node.Type.EXPRESSION;
import static org.elasticsearch.painless.tree.node.Type.EXTERNAL;
import static org.elasticsearch.painless.tree.node.Type.FALSE;
import static org.elasticsearch.painless.tree.node.Type.FIELD;
import static org.elasticsearch.painless.tree.node.Type.FOR;
import static org.elasticsearch.painless.tree.node.Type.IF;
import static org.elasticsearch.painless.tree.node.Type.NEWARRAY;
import static org.elasticsearch.painless.tree.node.Type.NEWOBJ;
import static org.elasticsearch.painless.tree.node.Type.NULL;
import static org.elasticsearch.painless.tree.node.Type.NUMERIC;
import static org.elasticsearch.painless.tree.node.Type.POST;
import static org.elasticsearch.painless.tree.node.Type.PRE;
import static org.elasticsearch.painless.tree.node.Type.RETURN;
import static org.elasticsearch.painless.tree.node.Type.SOURCE;
import static org.elasticsearch.painless.tree.node.Type.STRING;
import static org.elasticsearch.painless.tree.node.Type.THROW;
import static org.elasticsearch.painless.tree.node.Type.TRAP;
import static org.elasticsearch.painless.tree.node.Type.TRUE;
import static org.elasticsearch.painless.tree.node.Type.TRY;
import static org.elasticsearch.painless.tree.node.Type.UNARY;
import static org.elasticsearch.painless.tree.node.Type.VAR;
import static org.elasticsearch.painless.tree.node.Type.WHILE;

public class Walker extends PainlessParserBaseVisitor<Node> {
    public static Node buildPainlessTree(final String source) {
        return new Walker(source).source;
    }

    private Node source;

    private Walker(final String source) {
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
    public Node visitSource(final SourceContext ctx) {
        final Node node = new Node(location(ctx), SOURCE);

        for (final StatementContext statement : ctx.statement()) {
            node.children.add(visit(statement));
        }

        return node;
    }

    @Override
    public Node visitIf(final IfContext ctx) {
        final Node node = new Node(location(ctx), IF);

        node.children.add(visit(ctx.expression()));
        node.children.add(visit(ctx.block(0)));
        node.children.add(ctx.block(1) == null ? null : visit(ctx.block(1)));

        return node;
    }

    @Override
    public Node visitWhile(final WhileContext ctx) {
        final Node node = new Node(location(ctx), WHILE);

        node.children.add(visit(ctx.expression()));
        node.children.add(ctx.block() == null ? null : visit(ctx.block()));

        return node;
    }

    @Override
    public Node visitDo(final DoContext ctx) {
        final Node node = new Node(location(ctx), DO);

        node.children.add(visit(ctx.block()));
        node.children.add(visit(ctx.expression()));

        return node;
    }

    @Override
    public Node visitFor(final ForContext ctx) {
        final Node node = new Node(location(ctx), FOR);

        node.children.add(ctx.initializer() == null ? null : visit(ctx.initializer()));
        node.children.add(ctx.expression() == null ? null : visit(ctx.expression()));
        node.children.add(ctx.afterthought() == null ? null : visit(ctx.afterthought()));
        node.children.add(ctx.block() == null ? null : visit(ctx.block()));

        return node;
    }

    @Override
    public Node visitDecl(final DeclContext ctx) {
        return visit(ctx.declaration());
    }

    @Override
    public Node visitContinue(final ContinueContext ctx) {
        return new Node(location(ctx), CONTINUE);
    }

    @Override
    public Node visitBreak(final BreakContext ctx) {
        return new Node(location(ctx), BREAK);
    }

    @Override
    public Node visitReturn(final ReturnContext ctx) {
        final Node node = new Node(location(ctx), RETURN);

        node.children.add(visit(ctx.expression()));

        return node;
    }

    @Override
    public Node visitTry(final TryContext ctx) {
        final Node node = new Node(location(ctx), TRY);

        node.children.add(visit(ctx.block()));

        for (final TrapContext trap : ctx.trap()) {
            node.children.add(visit(trap));
        }

        return node;
    }

    @Override
    public Node visitThrow(final ThrowContext ctx) {
        final Node node = new Node(location(ctx), THROW);

        node.children.add(visit(ctx.expression()));

        return node;
    }

    @Override
    public Node visitExpr(final ExprContext ctx) {
        final Node node = new Node(location(ctx), EXPRESSION);

        node.children.add(visit(ctx.expression()));

        return node;
    }

    @Override
    public Node visitMultiple(final MultipleContext ctx) {
        final Node node = new Node(location(ctx), BLOCK);

        for (final StatementContext statement : ctx.statement()) {
            node.children.add(visit(statement));
        }

        return node;
    }

    @Override
    public Node visitSingle(final SingleContext ctx) {
        final Node node = new Node(location(ctx), BLOCK);

        node.children.add(visit(ctx.statement()));

        return node;
    }

    @Override
    public Node visitEmpty(final EmptyContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public Node visitEmptyscope(final EmptyscopeContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public Node visitInitializer(final InitializerContext ctx) {
        if (ctx.declaration() != null) {
            return visit(ctx.declaration());
        } else if (ctx.expression() != null) {
            return visit(ctx.expression());
        }

        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public Node visitAfterthought(final AfterthoughtContext ctx) {
        return visit(ctx.expression());
    }

    @Override
    public Node visitDeclaration(final DeclarationContext ctx) {
        final Node node = new Node(location(ctx), DECLARATION);

        final String type = ctx.decltype().identifier().getText();

        for (final DeclvarContext declvar : ctx.declvar()) {
            final Node var = visit(declvar);

            var.data.put("type", type);

            node.children.add(var);
        }

        return node;
    }

    @Override
    public Node visitDecltype(final DecltypeContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public Node visitDeclvar(final DeclvarContext ctx) {
        final Node node = new Node(location(ctx), DECLVAR);

        final String symbol = ctx.identifier().getText();
        node.data.put("symbol", symbol);

        node.children.add(ctx.expression() == null ? null : visit(ctx.expression()));

        return node;
    }

    @Override
    public Node visitTrap(final TrapContext ctx) {
        final Node node = new Node(location(ctx), TRAP);

        final String type = ctx.identifier(0).getText();
        final String symbol = ctx.identifier(1).getText();

        node.data.put("type", type);
        node.data.put("symbol", symbol);

        node.children.add(ctx.block() == null ? null : visit(ctx.block()));

        return node;
    }

    @Override
    public Node visitIdentifier(final IdentifierContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public Node visitGeneric(final GenericContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public Node visitPrecedence(final PrecedenceContext ctx) {
        return visit(ctx.expression());
    }

    @Override
    public Node visitNumeric(final NumericContext ctx) {
        final Node node = new Node(location(ctx), NUMERIC);

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
    public Node visitChar(final CharContext ctx) {
        final Node node = new Node(location(ctx), CHAR);

        node.data.put("char", ctx.CHAR().getText().charAt(1));

        return node;
    }

    @Override
    public Node visitTrue(final TrueContext ctx) {
        return new Node(location(ctx), TRUE);
    }

    @Override
    public Node visitFalse(FalseContext ctx) {
        return new Node(location(ctx), FALSE);
    }

    @Override
    public Node visitNull(final NullContext ctx) {
        return new Node(location(ctx), NULL);
    }

    @Override
    public Node visitPostinc(final PostincContext ctx) {
        final Node node = new Node(location(ctx), POST);

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
    public Node visitPreinc(final PreincContext ctx) {
        final Node node = new Node(location(ctx), PRE);

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
    public Node visitExternal(final ExternalContext ctx) {
        final Node node = new Node(location(ctx), EXTERNAL);

        visitExtstart(ctx.extstart(), node);

        return node;
    }

    @Override
    public Node visitUnary(final UnaryContext ctx) {
        if (ctx.SUB() != null && ctx.expression() instanceof NumericContext) {
            return visit(ctx.expression());
        } else {
            final Node node = new Node(location(ctx), UNARY);

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
    public Node visitCast(final CastContext ctx) {
        final Node node = new Node(location(ctx), CAST);

        node.data.put("type", ctx.decltype().identifier().getText());
        node.children.add(visit(ctx.expression()));

        return node;
    }

    @Override
    public Node visitBinary(final BinaryContext ctx) {
        final Node node = new Node(location(ctx), BINARY);

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
    public Node visitConditional(final ConditionalContext ctx) {
        final Node node = new Node(location(ctx), CONDITIONAL);

        node.children.add(visit(ctx.expression(0)));
        node.children.add(visit(ctx.expression(1)));
        node.children.add(visit(ctx.expression(2)));

        return node;
    }

    @Override
    public Node visitAssignment(final AssignmentContext ctx) {
        final Node node = new Node(location(ctx), ctx.ASSIGN() == null ? COMPOUND : ASSIGNMENT);

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

    private void visitExtstart(final ExtstartContext ctx, final Node parent) {
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
    public Node visitExtstart(final ExtstartContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtprec(final ExtprecContext ctx, final Node parent) {
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
    public Node visitExtprec(final ExtprecContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtcast(final ExtcastContext ctx, final Node parent) {
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

        final Node node = new Node(location(ctx), CAST);

        node.data.put("type", ctx.decltype().identifier().getText());

        parent.children.add(node);
    }

    @Override
    public Node visitExtcast(final ExtcastContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtbrace(final ExtbraceContext ctx, final Node parent) {
        final Node node = new Node(location(ctx), BRACE);

        node.children.add(visit(ctx.expression()));

        parent.children.add(node);

        if (ctx.extbrace() != null) {
            visitExtbrace(ctx.extbrace(), parent);
        } else if (ctx.extdot() != null) {
            visitExtdot(ctx.extdot(), parent);
        }
    }

    @Override
    public Node visitExtbrace(final ExtbraceContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtdot(final ExtdotContext ctx, final Node parent) {
        if (ctx.extcall() != null) {
            visitExtcall(ctx.extcall(), parent);
        } else if (ctx.extfield() != null) {
            visitExtfield(ctx.extfield(), parent);
        }
    }

    @Override
    public Node visitExtdot(final ExtdotContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtcall(final ExtcallContext ctx, final Node parent) {
        final Node node = new Node(location(ctx), CALL);

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
    public Node visitExtcall(final ExtcallContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtvar(final ExtvarContext ctx, final Node parent) {
        final Node node = new Node(location(ctx), VAR);

        node.data.put("symbol", ctx.identifier().getText());

        parent.children.add(node);

        if (ctx.extbrace() != null) {
            visitExtbrace(ctx.extbrace(), parent);
        } else if (ctx.extdot() != null) {
            visitExtdot(ctx.extdot(), parent);
        }
    }

    @Override
    public Node visitExtvar(final ExtvarContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtfield(final ExtfieldContext ctx, final Node parent) {
        final Node node = new Node(location(ctx), FIELD);

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
    public Node visitExtfield(final ExtfieldContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtnew(final ExtnewContext ctx, final Node parent) {
        final Node node;

        if (ctx.arguments() != null) {
            node = new Node(location(ctx), NEWOBJ);

            for (final ExpressionContext expression : ctx.arguments().expression()) {
                node.children.add(visit(expression));
            }
        } else if (ctx.expression().size() > 0) {
            node = new Node(location(ctx), NEWARRAY);

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
    public Node visitExtnew(final ExtnewContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    private void visitExtstring(final ExtstringContext ctx, final Node parent) {
        final Node node = new Node(location(ctx), STRING);

        node.data.put("string", ctx.getText().substring(1, ctx.getText().length() - 1));

        parent.children.add(node);

        if (ctx.extbrace() != null) {
            visitExtbrace(ctx.extbrace(), parent);
        } else if (ctx.extdot() != null) {
            visitExtdot(ctx.extdot(), parent);
        }
    }

    @Override
    public Node visitExtstring(final ExtstringContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }

    @Override
    public Node visitArguments(final ArgumentsContext ctx) {
        throw new IllegalStateException("Error " + location(ctx) + ": Unexpected state.");
    }
}
