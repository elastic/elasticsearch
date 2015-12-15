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

package org.elasticsearch.plan.a;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.antlr.v4.runtime.ParserRuleContext;

import static org.elasticsearch.plan.a.Adapter.*;
import static org.elasticsearch.plan.a.Definition.*;
import static org.elasticsearch.plan.a.PlanAParser.*;

class Analyzer extends PlanAParserBaseVisitor<Void> {
    private static class Variable {
        final String name;
        final Type type;
        final int slot;

        private Variable(final String name, final Type type, final int slot) {
            this.name = name;
            this.type = type;
            this.slot = slot;
        }
    }

    static void analyze(final Adapter adapter) {
        new Analyzer(adapter);
    }

    private final Adapter adapter;
    private final Definition definition;
    private final CompilerSettings settings;

    private final Deque<Integer> scopes;
    private final Deque<Variable> variables;

    private Analyzer(final Adapter adapter) {
        this.adapter = adapter;
        definition = adapter.definition;
        settings = adapter.settings;

        scopes = new ArrayDeque<>();
        variables = new ArrayDeque<>();

        incrementScope();
        addVariable(null, "this", definition.execType);
        addVariable(null, "input", definition.smapType);

        adapter.createStatementMetadata(adapter.root);
        visit(adapter.root);

        decrementScope();
    }

    void incrementScope() {
        scopes.push(0);
    }

    void decrementScope() {
        int remove = scopes.pop();

        while (remove > 0) {
            variables.pop();
            --remove;
        }
    }

    Variable getVariable(final String name) {
        final Iterator<Variable> itr = variables.iterator();

        while (itr.hasNext()) {
            final Variable variable = itr.next();

            if (variable.name.equals(name)) {
                return variable;
            }
        }

        return null;
    }

    Variable addVariable(final ParserRuleContext source, final String name, final Type type) {
        if (getVariable(name) != null) {
            if (source == null) {
                throw new IllegalArgumentException("Argument name [" + name + "] already defined within the scope.");
            } else {
                throw new IllegalArgumentException(
                        error(source) + "Variable name [" + name + "] already defined within the scope.");
            }
        }

        final Variable previous = variables.peekFirst();
        int slot = 0;

        if (previous != null) {
            slot += previous.slot + previous.type.type.getSize();
        }

        final Variable variable = new Variable(name, type, slot);
        variables.push(variable);

        final int update = scopes.pop() + 1;
        scopes.push(update);

        return variable;
    }

    @Override
    public Void visitSource(final SourceContext ctx) {
        final StatementMetadata sourcesmd = adapter.getStatementMetadata(ctx);
        final List<StatementContext> statectxs = ctx.statement();
        final StatementContext lastctx = statectxs.get(statectxs.size() - 1);

        incrementScope();

        for (final StatementContext statectx : statectxs) {
            if (sourcesmd.allExit) {
                throw new IllegalArgumentException(error(statectx) +
                        "Statement will never be executed because all prior paths exit.");
            }

            final StatementMetadata statesmd = adapter.createStatementMetadata(statectx);
            statesmd.last = statectx == lastctx;
            visit(statectx);

            if (statesmd.anyContinue) {
                throw new IllegalArgumentException(error(statectx) +
                        "Cannot have a continue statement outside of a loop.");
            }

            if (statesmd.anyBreak) {
                throw new IllegalArgumentException(error(statectx) +
                        "Cannot have a break statement outside of a loop.");
            }

            sourcesmd.allExit = statesmd.allExit;
            sourcesmd.allReturn = statesmd.allReturn;
        }

        decrementScope();

        return null;
    }

    @Override
    public Void visitIf(final IfContext ctx) {
        final StatementMetadata ifsmd = adapter.getStatementMetadata(ctx);

        incrementScope();

        final ExpressionContext exprctx = adapter.updateExpressionTree(ctx.expression());
        final ExpressionMetadata expremd = adapter.createExpressionMetadata(exprctx);
        expremd.to = definition.booleanType;
        visit(exprctx);
        markCast(expremd);

        if (expremd.postConst != null) {
            throw new IllegalArgumentException(error(ctx) + "If statement is not necessary.");
        }

        final BlockContext blockctx0 = ctx.block(0);
        final StatementMetadata blocksmd0 = adapter.createStatementMetadata(blockctx0);
        blocksmd0.last = ifsmd.last;
        visit(blockctx0);

        ifsmd.anyReturn = blocksmd0.anyReturn;
        ifsmd.anyBreak = blocksmd0.anyBreak;
        ifsmd.anyContinue = blocksmd0.anyContinue;

        if (ctx.ELSE() != null) {
            final BlockContext blockctx1 = ctx.block(1);
            final StatementMetadata blocksmd1 = adapter.createStatementMetadata(blockctx1);
            blocksmd1.last = ifsmd.last;
            visit(blockctx1);

            ifsmd.allExit = blocksmd0.allExit && blocksmd1.allExit;
            ifsmd.allReturn = blocksmd0.allReturn && blocksmd1.allReturn;
            ifsmd.anyReturn |= blocksmd1.anyReturn;
            ifsmd.allBreak = blocksmd0.allBreak && blocksmd1.allBreak;
            ifsmd.anyBreak |= blocksmd1.anyBreak;
            ifsmd.allContinue = blocksmd0.allContinue && blocksmd1.allContinue;
            ifsmd.anyContinue |= blocksmd1.anyContinue;
        }

        decrementScope();

        return null;
    }

    @Override
    public Void visitWhile(final WhileContext ctx) {
        final StatementMetadata whilesmd = adapter.getStatementMetadata(ctx);

        incrementScope();

        final ExpressionContext exprctx = adapter.updateExpressionTree(ctx.expression());
        final ExpressionMetadata expremd = adapter.createExpressionMetadata(exprctx);
        expremd.to = definition.booleanType;
        visit(exprctx);
        markCast(expremd);

        boolean exitrequired = false;

        if (expremd.postConst != null) {
            boolean constant = (boolean)expremd.postConst;

            if (!constant) {
                throw new IllegalArgumentException(error(ctx) + "The loop will never be executed.");
            }

            exitrequired = true;
        }

        final BlockContext blockctx = ctx.block();

        if (blockctx != null) {
            final StatementMetadata blocksmd = adapter.createStatementMetadata(blockctx);
            visit(blockctx);

            if (blocksmd.allReturn) {
                throw new IllegalArgumentException(error(ctx) + "All paths return so the loop is not necessary.");
            }

            if (blocksmd.allBreak) {
                throw new IllegalArgumentException(error(ctx) + "All paths break so the loop is not necessary.");
            }

            if (exitrequired && !blocksmd.anyReturn && !blocksmd.anyBreak) {
                throw new IllegalArgumentException(error(ctx) + "The loop will never exit.");
            }

            if (exitrequired && blocksmd.anyReturn && !blocksmd.anyBreak) {
                whilesmd.allExit = true;
                whilesmd.allReturn = true;
            }
        } else if (exitrequired) {
            throw new IllegalArgumentException(error(ctx) + "The loop will never exit.");
        }

        decrementScope();

        return null;
    }

    @Override
    public Void visitDo(final DoContext ctx) {
        final StatementMetadata dosmd = adapter.getStatementMetadata(ctx);

        incrementScope();

        final BlockContext blockctx = ctx.block();
        final StatementMetadata blocksmd = adapter.createStatementMetadata(blockctx);
        visit(blockctx);

        if (blocksmd.allReturn) {
            throw new IllegalArgumentException(error(ctx) + "All paths return so the loop is not necessary.");
        }

        if (blocksmd.allBreak) {
            throw new IllegalArgumentException(error(ctx) + "All paths break so the loop is not necessary.");
        }

        if (blocksmd.allContinue) {
            throw new IllegalArgumentException(error(ctx) + "The loop will never exit.");
        }

        final ExpressionContext exprctx = adapter.updateExpressionTree(ctx.expression());
        final ExpressionMetadata expremd = adapter.createExpressionMetadata(exprctx);
        expremd.to = definition.booleanType;
        visit(exprctx);
        markCast(expremd);

        if (expremd.postConst != null) {
            final boolean exitrequired = (boolean)expremd.postConst;

            if (exitrequired && !blocksmd.anyReturn && !blocksmd.anyBreak) {
                throw new IllegalArgumentException(error(ctx) + "The loop will never exit.");
            }

            if (exitrequired && blocksmd.anyReturn && !blocksmd.anyBreak) {
                dosmd.allExit = true;
                dosmd.allReturn = true;
            }

            if (!exitrequired && !blocksmd.anyContinue) {
                throw new IllegalArgumentException(error(ctx) + "All paths exit so the loop is not necessary.");
            }
        }

        decrementScope();

        return null;
    }

    @Override
    public Void visitFor(final ForContext ctx) {
        final StatementMetadata forsmd = adapter.getStatementMetadata(ctx);
        boolean exitrequired = false;

        incrementScope();

        final InitializerContext initctx = ctx.initializer();

        if (initctx != null) {
            adapter.createStatementMetadata(initctx);
            visit(initctx);
        }

        final ExpressionContext exprctx = adapter.updateExpressionTree(ctx.expression());

        if (exprctx != null) {
            final ExpressionMetadata expremd = adapter.createExpressionMetadata(exprctx);
            expremd.to = definition.booleanType;
            visit(exprctx);
            markCast(expremd);

            if (expremd.postConst != null) {
                boolean constant = (boolean)expremd.postConst;

                if (!constant) {
                    throw new IllegalArgumentException(error(ctx) + "The loop will never be executed.");
                }

                exitrequired = true;
            }
        } else {
            exitrequired = true;
        }

        final AfterthoughtContext atctx = ctx.afterthought();

        if (atctx != null) {
            adapter.createStatementMetadata(atctx);
            visit(atctx);
        }

        final BlockContext blockctx = ctx.block();

        if (blockctx != null) {
            final StatementMetadata blocksmd = adapter.createStatementMetadata(blockctx);
            visit(blockctx);

            if (blocksmd.allReturn) {
                throw new IllegalArgumentException(error(ctx) + "All paths return so the loop is not necessary.");
            }

            if (blocksmd.allBreak) {
                throw new IllegalArgumentException(error(ctx) + "All paths break so the loop is not necessary.");
            }

            if (exitrequired && !blocksmd.anyReturn && !blocksmd.anyBreak) {
                throw new IllegalArgumentException(error(ctx) + "The loop will never exit.");
            }

            if (exitrequired && blocksmd.anyReturn && !blocksmd.anyBreak) {
                forsmd.allExit = true;
                forsmd.allReturn = true;
            }
        } else if (exitrequired) {
            throw new IllegalArgumentException(error(ctx) + "The loop will never exit.");
        }

        decrementScope();

        return null;
    }

    @Override
    public Void visitDecl(final DeclContext ctx) {
        final DeclarationContext declctx = ctx.declaration();
        adapter.createStatementMetadata(declctx);
        visit(declctx);

        return null;
    }

    @Override
    public Void visitContinue(final ContinueContext ctx) {
        final StatementMetadata continuesmd = adapter.getStatementMetadata(ctx);

        continuesmd.allExit = true;
        continuesmd.allContinue = true;
        continuesmd.anyContinue = true;

        return null;
    }

    @Override
    public Void visitBreak(final BreakContext ctx) {
        final StatementMetadata breaksmd = adapter.getStatementMetadata(ctx);

        breaksmd.allExit = true;
        breaksmd.allBreak = true;
        breaksmd.anyBreak = true;

        return null;
    }

    @Override
    public Void visitReturn(final ReturnContext ctx) {
        final StatementMetadata returnsmd = adapter.getStatementMetadata(ctx);

        final ExpressionContext exprctx = adapter.updateExpressionTree(ctx.expression());
        final ExpressionMetadata expremd = adapter.createExpressionMetadata(exprctx);
        expremd.to = definition.objectType;
        visit(exprctx);
        markCast(expremd);

        returnsmd.allExit = true;
        returnsmd.allReturn = true;
        returnsmd.anyReturn = true;

        return null;
    }

    @Override
    public Void visitExpr(final ExprContext ctx) {
        final StatementMetadata exprsmd = adapter.getStatementMetadata(ctx);
        final ExpressionContext exprctx = adapter.updateExpressionTree(ctx.expression());
        final ExpressionMetadata expremd = adapter.createExpressionMetadata(exprctx);
        expremd.read = exprsmd.last;
        visit(exprctx);

        if (!expremd.statement && !exprsmd.last) {
            throw new IllegalArgumentException(error(ctx) + "Not a statement.");
        }

        final boolean rtn = exprsmd.last && expremd.from.sort != Sort.VOID;
        exprsmd.allExit = rtn;
        exprsmd.allReturn = rtn;
        exprsmd.anyReturn = rtn;
        expremd.to = rtn ? definition.objectType : expremd.from;
        markCast(expremd);

        return null;
    }

    @Override
    public Void visitMultiple(final MultipleContext ctx) {
        final StatementMetadata multiplesmd = adapter.getStatementMetadata(ctx);
        final List<StatementContext> statectxs = ctx.statement();
        final StatementContext lastctx = statectxs.get(statectxs.size() - 1);

        for (StatementContext statectx : statectxs) {
            if (multiplesmd.allExit) {
                throw new IllegalArgumentException(error(statectx) +
                        "Statement will never be executed because all prior paths exit.");
            }

            final StatementMetadata statesmd = adapter.createStatementMetadata(statectx);
            statesmd.last = multiplesmd.last && statectx == lastctx;
            visit(statectx);

            multiplesmd.allExit = statesmd.allExit;
            multiplesmd.allReturn = statesmd.allReturn && !statesmd.anyBreak && !statesmd.anyContinue;
            multiplesmd.anyReturn |= statesmd.anyReturn;
            multiplesmd.allBreak = !statesmd.anyReturn && statesmd.allBreak && !statesmd.anyContinue;
            multiplesmd.anyBreak |= statesmd.anyBreak;
            multiplesmd.allContinue = !statesmd.anyReturn && !statesmd.anyBreak && statesmd.allContinue;
            multiplesmd.anyContinue |= statesmd.anyContinue;
        }

        return null;
    }

    @Override
    public Void visitSingle(final SingleContext ctx) {
        final StatementMetadata singlesmd = adapter.getStatementMetadata(ctx);

        final StatementContext statectx = ctx.statement();
        final StatementMetadata statesmd = adapter.createStatementMetadata(statectx);
        statesmd.last = singlesmd.last;
        visit(statectx);

        singlesmd.allExit = statesmd.allExit;
        singlesmd.allReturn = statesmd.allReturn;
        singlesmd.anyReturn = statesmd.anyReturn;
        singlesmd.allBreak = statesmd.allBreak;
        singlesmd.anyBreak = statesmd.anyBreak;
        singlesmd.allContinue = statesmd.allContinue;
        singlesmd.anyContinue = statesmd.anyContinue;

        return null;
    }

    @Override
    public Void visitEmpty(final EmptyContext ctx) {
        throw new UnsupportedOperationException(error(ctx) + "Unexpected parser state.");
    }

    @Override
    public Void visitInitializer(InitializerContext ctx) {
        final DeclarationContext declctx = ctx.declaration();
        final ExpressionContext exprctx = adapter.updateExpressionTree(ctx.expression());

        if (declctx != null) {
            adapter.createStatementMetadata(declctx);
            visit(declctx);
        } else if (exprctx != null) {
            final ExpressionMetadata expremd = adapter.createExpressionMetadata(exprctx);
            expremd.read = false;
            visit(exprctx);

            expremd.to = expremd.from;
            markCast(expremd);

            if (!expremd.statement) {
                throw new IllegalArgumentException(error(exprctx) +
                        "The intializer of a for loop must be a statement.");
            }
        } else {
            throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
        }

        return null;
    }

    @Override
    public Void visitAfterthought(AfterthoughtContext ctx) {
        ExpressionContext exprctx = adapter.updateExpressionTree(ctx.expression());

        if (exprctx != null) {
            final ExpressionMetadata expremd = adapter.createExpressionMetadata(exprctx);
            expremd.read = false;
            visit(exprctx);

            expremd.to = expremd.from;
            markCast(expremd);

            if (!expremd.statement) {
                throw new IllegalArgumentException(error(exprctx) +
                        "The afterthought of a for loop must be a statement.");
            }
        }

        return null;
    }

    @Override
    public Void visitDeclaration(final DeclarationContext ctx) {
        final DecltypeContext decltypectx = ctx.decltype();
        final ExpressionMetadata decltypeemd = adapter.createExpressionMetadata(decltypectx);
        visit(decltypectx);

        for (final DeclvarContext declvarctx : ctx.declvar()) {
            final ExpressionMetadata declvaremd = adapter.createExpressionMetadata(declvarctx);
            declvaremd.to = decltypeemd.from;
            visit(declvarctx);
        }

        return null;
    }

    @Override
    public Void visitDecltype(final DecltypeContext ctx) {
        final ExpressionMetadata decltypeemd = adapter.getExpressionMetadata(ctx);

        final String name = ctx.getText();
        decltypeemd.from = definition.getType(name);

        return null;
    }

    @Override
    public Void visitDeclvar(final DeclvarContext ctx) {
        final ExpressionMetadata declvaremd = adapter.getExpressionMetadata(ctx);

        final String name = ctx.ID().getText();
        declvaremd.postConst = addVariable(ctx, name, declvaremd.to).slot;

        final ExpressionContext exprctx = adapter.updateExpressionTree(ctx.expression());

        if (exprctx != null) {
            final ExpressionMetadata expremd = adapter.createExpressionMetadata(exprctx);
            expremd.to = declvaremd.to;
            visit(exprctx);
            markCast(expremd);
        }

        return null;
    }

    @Override
    public Void visitPrecedence(final PrecedenceContext ctx) {
        throw new UnsupportedOperationException(error(ctx) + "Unexpected parser state.");
    }

    @Override
    public Void visitNumeric(final NumericContext ctx) {
        final ExpressionMetadata numericemd = adapter.getExpressionMetadata(ctx);
        final boolean negate = ctx.parent instanceof UnaryContext && ((UnaryContext)ctx.parent).SUB() != null;

        if (ctx.DECIMAL() != null) {
            final String svalue = (negate ? "-" : "") + ctx.DECIMAL().getText();

            if (svalue.endsWith("f") || svalue.endsWith("F")) {
                try {
                    numericemd.from = definition.floatType;
                    numericemd.preConst = Float.parseFloat(svalue.substring(0, svalue.length() - 1));
                } catch (NumberFormatException exception) {
                    throw new IllegalArgumentException(error(ctx) + "Invalid float constant [" + svalue + "].");
                }
            } else {
                try {
                    numericemd.from = definition.doubleType;
                    numericemd.preConst = Double.parseDouble(svalue);
                } catch (NumberFormatException exception) {
                    throw new IllegalArgumentException(error(ctx) + "Invalid double constant [" + svalue + "].");
                }
            }
        } else {
            String svalue = negate ? "-" : "";
            int radix;

            if (ctx.OCTAL() != null) {
                svalue += ctx.OCTAL().getText();
                radix = 8;
            } else if (ctx.INTEGER() != null) {
                svalue += ctx.INTEGER().getText();
                radix = 10;
            } else if (ctx.HEX() != null) {
                svalue += ctx.HEX().getText();
                radix = 16;
            } else {
                throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
            }

            if (svalue.endsWith("d") || svalue.endsWith("D")) {
                try {
                    numericemd.from = definition.doubleType;
                    numericemd.preConst = Double.parseDouble(svalue.substring(0, svalue.length() - 1));
                } catch (NumberFormatException exception) {
                    throw new IllegalArgumentException(error(ctx) + "Invalid float constant [" + svalue + "].");
                }
            } else if (svalue.endsWith("f") || svalue.endsWith("F")) {
                try {
                    numericemd.from = definition.floatType;
                    numericemd.preConst = Float.parseFloat(svalue.substring(0, svalue.length() - 1));
                } catch (NumberFormatException exception) {
                    throw new IllegalArgumentException(error(ctx) + "Invalid float constant [" + svalue + "].");
                }
            } else if (svalue.endsWith("l") || svalue.endsWith("L")) {
                try {
                    numericemd.from = definition.longType;
                    numericemd.preConst = Long.parseLong(svalue.substring(0, svalue.length() - 1), radix);
                } catch (NumberFormatException exception) {
                    throw new IllegalArgumentException(error(ctx) + "Invalid long constant [" + svalue + "].");
                }
            } else {
                try {
                    final Type type = numericemd.to;
                    final Sort sort = type == null ? Sort.INT : type.sort;
                    final int value = Integer.parseInt(svalue, radix);

                    if (sort == Sort.BYTE && value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
                        numericemd.from = definition.byteType;
                        numericemd.preConst = (byte)value;
                    } else if (sort == Sort.CHAR && value >= Character.MIN_VALUE && value <= Character.MAX_VALUE) {
                        numericemd.from = definition.charType;
                        numericemd.preConst = (char)value;
                    } else if (sort == Sort.SHORT && value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
                        numericemd.from = definition.shortType;
                        numericemd.preConst = (short)value;
                    } else {
                        numericemd.from = definition.intType;
                        numericemd.preConst = value;
                    }
                } catch (NumberFormatException exception) {
                    throw new IllegalArgumentException(error(ctx) + "Invalid int constant [" + svalue + "].");
                }
            }
        }

        return null;
    }

    @Override
    public Void visitChar(final CharContext ctx) {
        final ExpressionMetadata charemd = adapter.getExpressionMetadata(ctx);

        if (ctx.CHAR() == null) {
            throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
        }

        charemd.preConst = ctx.CHAR().getText().charAt(0);
        charemd.from = definition.charType;

        return null;
    }

    @Override
    public Void visitTrue(final TrueContext ctx) {
        final ExpressionMetadata trueemd = adapter.getExpressionMetadata(ctx);

        if (ctx.TRUE() == null) {
            throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
        }

        trueemd.preConst = true;
        trueemd.from = definition.booleanType;

        return null;
    }

    @Override
    public Void visitFalse(final FalseContext ctx) {
        final ExpressionMetadata falseemd = adapter.getExpressionMetadata(ctx);

        if (ctx.FALSE() == null) {
            throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
        }

        falseemd.preConst = false;
        falseemd.from = definition.booleanType;

        return null;
    }

    @Override
    public Void visitNull(final NullContext ctx) {
        final ExpressionMetadata nullemd = adapter.getExpressionMetadata(ctx);

        if (ctx.NULL() == null) {
            throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
        }

        nullemd.isNull = true;

        if (nullemd.to != null) {
            if (nullemd.to.sort.primitive) {
                throw new IllegalArgumentException("Cannot cast null to a primitive type [" + nullemd.to.name + "].");
            }

            nullemd.from = nullemd.to;
        } else {
            nullemd.from = definition.objectType;
        }

        return null;
    }

    @Override
    public Void visitExternal(final ExternalContext ctx) {
        final ExpressionMetadata extemd = adapter.getExpressionMetadata(ctx);

        final ExtstartContext extstartctx = ctx.extstart();
        final ExternalMetadata extstartemd = adapter.createExternalMetadata(extstartctx);
        extstartemd.read = extemd.read;
        visit(extstartctx);

        extemd.statement = extstartemd.statement;
        extemd.preConst = extstartemd.constant;
        extemd.from = extstartemd.current;
        extemd.typesafe = extstartemd.current.sort != Sort.DEF;

        return null;
    }

    @Override
    public Void visitPostinc(final PostincContext ctx) {
        final ExpressionMetadata postincemd = adapter.getExpressionMetadata(ctx);

        final ExtstartContext extstartctx = ctx.extstart();
        final ExternalMetadata extstartemd = adapter.createExternalMetadata(extstartctx);
        extstartemd.read = postincemd.read;
        extstartemd.storeExpr = ctx.increment();
        extstartemd.token = ADD;
        extstartemd.post = true;
        visit(extstartctx);

        postincemd.statement = true;
        postincemd.from = extstartemd.read ? extstartemd.current : definition.voidType;
        postincemd.typesafe = extstartemd.current.sort != Sort.DEF;

        return null;
    }

    @Override
    public Void visitPreinc(final PreincContext ctx) {
        final ExpressionMetadata preincemd = adapter.getExpressionMetadata(ctx);

        final ExtstartContext extstartctx = ctx.extstart();
        final ExternalMetadata extstartemd = adapter.createExternalMetadata(extstartctx);
        extstartemd.read = preincemd.read;
        extstartemd.storeExpr = ctx.increment();
        extstartemd.token = ADD;
        extstartemd.pre = true;
        visit(extstartctx);

        preincemd.statement = true;
        preincemd.from = extstartemd.read ? extstartemd.current : definition.voidType;
        preincemd.typesafe = extstartemd.current.sort != Sort.DEF;

        return null;
    }

    @Override
    public Void visitUnary(final UnaryContext ctx) {
        final ExpressionMetadata unaryemd = adapter.getExpressionMetadata(ctx);

        final ExpressionContext exprctx = adapter.updateExpressionTree(ctx.expression());
        final ExpressionMetadata expremd = adapter.createExpressionMetadata(exprctx);

        if (ctx.BOOLNOT() != null) {
            expremd.to = definition.booleanType;
            visit(exprctx);
            markCast(expremd);

            if (expremd.postConst != null) {
                unaryemd.preConst = !(boolean)expremd.postConst;
            }

            unaryemd.from = definition.booleanType;
        } else if (ctx.BWNOT() != null || ctx.ADD() != null || ctx.SUB() != null) {
            visit(exprctx);

            final Type promote = promoteNumeric(expremd.from, ctx.BWNOT() == null, true);

            if (promote == null) {
                throw new ClassCastException("Cannot apply [" + ctx.getChild(0).getText() + "] " +
                        "operation to type [" + expremd.from.name + "].");
            }

            expremd.to = promote;
            markCast(expremd);

            if (expremd.postConst != null) {
                final Sort sort = promote.sort;

                if (ctx.BWNOT() != null) {
                    if (sort == Sort.INT) {
                        unaryemd.preConst = ~(int)expremd.postConst;
                    } else if (sort == Sort.LONG) {
                        unaryemd.preConst = ~(long)expremd.postConst;
                    } else {
                        throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
                    }
                } else if (ctx.SUB() != null) {
                    if (exprctx instanceof NumericContext) {
                        unaryemd.preConst = expremd.postConst;
                    } else {
                        if (sort == Sort.INT) {
                            if (settings.getNumericOverflow()) {
                                unaryemd.preConst = -(int)expremd.postConst;
                            } else {
                                unaryemd.preConst = Math.negateExact((int)expremd.postConst);
                            }
                        } else if (sort == Sort.LONG) {
                            if (settings.getNumericOverflow()) {
                                unaryemd.preConst = -(long)expremd.postConst;
                            } else {
                                unaryemd.preConst = Math.negateExact((long)expremd.postConst);
                            }
                        } else if (sort == Sort.FLOAT) {
                            unaryemd.preConst = -(float)expremd.postConst;
                        } else if (sort == Sort.DOUBLE) {
                            unaryemd.preConst = -(double)expremd.postConst;
                        } else {
                            throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
                        }
                    }
                } else if (ctx.ADD() != null) {
                    if (sort == Sort.INT) {
                        unaryemd.preConst = +(int)expremd.postConst;
                    } else if (sort == Sort.LONG) {
                        unaryemd.preConst = +(long)expremd.postConst;
                    } else if (sort == Sort.FLOAT) {
                        unaryemd.preConst = +(float)expremd.postConst;
                    } else if (sort == Sort.DOUBLE) {
                        unaryemd.preConst = +(double)expremd.postConst;
                    } else {
                        throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
                    }
                } else {
                    throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
                }
            }

            unaryemd.from = promote;
            unaryemd.typesafe = expremd.typesafe;
        } else {
            throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
        }

        return null;
    }

    @Override
    public Void visitCast(final CastContext ctx) {
        final ExpressionMetadata castemd = adapter.getExpressionMetadata(ctx);

        final DecltypeContext decltypectx = ctx.decltype();
        final ExpressionMetadata decltypemd = adapter.createExpressionMetadata(decltypectx);
        visit(decltypectx);

        final Type type = decltypemd.from;
        castemd.from = type;

        final ExpressionContext exprctx = adapter.updateExpressionTree(ctx.expression());
        final ExpressionMetadata expremd = adapter.createExpressionMetadata(exprctx);
        expremd.to = type;
        expremd.explicit = true;
        visit(exprctx);
        markCast(expremd);

        if (expremd.postConst != null) {
            castemd.preConst = expremd.postConst;
        }

        castemd.typesafe = expremd.typesafe && castemd.from.sort != Sort.DEF;

        return null;
    }

    @Override
    public Void visitBinary(final BinaryContext ctx) {
        final ExpressionMetadata binaryemd = adapter.getExpressionMetadata(ctx);

        final ExpressionContext exprctx0 = adapter.updateExpressionTree(ctx.expression(0));
        final ExpressionMetadata expremd0 = adapter.createExpressionMetadata(exprctx0);
        visit(exprctx0);

        final ExpressionContext exprctx1 = adapter.updateExpressionTree(ctx.expression(1));
        final ExpressionMetadata expremd1 = adapter.createExpressionMetadata(exprctx1);
        visit(exprctx1);

        final boolean decimal = ctx.MUL() != null || ctx.DIV() != null || ctx.REM() != null || ctx.SUB() != null;
        final boolean add = ctx.ADD() != null;
        final boolean xor = ctx.BWXOR() != null;
        final Type promote = add ? promoteAdd(expremd0.from, expremd1.from) :
                             xor ? promoteXor(expremd0.from, expremd1.from) :
                                   promoteNumeric(expremd0.from, expremd1.from, decimal, true);

        if (promote == null) {
            throw new ClassCastException("Cannot apply [" + ctx.getChild(1).getText() + "] " +
                    "operation to types [" + expremd0.from.name + "] and [" + expremd1.from.name + "].");
        }

        final Sort sort = promote.sort;
        expremd0.to = add && sort == Sort.STRING ? expremd0.from : promote;
        expremd1.to = add && sort == Sort.STRING ? expremd1.from : promote;
        markCast(expremd0);
        markCast(expremd1);

        if (expremd0.postConst != null && expremd1.postConst != null) {
            if (ctx.MUL() != null) {
                if (sort == Sort.INT) {
                    if (settings.getNumericOverflow()) {
                        binaryemd.preConst = (int)expremd0.postConst * (int)expremd1.postConst;
                    } else {
                        binaryemd.preConst = Math.multiplyExact((int)expremd0.postConst, (int)expremd1.postConst);
                    }
                } else if (sort == Sort.LONG) {
                    if (settings.getNumericOverflow()) {
                        binaryemd.preConst = (long)expremd0.postConst * (long)expremd1.postConst;
                    } else {
                        binaryemd.preConst = Math.multiplyExact((long)expremd0.postConst, (long)expremd1.postConst);
                    }
                } else if (sort == Sort.FLOAT) {
                    if (settings.getNumericOverflow()) {
                        binaryemd.preConst = (float)expremd0.postConst * (float)expremd1.postConst;
                    } else {
                        binaryemd.preConst = Utility.multiplyWithoutOverflow((float)expremd0.postConst, (float)expremd1.postConst);
                    }
                } else if (sort == Sort.DOUBLE) {
                    if (settings.getNumericOverflow()) {
                        binaryemd.preConst = (double)expremd0.postConst * (double)expremd1.postConst;
                    } else {
                        binaryemd.preConst = Utility.multiplyWithoutOverflow((double)expremd0.postConst, (double)expremd1.postConst);
                    }
                } else {
                    throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
                }
            } else if (ctx.DIV() != null) {
                if (sort == Sort.INT) {
                    if (settings.getNumericOverflow()) {
                        binaryemd.preConst = (int)expremd0.postConst / (int)expremd1.postConst;
                    } else {
                        binaryemd.preConst = Utility.divideWithoutOverflow((int)expremd0.postConst, (int)expremd1.postConst);
                    }
                } else if (sort == Sort.LONG) {
                    if (settings.getNumericOverflow()) {
                        binaryemd.preConst = (long)expremd0.postConst / (long)expremd1.postConst;
                    } else {
                        binaryemd.preConst = Utility.divideWithoutOverflow((long)expremd0.postConst, (long)expremd1.postConst);
                    }
                } else if (sort == Sort.FLOAT) {
                    if (settings.getNumericOverflow()) {
                        binaryemd.preConst = (float)expremd0.postConst / (float)expremd1.postConst;
                    } else {
                        binaryemd.preConst = Utility.divideWithoutOverflow((float)expremd0.postConst, (float)expremd1.postConst);
                    }
                } else if (sort == Sort.DOUBLE) {
                    if (settings.getNumericOverflow()) {
                        binaryemd.preConst = (double)expremd0.postConst / (double)expremd1.postConst;
                    } else {
                        binaryemd.preConst = Utility.divideWithoutOverflow((double)expremd0.postConst, (double)expremd1.postConst);
                    }
                } else {
                    throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
                }
            } else if (ctx.REM() != null) {
                if (sort == Sort.INT) {
                    binaryemd.preConst = (int)expremd0.postConst % (int)expremd1.postConst;
                } else if (sort == Sort.LONG) {
                    binaryemd.preConst = (long)expremd0.postConst % (long)expremd1.postConst;
                } else if (sort == Sort.FLOAT) {
                    if (settings.getNumericOverflow()) {
                        binaryemd.preConst = (float)expremd0.postConst % (float)expremd1.postConst;
                    } else {
                        binaryemd.preConst = Utility.remainderWithoutOverflow((float)expremd0.postConst, (float)expremd1.postConst);
                    }
                } else if (sort == Sort.DOUBLE) {
                    if (settings.getNumericOverflow()) {
                        binaryemd.preConst = (double)expremd0.postConst % (double)expremd1.postConst;
                    } else {
                        binaryemd.preConst = Utility.remainderWithoutOverflow((double)expremd0.postConst, (double)expremd1.postConst);
                    }
                } else {
                    throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
                }
            } else if (ctx.ADD() != null) {
                if (sort == Sort.INT) {
                    if (settings.getNumericOverflow()) {
                        binaryemd.preConst = (int)expremd0.postConst + (int)expremd1.postConst;
                    } else {
                        binaryemd.preConst = Math.addExact((int)expremd0.postConst, (int)expremd1.postConst);
                    }
                } else if (sort == Sort.LONG) {
                    if (settings.getNumericOverflow()) {
                        binaryemd.preConst = (long)expremd0.postConst + (long)expremd1.postConst;
                    } else {
                        binaryemd.preConst = Math.addExact((long)expremd0.postConst, (long)expremd1.postConst);
                    }
                } else if (sort == Sort.FLOAT) {
                    if (settings.getNumericOverflow()) {
                        binaryemd.preConst = (float)expremd0.postConst + (float)expremd1.postConst;
                    } else {
                        binaryemd.preConst = Utility.addWithoutOverflow((float)expremd0.postConst, (float)expremd1.postConst);
                    }
                } else if (sort == Sort.DOUBLE) {
                    if (settings.getNumericOverflow()) {
                        binaryemd.preConst = (double)expremd0.postConst + (double)expremd1.postConst;
                    } else {
                        binaryemd.preConst = Utility.addWithoutOverflow((double)expremd0.postConst, (double)expremd1.postConst);
                    }
                } else if (sort == Sort.STRING) {
                    binaryemd.preConst = "" + expremd0.postConst + expremd1.postConst;
                } else {
                    throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
                }
            } else if (ctx.SUB() != null) {
                if (sort == Sort.INT) {
                    if (settings.getNumericOverflow()) {
                        binaryemd.preConst = (int)expremd0.postConst - (int)expremd1.postConst;
                    } else {
                        binaryemd.preConst = Math.subtractExact((int)expremd0.postConst, (int)expremd1.postConst);
                    }
                } else if (sort == Sort.LONG) {
                    if (settings.getNumericOverflow()) {
                        binaryemd.preConst = (long)expremd0.postConst - (long)expremd1.postConst;
                    } else {
                        binaryemd.preConst = Math.subtractExact((long)expremd0.postConst, (long)expremd1.postConst);
                    }
                } else if (sort == Sort.FLOAT) {
                    if (settings.getNumericOverflow()) {
                        binaryemd.preConst = (float)expremd0.postConst - (float)expremd1.postConst;
                    } else {
                        binaryemd.preConst = Utility.subtractWithoutOverflow((float)expremd0.postConst, (float)expremd1.postConst);
                    }
                } else if (sort == Sort.DOUBLE) {
                    if (settings.getNumericOverflow()) {
                        binaryemd.preConst = (double)expremd0.postConst - (double)expremd1.postConst;
                    } else {
                        binaryemd.preConst = Utility.subtractWithoutOverflow((double)expremd0.postConst, (double)expremd1.postConst);
                    }
                } else {
                    throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
                }
            } else if (ctx.LSH() != null) {
                if (sort == Sort.INT) {
                    binaryemd.preConst = (int)expremd0.postConst << (int)expremd1.postConst;
                } else if (sort == Sort.LONG) {
                    binaryemd.preConst = (long)expremd0.postConst << (long)expremd1.postConst;
                } else {
                    throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
                }
            } else if (ctx.RSH() != null) {
                if (sort == Sort.INT) {
                    binaryemd.preConst = (int)expremd0.postConst >> (int)expremd1.postConst;
                } else if (sort == Sort.LONG) {
                    binaryemd.preConst = (long)expremd0.postConst >> (long)expremd1.postConst;
                } else {
                    throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
                }
            } else if (ctx.USH() != null) {
                if (sort == Sort.INT) {
                    binaryemd.preConst = (int)expremd0.postConst >>> (int)expremd1.postConst;
                } else if (sort == Sort.LONG) {
                    binaryemd.preConst = (long)expremd0.postConst >>> (long)expremd1.postConst;
                } else {
                    throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
                }
            } else if (ctx.BWAND() != null) {
                if (sort == Sort.INT) {
                    binaryemd.preConst = (int)expremd0.postConst & (int)expremd1.postConst;
                } else if (sort == Sort.LONG) {
                    binaryemd.preConst = (long)expremd0.postConst & (long)expremd1.postConst;
                } else {
                    throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
                }
            } else if (ctx.BWXOR() != null) {
                if (sort == Sort.BOOL) {
                    binaryemd.preConst = (boolean)expremd0.postConst ^ (boolean)expremd1.postConst;
                } else if (sort == Sort.INT) {
                    binaryemd.preConst = (int)expremd0.postConst ^ (int)expremd1.postConst;
                } else if (sort == Sort.LONG) {
                    binaryemd.preConst = (long)expremd0.postConst ^ (long)expremd1.postConst;
                } else {
                    throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
                }
            } else if (ctx.BWOR() != null) {
                if (sort == Sort.INT) {
                    binaryemd.preConst = (int)expremd0.postConst | (int)expremd1.postConst;
                } else if (sort == Sort.LONG) {
                    binaryemd.preConst = (long)expremd0.postConst | (long)expremd1.postConst;
                } else {
                    throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
                }
            } else {
                throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
            }
        }

        binaryemd.from = promote;
        binaryemd.typesafe = expremd0.typesafe && expremd1.typesafe;

        return null;
    }

    @Override
    public Void visitComp(final CompContext ctx) {
        final ExpressionMetadata compemd = adapter.getExpressionMetadata(ctx);
        final boolean equality = ctx.EQ() != null || ctx.NE() != null;
        final boolean reference = ctx.EQR() != null || ctx.NER() != null;

        final ExpressionContext exprctx0 = adapter.updateExpressionTree(ctx.expression(0));
        final ExpressionMetadata expremd0 = adapter.createExpressionMetadata(exprctx0);
        visit(exprctx0);

        final ExpressionContext exprctx1 = adapter.updateExpressionTree(ctx.expression(1));
        final ExpressionMetadata expremd1 = adapter.createExpressionMetadata(exprctx1);
        visit(exprctx1);

        if (expremd0.isNull && expremd1.isNull) {
            throw new IllegalArgumentException(error(ctx) + "Unnecessary comparison of null constants.");
        }

        final Type promote = equality ? promoteEquality(expremd0.from, expremd1.from) :
                reference ? promoteReference(expremd0.from, expremd1.from) :
                            promoteNumeric(expremd0.from, expremd1.from, true, true);

        if (promote == null) {
            throw new ClassCastException("Cannot apply [" + ctx.getChild(1).getText() + "] " +
                    "operation to types [" + expremd0.from.name + "] and [" + expremd1.from.name + "].");
        }

        expremd0.to = promote;
        expremd1.to = promote;
        markCast(expremd0);
        markCast(expremd1);

        if (expremd0.postConst != null && expremd1.postConst != null) {
            final Sort sort = promote.sort;

            if (ctx.EQ() != null || ctx.EQR() != null) {
                if (sort == Sort.BOOL) {
                    compemd.preConst = (boolean)expremd0.postConst == (boolean)expremd1.postConst;
                } else if (sort == Sort.INT) {
                    compemd.preConst = (int)expremd0.postConst == (int)expremd1.postConst;
                } else if (sort == Sort.LONG) {
                    compemd.preConst = (long)expremd0.postConst == (long)expremd1.postConst;
                } else if (sort == Sort.FLOAT) {
                    compemd.preConst = (float)expremd0.postConst == (float)expremd1.postConst;
                } else if (sort == Sort.DOUBLE) {
                    compemd.preConst = (double)expremd0.postConst == (double)expremd1.postConst;
                } else {
                    if (ctx.EQ() != null && !expremd0.isNull && !expremd1.isNull) {
                        compemd.preConst = expremd0.postConst.equals(expremd1.postConst);
                    } else if (ctx.EQR() != null) {
                        compemd.preConst = expremd0.postConst == expremd1.postConst;
                    }
                }
            } else if (ctx.NE() != null || ctx.NER() != null) {
                if (sort == Sort.BOOL) {
                    compemd.preConst = (boolean)expremd0.postConst != (boolean)expremd1.postConst;
                } else if (sort == Sort.INT) {
                    compemd.preConst = (int)expremd0.postConst != (int)expremd1.postConst;
                } else if (sort == Sort.LONG) {
                    compemd.preConst = (long)expremd0.postConst != (long)expremd1.postConst;
                } else if (sort == Sort.FLOAT) {
                    compemd.preConst = (float)expremd0.postConst != (float)expremd1.postConst;
                } else if (sort == Sort.DOUBLE) {
                    compemd.preConst = (double)expremd0.postConst != (double)expremd1.postConst;
                } else {
                    if (ctx.NE() != null && !expremd0.isNull && !expremd1.isNull) {
                        compemd.preConst = expremd0.postConst.equals(expremd1.postConst);
                    } else if (ctx.NER() != null) {
                        compemd.preConst = expremd0.postConst == expremd1.postConst;
                    }
                }
            } else if (ctx.GTE() != null) {
                if (sort == Sort.INT) {
                    compemd.preConst = (int)expremd0.postConst >= (int)expremd1.postConst;
                } else if (sort == Sort.LONG) {
                    compemd.preConst = (long)expremd0.postConst >= (long)expremd1.postConst;
                } else if (sort == Sort.FLOAT) {
                    compemd.preConst = (float)expremd0.postConst >= (float)expremd1.postConst;
                } else if (sort == Sort.DOUBLE) {
                    compemd.preConst = (double)expremd0.postConst >= (double)expremd1.postConst;
                }
            } else if (ctx.GT() != null) {
                if (sort == Sort.INT) {
                    compemd.preConst = (int)expremd0.postConst > (int)expremd1.postConst;
                } else if (sort == Sort.LONG) {
                    compemd.preConst = (long)expremd0.postConst > (long)expremd1.postConst;
                } else if (sort == Sort.FLOAT) {
                    compemd.preConst = (float)expremd0.postConst > (float)expremd1.postConst;
                } else if (sort == Sort.DOUBLE) {
                    compemd.preConst = (double)expremd0.postConst > (double)expremd1.postConst;
                }
            } else if (ctx.LTE() != null) {
                if (sort == Sort.INT) {
                    compemd.preConst = (int)expremd0.postConst <= (int)expremd1.postConst;
                } else if (sort == Sort.LONG) {
                    compemd.preConst = (long)expremd0.postConst <= (long)expremd1.postConst;
                } else if (sort == Sort.FLOAT) {
                    compemd.preConst = (float)expremd0.postConst <= (float)expremd1.postConst;
                } else if (sort == Sort.DOUBLE) {
                    compemd.preConst = (double)expremd0.postConst <= (double)expremd1.postConst;
                }
            } else if (ctx.LT() != null) {
                if (sort == Sort.INT) {
                    compemd.preConst = (int)expremd0.postConst < (int)expremd1.postConst;
                } else if (sort == Sort.LONG) {
                    compemd.preConst = (long)expremd0.postConst < (long)expremd1.postConst;
                } else if (sort == Sort.FLOAT) {
                    compemd.preConst = (float)expremd0.postConst < (float)expremd1.postConst;
                } else if (sort == Sort.DOUBLE) {
                    compemd.preConst = (double)expremd0.postConst < (double)expremd1.postConst;
                }
            } else {
                throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
            }
        }

        compemd.from = definition.booleanType;
        compemd.typesafe = expremd0.typesafe && expremd1.typesafe;

        return null;
    }

    @Override
    public Void visitBool(final BoolContext ctx) {
        final ExpressionMetadata boolemd = adapter.getExpressionMetadata(ctx);

        final ExpressionContext exprctx0 = adapter.updateExpressionTree(ctx.expression(0));
        final ExpressionMetadata expremd0 = adapter.createExpressionMetadata(exprctx0);
        expremd0.to = definition.booleanType;
        visit(exprctx0);
        markCast(expremd0);

        final ExpressionContext exprctx1 = adapter.updateExpressionTree(ctx.expression(1));
        final ExpressionMetadata expremd1 = adapter.createExpressionMetadata(exprctx1);
        expremd1.to = definition.booleanType;
        visit(exprctx1);
        markCast(expremd1);

        if (expremd0.postConst != null && expremd1.postConst != null) {
            if (ctx.BOOLAND() != null) {
                boolemd.preConst = (boolean)expremd0.postConst && (boolean)expremd1.postConst;
            } else if (ctx.BOOLOR() != null) {
                boolemd.preConst = (boolean)expremd0.postConst || (boolean)expremd1.postConst;
            } else {
                throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
            }
        }

        boolemd.from = definition.booleanType;
        boolemd.typesafe = expremd0.typesafe && expremd1.typesafe;

        return null;
    }

    @Override
    public Void visitConditional(final ConditionalContext ctx) {
        final ExpressionMetadata condemd = adapter.getExpressionMetadata(ctx);

        final ExpressionContext exprctx0 = adapter.updateExpressionTree(ctx.expression(0));
        final ExpressionMetadata expremd0 = adapter.createExpressionMetadata(exprctx0);
        expremd0.to = definition.booleanType;
        visit(exprctx0);
        markCast(expremd0);

        if (expremd0.postConst != null) {
            throw new IllegalArgumentException(error(ctx) + "Unnecessary conditional statement.");
        }

        final ExpressionContext exprctx1 = adapter.updateExpressionTree(ctx.expression(1));
        final ExpressionMetadata expremd1 = adapter.createExpressionMetadata(exprctx1);
        expremd1.to = condemd.to;
        expremd1.explicit = condemd.explicit;
        visit(exprctx1);

        final ExpressionContext exprctx2 = adapter.updateExpressionTree(ctx.expression(2));
        final ExpressionMetadata expremd2 = adapter.createExpressionMetadata(exprctx2);
        expremd2.to = condemd.to;
        expremd2.explicit = condemd.explicit;
        visit(exprctx2);

        if (condemd.to == null) {
            final Type promote = promoteConditional(expremd1.from, expremd2.from, expremd1.preConst, expremd2.preConst);

            expremd1.to = promote;
            expremd2.to = promote;
            condemd.from = promote;
        } else {
            condemd.from = condemd.to;
        }

        markCast(expremd1);
        markCast(expremd2);

        condemd.typesafe = expremd0.typesafe && expremd1.typesafe;

        return null;
    }

    @Override
    public Void visitAssignment(final AssignmentContext ctx) {
        final ExpressionMetadata assignemd = adapter.getExpressionMetadata(ctx);

        final ExtstartContext extstartctx = ctx.extstart();
        final ExternalMetadata extstartemd = adapter.createExternalMetadata(extstartctx);

        extstartemd.read = assignemd.read;
        extstartemd.storeExpr = adapter.updateExpressionTree(ctx.expression());

        if (ctx.AMUL() != null) {
            extstartemd.token = MUL;
        } else if (ctx.ADIV() != null) {
            extstartemd.token = DIV;
        } else if (ctx.AREM() != null) {
            extstartemd.token = REM;
        } else if (ctx.AADD() != null) {
            extstartemd.token = ADD;
        } else if (ctx.ASUB() != null) {
            extstartemd.token = SUB;
        } else if (ctx.ALSH() != null) {
            extstartemd.token = LSH;
        } else if (ctx.AUSH() != null) {
            extstartemd.token = USH;
        } else if (ctx.ARSH() != null) {
            extstartemd.token = RSH;
        } else if (ctx.AAND() != null) {
            extstartemd.token = BWAND;
        } else if (ctx.AXOR() != null) {
            extstartemd.token = BWXOR;
        } else if (ctx.AOR() != null) {
            extstartemd.token = BWOR;
        }

        visit(extstartctx);

        assignemd.statement = true;
        assignemd.from = extstartemd.read ? extstartemd.current : definition.voidType;
        assignemd.typesafe = extstartemd.current.sort != Sort.DEF;

        return null;
    }

    @Override
    public Void visitExtstart(final ExtstartContext ctx) {
        final ExtprecContext precctx = ctx.extprec();
        final ExtcastContext castctx = ctx.extcast();
        final ExttypeContext typectx = ctx.exttype();
        final ExtvarContext varctx = ctx.extvar();
        final ExtnewContext newctx = ctx.extnew();
        final ExtstringContext stringctx = ctx.extstring();

        if (precctx != null) {
            adapter.createExtNodeMetadata(ctx, precctx);
            visit(precctx);
        } else if (castctx != null) {
            adapter.createExtNodeMetadata(ctx, castctx);
            visit(castctx);
        } else if (typectx != null) {
            adapter.createExtNodeMetadata(ctx, typectx);
            visit(typectx);
        } else if (varctx != null) {
            adapter.createExtNodeMetadata(ctx, varctx);
            visit(varctx);
        } else if (newctx != null) {
            adapter.createExtNodeMetadata(ctx, newctx);
            visit(newctx);
        } else if (stringctx != null) {
            adapter.createExtNodeMetadata(ctx, stringctx);
            visit(stringctx);
        } else {
            throw new IllegalStateException();
        }

        return null;
    }

    @Override
    public Void visitExtprec(final ExtprecContext ctx) {
        final ExtNodeMetadata precenmd = adapter.getExtNodeMetadata(ctx);
        final ParserRuleContext parent = precenmd.parent;
        final ExternalMetadata parentemd = adapter.getExternalMetadata(parent);

        final ExtprecContext precctx = ctx.extprec();
        final ExtcastContext castctx = ctx.extcast();
        final ExttypeContext typectx = ctx.exttype();
        final ExtvarContext varctx = ctx.extvar();
        final ExtnewContext newctx = ctx.extnew();
        final ExtstringContext stringctx = ctx.extstring();

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        if (dotctx != null || bracectx != null) {
            ++parentemd.scope;
        }

        if (precctx != null) {
            adapter.createExtNodeMetadata(parent, precctx);
            visit(precctx);
        } else if (castctx != null) {
            adapter.createExtNodeMetadata(parent, castctx);
            visit(castctx);
        } else if (typectx != null) {
            adapter.createExtNodeMetadata(parent, typectx);
            visit(typectx);
        } else if (varctx != null) {
            adapter.createExtNodeMetadata(parent, varctx);
            visit(varctx);
        } else if (newctx != null) {
            adapter.createExtNodeMetadata(parent, newctx);
            visit(newctx);
        } else if (stringctx != null) {
            adapter.createExtNodeMetadata(ctx, stringctx);
            visit(stringctx);
        } else {
            throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
        }

        parentemd.statement = false;

        if (dotctx != null) {
            --parentemd.scope;

            adapter.createExtNodeMetadata(parent, dotctx);
            visit(dotctx);
        } else if (bracectx != null) {
            --parentemd.scope;

            adapter.createExtNodeMetadata(parent, bracectx);
            visit(bracectx);
        }

        return null;
    }

    @Override
    public Void visitExtcast(final ExtcastContext ctx) {
        final ExtNodeMetadata castenmd = adapter.getExtNodeMetadata(ctx);
        final ParserRuleContext parent = castenmd.parent;
        final ExternalMetadata parentemd = adapter.getExternalMetadata(parent);

        final ExtprecContext precctx = ctx.extprec();
        final ExtcastContext castctx = ctx.extcast();
        final ExttypeContext typectx = ctx.exttype();
        final ExtvarContext varctx = ctx.extvar();
        final ExtnewContext newctx = ctx.extnew();
        final ExtstringContext stringctx = ctx.extstring();

        if (precctx != null) {
            adapter.createExtNodeMetadata(parent, precctx);
            visit(precctx);
        } else if (castctx != null) {
            adapter.createExtNodeMetadata(parent, castctx);
            visit(castctx);
        } else if (typectx != null) {
            adapter.createExtNodeMetadata(parent, typectx);
            visit(typectx);
        } else if (varctx != null) {
            adapter.createExtNodeMetadata(parent, varctx);
            visit(varctx);
        } else if (newctx != null) {
            adapter.createExtNodeMetadata(parent, newctx);
            visit(newctx);
        } else if (stringctx != null) {
            adapter.createExtNodeMetadata(ctx, stringctx);
            visit(stringctx);
        } else {
            throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
        }

        final DecltypeContext declctx = ctx.decltype();
        final ExpressionMetadata declemd = adapter.createExpressionMetadata(declctx);
        visit(declctx);

        castenmd.castTo = getLegalCast(ctx, parentemd.current, declemd.from, true);
        castenmd.type = declemd.from;
        parentemd.current = declemd.from;
        parentemd.statement = false;

        return null;
    }

    @Override
    public Void visitExtbrace(final ExtbraceContext ctx) {
        final ExtNodeMetadata braceenmd = adapter.getExtNodeMetadata(ctx);
        final ParserRuleContext parent = braceenmd.parent;
        final ExternalMetadata parentemd = adapter.getExternalMetadata(parent);

        final boolean array = parentemd.current.sort == Sort.ARRAY;
        final boolean def = parentemd.current.sort == Sort.DEF;
        boolean map = false;
        boolean list = false;

        try {
            parentemd.current.clazz.asSubclass(Map.class);
            map = true;
        } catch (ClassCastException exception) {
            // Do nothing.
        }

        try {
            parentemd.current.clazz.asSubclass(List.class);
            list = true;
        } catch (ClassCastException exception) {
            // Do nothing.
        }

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        braceenmd.last = parentemd.scope == 0 && dotctx == null && bracectx == null;

        final ExpressionContext exprctx = adapter.updateExpressionTree(ctx.expression());
        final ExpressionMetadata expremd = adapter.createExpressionMetadata(exprctx);

        if (array || def) {
            expremd.to = array ? definition.intType : definition.objectType;
            visit(exprctx);
            markCast(expremd);

            braceenmd.target = "#brace";
            braceenmd.type = def ? definition.defType :
                    definition.getType(parentemd.current.struct, parentemd.current.type.getDimensions() - 1);
            analyzeLoadStoreExternal(ctx);
            parentemd.current = braceenmd.type;

            if (dotctx != null) {
                adapter.createExtNodeMetadata(parent, dotctx);
                visit(dotctx);
            } else if (bracectx != null) {
                adapter.createExtNodeMetadata(parent, bracectx);
                visit(bracectx);
            }
        } else {
            final boolean store = braceenmd.last && parentemd.storeExpr != null;
            final boolean get = parentemd.read || parentemd.token > 0 || !braceenmd.last;
            final boolean set = braceenmd.last && store;

            Method getter;
            Method setter;
            Type valuetype;
            Type settype;

            if (map) {
                getter = parentemd.current.struct.methods.get("get");
                setter = parentemd.current.struct.methods.get("put");

                if (getter != null && (getter.rtn.sort == Sort.VOID || getter.arguments.size() != 1)) {
                    throw new IllegalArgumentException(error(ctx) +
                            "Illegal map get shortcut for type [" + parentemd.current.name + "].");
                }

                if (setter != null && setter.arguments.size() != 2) {
                    throw new IllegalArgumentException(error(ctx) +
                            "Illegal map set shortcut for type [" + parentemd.current.name + "].");
                }

                if (getter != null && setter != null && (!getter.arguments.get(0).equals(setter.arguments.get(0))
                        || !getter.rtn.equals(setter.arguments.get(1)))) {
                    throw new IllegalArgumentException(error(ctx) + "Shortcut argument types must match.");
                }

                valuetype = setter != null ? setter.arguments.get(0) : getter != null ? getter.arguments.get(0) : null;
                settype = setter == null ? null : setter.arguments.get(1);
            } else if (list) {
                getter = parentemd.current.struct.methods.get("get");
                setter = parentemd.current.struct.methods.get("add");

                if (getter != null && (getter.rtn.sort == Sort.VOID || getter.arguments.size() != 1 ||
                        getter.arguments.get(0).sort != Sort.INT)) {
                    throw new IllegalArgumentException(error(ctx) +
                            "Illegal list get shortcut for type [" + parentemd.current.name + "].");
                }

                if (setter != null && (setter.arguments.size() != 2 || setter.arguments.get(0).sort != Sort.INT)) {
                    throw new IllegalArgumentException(error(ctx) +
                            "Illegal list set shortcut for type [" + parentemd.current.name + "].");
                }

                if (getter != null && setter != null && (!getter.arguments.get(0).equals(setter.arguments.get(0))
                        || !getter.rtn.equals(setter.arguments.get(1)))) {
                    throw new IllegalArgumentException(error(ctx) + "Shortcut argument types must match.");
                }

                valuetype = definition.intType;
                settype = setter == null ? null : setter.arguments.get(1);
            } else {
                throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
            }

            if ((get || set) && (!get || getter != null) && (!set || setter != null)) {
                expremd.to = valuetype;
                visit(exprctx);
                markCast(expremd);

                braceenmd.target = new Object[] {getter, setter, true, null};
                braceenmd.type = get ? getter.rtn : settype;
                analyzeLoadStoreExternal(ctx);
                parentemd.current = get ? getter.rtn : setter.rtn;
            }
        }

        if (braceenmd.target == null) {
            throw new IllegalArgumentException(error(ctx) +
                    "Attempting to address a non-array type [" + parentemd.current.name + "] as an array.");
        }

        return null;
    }

    @Override
    public Void visitExtdot(final ExtdotContext ctx) {
        final ExtNodeMetadata dotemnd = adapter.getExtNodeMetadata(ctx);
        final ParserRuleContext parent = dotemnd.parent;

        final ExtcallContext callctx = ctx.extcall();
        final ExtfieldContext fieldctx = ctx.extfield();

        if (callctx != null) {
            adapter.createExtNodeMetadata(parent, callctx);
            visit(callctx);
        } else if (fieldctx != null) {
            adapter.createExtNodeMetadata(parent, fieldctx);
            visit(fieldctx);
        }

        return null;
    }

    @Override
    public Void visitExttype(final ExttypeContext ctx) {
        final ExtNodeMetadata typeenmd = adapter.getExtNodeMetadata(ctx);
        final ParserRuleContext parent = typeenmd.parent;
        final ExternalMetadata parentemd = adapter.getExternalMetadata(parent);

        if (parentemd.current != null) {
            throw new IllegalArgumentException(error(ctx) + "Unexpected static type.");
        }

        final String typestr = ctx.TYPE().getText();
        typeenmd.type = definition.getType(typestr);
        parentemd.current = typeenmd.type;
        parentemd.statik = true;

        final ExtdotContext dotctx = ctx.extdot();
        adapter.createExtNodeMetadata(parent, dotctx);
        visit(dotctx);

        return null;
    }

    @Override
    public Void visitExtcall(final ExtcallContext ctx) {
        final ExtNodeMetadata callenmd = adapter.getExtNodeMetadata(ctx);
        final ParserRuleContext parent = callenmd.parent;
        final ExternalMetadata parentemd = adapter.getExternalMetadata(parent);

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        callenmd.last = parentemd.scope == 0 && dotctx == null && bracectx == null;

        final String name = ctx.EXTID().getText();

        if (parentemd.current.sort == Sort.ARRAY) {
            throw new IllegalArgumentException(error(ctx) + "Unexpected call [" + name + "] on an array.");
        } else if (callenmd.last && parentemd.storeExpr != null) {
            throw new IllegalArgumentException(error(ctx) + "Cannot assign a value to a call [" + name + "].");
        }

        final Struct struct = parentemd.current.struct;
        final List<ExpressionContext> arguments = ctx.arguments().expression();
        final int size = arguments.size();
        Type[] types;

        final Method method = parentemd.statik ? struct.functions.get(name) : struct.methods.get(name);
        final boolean def = parentemd.current.sort == Sort.DEF;

        if (method == null && !def) {
            throw new IllegalArgumentException(
                    error(ctx) + "Unknown call [" + name + "] on type [" + struct.name + "].");
        } else if (method != null) {
            types = new Type[method.arguments.size()];
            method.arguments.toArray(types);

            callenmd.target = method;
            callenmd.type = method.rtn;
            parentemd.statement = !parentemd.read && callenmd.last;
            parentemd.current = method.rtn;

            if (size != types.length) {
                throw new IllegalArgumentException(error(ctx) + "When calling [" + name + "] on type " +
                        "[" + struct.name + "] expected [" + types.length + "] arguments," +
                        " but found [" + arguments.size() + "].");
            }
        } else {
            types = new Type[arguments.size()];
            Arrays.fill(types, definition.defType);

            callenmd.target = name;
            callenmd.type = definition.defType;
            parentemd.statement = !parentemd.read && callenmd.last;
            parentemd.current = callenmd.type;
        }

        for (int argument = 0; argument < size; ++argument) {
            final ExpressionContext exprctx = adapter.updateExpressionTree(arguments.get(argument));
            final ExpressionMetadata expremd = adapter.createExpressionMetadata(exprctx);
            expremd.to = types[argument];
            visit(exprctx);
            markCast(expremd);
        }

        parentemd.statik = false;

        if (dotctx != null) {
            adapter.createExtNodeMetadata(parent, dotctx);
            visit(dotctx);
        } else if (bracectx != null) {
            adapter.createExtNodeMetadata(parent, bracectx);
            visit(bracectx);
        }

        return null;
    }

    @Override
    public Void visitExtvar(final ExtvarContext ctx) {
        final ExtNodeMetadata varenmd = adapter.getExtNodeMetadata(ctx);
        final ParserRuleContext parent = varenmd.parent;
        final ExternalMetadata parentemd = adapter.getExternalMetadata(parent);

        final String name = ctx.ID().getText();

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        if (parentemd.current != null) {
            throw new IllegalStateException(error(ctx) + "Unexpected variable [" + name + "] load.");
        }

        varenmd.last = parentemd.scope == 0 && dotctx == null && bracectx == null;

        final Variable variable = getVariable(name);

        if (variable == null) {
            throw new IllegalArgumentException(error(ctx) + "Unknown variable [" + name + "].");
        }

        varenmd.target = variable.slot;
        varenmd.type = variable.type;
        analyzeLoadStoreExternal(ctx);
        parentemd.current = varenmd.type;

        if (dotctx != null) {
            adapter.createExtNodeMetadata(parent, dotctx);
            visit(dotctx);
        } else if (bracectx != null) {
            adapter.createExtNodeMetadata(parent, bracectx);
            visit(bracectx);
        }

        return null;
    }

    @Override
    public Void visitExtfield(final ExtfieldContext ctx) {
        final ExtNodeMetadata memberenmd = adapter.getExtNodeMetadata(ctx);
        final ParserRuleContext parent = memberenmd.parent;
        final ExternalMetadata parentemd = adapter.getExternalMetadata(parent);

        if (ctx.EXTID() == null && ctx.EXTINTEGER() == null) {
            throw new IllegalArgumentException(error(ctx) + "Unexpected parser state.");
        }

        final String value = ctx.EXTID() == null ? ctx.EXTINTEGER().getText() : ctx.EXTID().getText();

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        memberenmd.last = parentemd.scope == 0 && dotctx == null && bracectx == null;
        final boolean store = memberenmd.last && parentemd.storeExpr != null;

        if (parentemd.current == null) {
            throw new IllegalStateException(error(ctx) + "Unexpected field [" + value + "] load.");
        }

        if (parentemd.current.sort == Sort.ARRAY) {
            if ("length".equals(value)) {
                if (!parentemd.read) {
                    throw new IllegalArgumentException(error(ctx) + "Must read array field [length].");
                } else if (store) {
                    throw new IllegalArgumentException(
                            error(ctx) + "Cannot write to read-only array field [length].");
                }

                memberenmd.target = "#length";
                memberenmd.type = definition.intType;
                parentemd.current = definition.intType;
            } else {
                throw new IllegalArgumentException(error(ctx) + "Unexpected array field [" + value + "].");
            }
        } else if (parentemd.current.sort == Sort.DEF) {
            memberenmd.target = value;
            memberenmd.type = definition.defType;
            analyzeLoadStoreExternal(ctx);
            parentemd.current = memberenmd.type;
        } else {
            final Struct struct = parentemd.current.struct;
            final Field field = parentemd.statik ? struct.statics.get(value) : struct.members.get(value);

            if (field != null) {
                if (store && java.lang.reflect.Modifier.isFinal(field.reflect.getModifiers())) {
                    throw new IllegalArgumentException(error(ctx) + "Cannot write to read-only" +
                            " field [" + value + "] for type [" + struct.name + "].");
                }

                memberenmd.target = field;
                memberenmd.type = field.type;
                analyzeLoadStoreExternal(ctx);
                parentemd.current = memberenmd.type;
            } else {
                final boolean get = parentemd.read || parentemd.token > 0 || !memberenmd.last;
                final boolean set = memberenmd.last && store;

                Method getter = struct.methods.get("get" + Character.toUpperCase(value.charAt(0)) + value.substring(1));
                Method setter = struct.methods.get("set" + Character.toUpperCase(value.charAt(0)) + value.substring(1));
                Object constant = null;

                if (getter != null && (getter.rtn.sort == Sort.VOID || !getter.arguments.isEmpty())) {
                    throw new IllegalArgumentException(error(ctx) +
                            "Illegal get shortcut on field [" + value + "] for type [" + struct.name + "].");
                }

                if (setter != null && (setter.rtn.sort != Sort.VOID || setter.arguments.size() != 1)) {
                    throw new IllegalArgumentException(error(ctx) +
                            "Illegal set shortcut on field [" + value + "] for type [" + struct.name + "].");
                }

                Type settype = setter == null ? null : setter.arguments.get(0);

                if (getter == null && setter == null) {
                    if (ctx.EXTID() != null) {
                        try {
                            parentemd.current.clazz.asSubclass(Map.class);

                            getter = parentemd.current.struct.methods.get("get");
                            setter = parentemd.current.struct.methods.get("put");

                            if (getter != null && (getter.rtn.sort == Sort.VOID || getter.arguments.size() != 1 ||
                                getter.arguments.get(0).sort != Sort.STRING)) {
                                throw new IllegalArgumentException(error(ctx) +
                                        "Illegal map get shortcut [" + value + "] for type [" + struct.name + "].");
                            }

                            if (setter != null && (setter.arguments.size() != 2 ||
                                    setter.arguments.get(0).sort != Sort.STRING)) {
                                throw new IllegalArgumentException(error(ctx) +
                                        "Illegal map set shortcut [" + value + "] for type [" + struct.name + "].");
                            }

                            if (getter != null && setter != null && !getter.rtn.equals(setter.arguments.get(1))) {
                                throw new IllegalArgumentException(error(ctx) + "Shortcut argument types must match.");
                            }

                            settype = setter == null ? null : setter.arguments.get(1);
                            constant = value;
                        } catch (ClassCastException exception) {
                            //Do nothing.
                        }
                    } else if (ctx.EXTINTEGER() != null) {
                        try {
                            parentemd.current.clazz.asSubclass(List.class);

                            getter = parentemd.current.struct.methods.get("get");
                            setter = parentemd.current.struct.methods.get("add");

                            if (getter != null && (getter.rtn.sort == Sort.VOID || getter.arguments.size() != 1 ||
                                    getter.arguments.get(0).sort != Sort.INT)) {
                                throw new IllegalArgumentException(error(ctx) +
                                        "Illegal list get shortcut [" + value + "] for type [" + struct.name + "].");
                            }

                            if (setter != null && (setter.rtn.sort != Sort.VOID || setter.arguments.size() != 2 ||
                                    setter.arguments.get(0).sort != Sort.INT)) {
                                throw new IllegalArgumentException(error(ctx) +
                                        "Illegal list add shortcut [" + value + "] for type [" + struct.name + "].");
                            }

                            if (getter != null && setter != null && !getter.rtn.equals(setter.arguments.get(1))) {
                                throw new IllegalArgumentException(error(ctx) + "Shortcut argument types must match.");
                            }

                            settype = setter == null ? null : setter.arguments.get(1);

                            try {
                                constant = Integer.parseInt(value);
                            } catch (NumberFormatException exception) {
                                throw new IllegalArgumentException(error(ctx) +
                                        "Illegal list shortcut value [" + value + "].");
                            }
                        } catch (ClassCastException exception) {
                            //Do nothing.
                        }
                    } else {
                        throw new IllegalStateException(error(ctx) + "Unexpected parser state.");
                    }
                }

                if ((get || set) && (!get || getter != null) && (!set || setter != null)) {
                    memberenmd.target = new Object[] {getter, setter, constant != null, constant};
                    memberenmd.type = get ? getter.rtn : settype;
                    analyzeLoadStoreExternal(ctx);
                    parentemd.current = get ? getter.rtn : setter.rtn;
                }
            }

            if (memberenmd.target == null) {
                throw new IllegalArgumentException(
                        error(ctx) + "Unknown field [" + value + "] for type [" + struct.name + "].");
            }
        }

        parentemd.statik = false;

        if (dotctx != null) {
            adapter.createExtNodeMetadata(parent, dotctx);
            visit(dotctx);
        } else if (bracectx != null) {
            adapter.createExtNodeMetadata(parent, bracectx);
            visit(bracectx);
        }

        return null;
    }

    @Override
    public Void visitExtnew(ExtnewContext ctx) {
        final ExtNodeMetadata newenmd = adapter.getExtNodeMetadata(ctx);
        final ParserRuleContext parent = newenmd.parent;
        final ExternalMetadata parentemd = adapter.getExternalMetadata(parent);

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        newenmd.last = parentemd.scope == 0 && dotctx == null && bracectx == null;

        final String name = ctx.TYPE().getText();
        final Struct struct = definition.structs.get(name);

        if (parentemd.current != null) {
            throw new IllegalArgumentException(error(ctx) + "Unexpected new call.");
        } else if (struct == null) {
            throw new IllegalArgumentException(error(ctx) + "Specified type [" + name + "] not found.");
        } else if (newenmd.last && parentemd.storeExpr != null) {
            throw new IllegalArgumentException(error(ctx) + "Cannot assign a value to a new call.");
        }

        final boolean newclass = ctx.arguments() != null;
        final boolean newarray = !ctx.expression().isEmpty();

        final List<ExpressionContext> arguments = newclass ? ctx.arguments().expression() : ctx.expression();
        final int size = arguments.size();

        Type[] types;

        if (newarray) {
            if (!parentemd.read) {
                throw new IllegalArgumentException(error(ctx) + "A newly created array must be assigned.");
            }

            types = new Type[size];
            Arrays.fill(types, definition.intType);

            newenmd.target = "#makearray";

            if (size > 1) {
                newenmd.type = definition.getType(struct, size);
                parentemd.current = newenmd.type;
            } else if (size == 1) {
                newenmd.type = definition.getType(struct, 0);
                parentemd.current = definition.getType(struct, 1);
            } else {
                throw new IllegalArgumentException(error(ctx) + "A newly created array cannot have zero dimensions.");
            }
        } else if (newclass) {
            final Constructor constructor = struct.constructors.get("new");

            if (constructor != null) {
                types = new Type[constructor.arguments.size()];
                constructor.arguments.toArray(types);

                newenmd.target = constructor;
                newenmd.type = definition.getType(struct, 0);
                parentemd.statement = !parentemd.read && newenmd.last;
                parentemd.current = newenmd.type;
            } else {
                throw new IllegalArgumentException(
                        error(ctx) + "Unknown new call on type [" + struct.name + "].");
            }
        } else {
            throw new IllegalArgumentException(error(ctx) + "Unknown parser state.");
        }

        if (size != types.length) {
            throw new IllegalArgumentException(error(ctx) + "When calling [" + name + "] on type " +
                    "[" + struct.name + "] expected [" + types.length + "] arguments," +
                    " but found [" + arguments.size() + "].");
        }

        for (int argument = 0; argument < size; ++argument) {
            final ExpressionContext exprctx = adapter.updateExpressionTree(arguments.get(argument));
            final ExpressionMetadata expremd = adapter.createExpressionMetadata(exprctx);
            expremd.to = types[argument];
            visit(exprctx);
            markCast(expremd);
        }

        if (dotctx != null) {
            adapter.createExtNodeMetadata(parent, dotctx);
            visit(dotctx);
        } else if (bracectx != null) {
            adapter.createExtNodeMetadata(parent, bracectx);
            visit(bracectx);
        }

        return null;
    }

    @Override
    public Void visitExtstring(final ExtstringContext ctx) {
        final ExtNodeMetadata memberenmd = adapter.getExtNodeMetadata(ctx);
        final ParserRuleContext parent = memberenmd.parent;
        final ExternalMetadata parentemd = adapter.getExternalMetadata(parent);

        final String string = ctx.STRING().getText();

        final ExtdotContext dotctx = ctx.extdot();
        final ExtbraceContext bracectx = ctx.extbrace();

        memberenmd.last = parentemd.scope == 0 && dotctx == null && bracectx == null;
        final boolean store = memberenmd.last && parentemd.storeExpr != null;

        if (parentemd.current != null) {
            throw new IllegalStateException(error(ctx) + "Unexpected String constant [" + string + "].");
        }

        if (!parentemd.read) {
            throw new IllegalArgumentException(error(ctx) + "Must read String constant [" + string + "].");
        } else if (store) {
            throw new IllegalArgumentException(
                    error(ctx) + "Cannot write to read-only String constant [" + string + "].");
        }

        memberenmd.target = string;
        memberenmd.type = definition.stringType;
        parentemd.current = definition.stringType;

        if (memberenmd.last) {
            parentemd.constant = string;
        }

        if (dotctx != null) {
            adapter.createExtNodeMetadata(parent, dotctx);
            visit(dotctx);
        } else if (bracectx != null) {
            adapter.createExtNodeMetadata(parent, bracectx);
            visit(bracectx);
        }

        return null;
    }

    @Override
    public Void visitArguments(final ArgumentsContext ctx) {
        throw new UnsupportedOperationException(error(ctx) + "Unexpected parser state.");
    }

    @Override
    public Void visitIncrement(IncrementContext ctx) {
        final ExpressionMetadata incremd = adapter.getExpressionMetadata(ctx);
        final Sort sort = incremd.to == null ? null : incremd.to.sort;
        final boolean positive = ctx.INCR() != null;

        if (incremd.to == null) {
            incremd.preConst = positive ? 1 : -1;
            incremd.from = definition.intType;
        } else {
            switch (sort) {
                case LONG:
                    incremd.preConst = positive ? 1L : -1L;
                    incremd.from = definition.longType;
                case FLOAT:
                    incremd.preConst = positive ? 1.0F : -1.0F;
                    incremd.from = definition.floatType;
                case DOUBLE:
                    incremd.preConst = positive ? 1.0 : -1.0;
                    incremd.from = definition.doubleType;
                default:
                    incremd.preConst = positive ? 1 : -1;
                    incremd.from = definition.intType;
            }
        }

        return null;
    }

    private void analyzeLoadStoreExternal(final ParserRuleContext source) {
        final ExtNodeMetadata extenmd = adapter.getExtNodeMetadata(source);
        final ParserRuleContext parent = extenmd.parent;
        final ExternalMetadata parentemd = adapter.getExternalMetadata(parent);

        if (extenmd.last && parentemd.storeExpr != null) {
            final ParserRuleContext store = parentemd.storeExpr;
            final ExpressionMetadata storeemd = adapter.createExpressionMetadata(parentemd.storeExpr);
            final int token = parentemd.token;

            if (token > 0) {
                visit(store);

                final boolean add = token == ADD;
                final boolean xor = token == BWAND || token == BWXOR || token == BWOR;
                final boolean decimal = token == MUL || token == DIV || token == REM || token == SUB;

                extenmd.promote = add ? promoteAdd(extenmd.type, storeemd.from) :
                                  xor ? promoteXor(extenmd.type, storeemd.from) :
                                        promoteNumeric(extenmd.type, storeemd.from, decimal, true);

                if (extenmd.promote == null) {
                    throw new IllegalArgumentException("Cannot apply compound assignment to " +
                            " types [" + extenmd.type.name + "] and [" + storeemd.from.name + "].");
                }

                extenmd.castFrom = getLegalCast(source, extenmd.type, extenmd.promote, false);
                extenmd.castTo = getLegalCast(source, extenmd.promote, extenmd.type, true);

                storeemd.to = add && extenmd.promote.sort == Sort.STRING ? storeemd.from : extenmd.promote;
                markCast(storeemd);
            } else {
                storeemd.to = extenmd.type;
                visit(store);
                markCast(storeemd);
            }
        }
    }

    private void markCast(final ExpressionMetadata emd) {
        if (emd.from == null) {
            throw new IllegalStateException(error(emd.source) + "From cast type should never be null.");
        }

        if (emd.to != null) {
            emd.cast = getLegalCast(emd.source, emd.from, emd.to, emd.explicit || !emd.typesafe);

            if (emd.preConst != null && emd.to.sort.constant) {
                emd.postConst = constCast(emd.source, emd.preConst, emd.cast);
            }
        } else {
            throw new IllegalStateException(error(emd.source) + "To cast type should never be null.");
        }
    }

    private Cast getLegalCast(final ParserRuleContext source, final Type from, final Type to, final boolean explicit) {
        final Cast cast = new Cast(from, to);

        if (from.equals(to)) {
            return cast;
        }

        if (from.sort == Sort.DEF && to.sort != Sort.VOID || from.sort != Sort.VOID && to.sort == Sort.DEF) {
            final Transform transform = definition.transforms.get(cast);

            if (transform != null) {
                return transform;
            }

            return cast;
        }

        switch (from.sort) {
            case BOOL:
                switch (to.sort) {
                    case OBJECT:
                    case BOOL_OBJ:
                        return checkTransform(source, cast);
                }

                break;
            case BYTE:
                switch (to.sort) {
                    case SHORT:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        return cast;
                    case CHAR:
                        if (explicit)
                            return cast;

                        break;
                    case OBJECT:
                    case NUMBER:
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case SHORT:
                switch (to.sort) {
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        return cast;
                    case BYTE:
                    case CHAR:
                        if (explicit)
                            return cast;

                        break;
                    case OBJECT:
                    case NUMBER:
                    case SHORT_OBJ:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE_OBJ:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case CHAR:
                switch (to.sort) {
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        return cast;
                    case BYTE:
                    case SHORT:
                        if (explicit)
                            return cast;

                        break;
                    case OBJECT:
                    case NUMBER:
                    case CHAR_OBJ:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case INT:
                switch (to.sort) {
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        return cast;
                    case BYTE:
                    case SHORT:
                    case CHAR:
                        if (explicit)
                            return cast;

                        break;
                    case OBJECT:
                    case NUMBER:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case LONG:
                switch (to.sort) {
                    case FLOAT:
                    case DOUBLE:
                        return cast;
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                        if (explicit)
                            return cast;

                        break;
                    case OBJECT:
                    case NUMBER:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                    case INT_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case FLOAT:
                switch (to.sort) {
                    case DOUBLE:
                        return cast;
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case LONG:
                        if (explicit)
                            return cast;

                        break;
                    case OBJECT:
                    case NUMBER:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                    case INT_OBJ:
                    case LONG_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case DOUBLE:
                switch (to.sort) {
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case LONG:
                    case FLOAT:
                        if (explicit)
                            return cast;

                        break;
                    case OBJECT:
                    case NUMBER:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case OBJECT:
            case NUMBER:
                switch (to.sort) {
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case BOOL_OBJ:
                switch (to.sort) {
                    case BOOL:
                        return checkTransform(source, cast);
                }

                break;
            case BYTE_OBJ:
                switch (to.sort) {
                    case BYTE:
                    case SHORT:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case SHORT_OBJ:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case CHAR:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case SHORT_OBJ:
                switch (to.sort) {
                    case SHORT:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE:
                    case CHAR:
                    case BYTE_OBJ:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case CHAR_OBJ:
                switch (to.sort) {
                    case CHAR:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE:
                    case SHORT:
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case INT_OBJ:
                switch (to.sort) {
                    case INT:
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case LONG_OBJ:
                switch (to.sort) {
                    case LONG:
                    case FLOAT:
                    case DOUBLE:
                    case FLOAT_OBJ:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                    case INT_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case FLOAT_OBJ:
                switch (to.sort) {
                    case FLOAT:
                    case DOUBLE:
                    case DOUBLE_OBJ:
                        return checkTransform(source, cast);
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case LONG:
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                    case INT_OBJ:
                    case LONG_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
            case DOUBLE_OBJ:
                switch (to.sort) {
                    case DOUBLE:
                        return checkTransform(source, cast);
                    case BYTE:
                    case SHORT:
                    case CHAR:
                    case INT:
                    case LONG:
                    case FLOAT:
                    case BYTE_OBJ:
                    case SHORT_OBJ:
                    case CHAR_OBJ:
                    case INT_OBJ:
                    case LONG_OBJ:
                    case FLOAT_OBJ:
                        if (explicit)
                            return checkTransform(source, cast);

                        break;
                }

                break;
        }

        try {
            from.clazz.asSubclass(to.clazz);

            return cast;
        } catch (ClassCastException cce0) {
            try {
                if (explicit) {
                    to.clazz.asSubclass(from.clazz);

                    return cast;
                } else {
                    throw new ClassCastException(
                            error(source) + "Cannot cast from [" + from.name + "] to [" + to.name + "].");
                }
            } catch (ClassCastException cce1) {
                throw new ClassCastException(
                        error(source) + "Cannot cast from [" + from.name + "] to [" + to.name + "].");
            }
        }
    }

    private Transform checkTransform(final ParserRuleContext source, final Cast cast) {
        final Transform transform = definition.transforms.get(cast);

        if (transform == null) {
            throw new ClassCastException(
                    error(source) + "Cannot cast from [" + cast.from.name + "] to [" + cast.to.name + "].");
        }

        return transform;
    }

    private Object constCast(final ParserRuleContext source, final Object constant, final Cast cast) {
        if (cast instanceof Transform) {
            final Transform transform = (Transform)cast;
            return invokeTransform(source, transform, constant);
        } else {
            final Sort fsort = cast.from.sort;
            final Sort tsort = cast.to.sort;

            if (fsort == tsort) {
                return constant;
            } else if (fsort.numeric && tsort.numeric) {
                Number number;

                if (fsort == Sort.CHAR) {
                    number = (int)(char)constant;
                } else {
                    number = (Number)constant;
                }

                switch (tsort) {
                    case BYTE:   return number.byteValue();
                    case SHORT:  return number.shortValue();
                    case CHAR:   return (char)number.intValue();
                    case INT:    return number.intValue();
                    case LONG:   return number.longValue();
                    case FLOAT:  return number.floatValue();
                    case DOUBLE: return number.doubleValue();
                    default:
                        throw new IllegalStateException(error(source) + "Expected numeric type for cast.");
                }
            } else {
                throw new IllegalStateException(error(source) + "No valid constant cast from " +
                        "[" + cast.from.clazz.getCanonicalName() + "] to " +
                        "[" + cast.to.clazz.getCanonicalName() + "].");
            }
        }
    }

    private Object invokeTransform(final ParserRuleContext source, final Transform transform, final Object object) {
        final Method method = transform.method;
        final java.lang.reflect.Method jmethod = method.reflect;
        final int modifiers = jmethod.getModifiers();

        try {
            if (java.lang.reflect.Modifier.isStatic(modifiers)) {
                return jmethod.invoke(null, object);
            } else {
                return jmethod.invoke(object);
            }
        } catch (IllegalAccessException | IllegalArgumentException |
                java.lang.reflect.InvocationTargetException | NullPointerException |
                ExceptionInInitializerError exception) {
            throw new IllegalStateException(error(source) + "Unable to invoke transform to cast constant from " +
                    "[" + transform.from.name + "] to [" + transform.to.name + "].");
        }
    }

    private Type promoteNumeric(final Type from, boolean decimal, boolean primitive) {
        final Sort sort = from.sort;

        if (sort == Sort.DEF) {
            return definition.defType;
        } else if ((sort == Sort.DOUBLE || sort == Sort.DOUBLE_OBJ || sort == Sort.NUMBER) && decimal) {
             return primitive ? definition.doubleType : definition.doubleobjType;
        } else if ((sort == Sort.FLOAT || sort == Sort.FLOAT_OBJ) && decimal) {
            return primitive ? definition.floatType : definition.floatobjType;
        } else if (sort == Sort.LONG || sort == Sort.LONG_OBJ || sort == Sort.NUMBER) {
            return primitive ? definition.longType : definition.longobjType;
        } else if (sort.numeric) {
            return primitive ? definition.intType : definition.intobjType;
        }

        return null;
    }

    private Type promoteNumeric(final Type from0, final Type from1, boolean decimal, boolean primitive) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return definition.defType;
        }

        if (decimal) {
            if (sort0 == Sort.DOUBLE || sort0 == Sort.DOUBLE_OBJ || sort0 == Sort.NUMBER ||
                    sort1 == Sort.DOUBLE || sort1 == Sort.DOUBLE_OBJ || sort1 == Sort.NUMBER) {
                return primitive ? definition.doubleType : definition.doubleobjType;
            } else if (sort0 == Sort.FLOAT || sort0 == Sort.FLOAT_OBJ || sort1 == Sort.FLOAT || sort1 == Sort.FLOAT_OBJ) {
                return primitive ? definition.floatType : definition.floatobjType;
            }
        }

        if (sort0 == Sort.LONG || sort0 == Sort.LONG_OBJ || sort0 == Sort.NUMBER ||
                sort1 == Sort.LONG || sort1 == Sort.LONG_OBJ || sort1 == Sort.NUMBER) {
             return primitive ? definition.longType : definition.longobjType;
        } else if (sort0.numeric && sort1.numeric) {
            return primitive ? definition.intType : definition.intobjType;
        }

        return null;
    }

    private Type promoteAdd(final Type from0, final Type from1) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.STRING || sort1 == Sort.STRING) {
            return definition.stringType;
        }

        return promoteNumeric(from0, from1, true, true);
    }

    private Type promoteXor(final Type from0, final Type from1) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0.bool || sort1.bool) {
            return definition.booleanType;
        }

        return promoteNumeric(from0, from1, false, true);
    }

    private Type promoteEquality(final Type from0, final Type from1) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return definition.defType;
        }

        final boolean primitive = sort0.primitive && sort1.primitive;

        if (sort0.bool && sort1.bool) {
            return primitive ? definition.booleanType : definition.byteobjType;
        }

        if (sort0.numeric && sort1.numeric) {
            return promoteNumeric(from0, from1, true, primitive);
        }

        return definition.objectType;
    }

    private Type promoteReference(final Type from0, final Type from1) {
        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return definition.defType;
        }

        if (sort0.primitive && sort1.primitive) {
            if (sort0.bool && sort1.bool) {
                return definition.booleanType;
            }

            if (sort0.numeric && sort1.numeric) {
                return promoteNumeric(from0, from1, true, true);
            }
        }

        return definition.objectType;
    }

    private Type promoteConditional(final Type from0, final Type from1, final Object const0, final Object const1) {
        if (from0.equals(from1)) {
            return from0;
        }

        final Sort sort0 = from0.sort;
        final Sort sort1 = from1.sort;

        if (sort0 == Sort.DEF || sort1 == Sort.DEF) {
            return definition.defType;
        }

        final boolean primitive = sort0.primitive && sort1.primitive;

        if (sort0.bool && sort1.bool) {
            return primitive ? definition.booleanType : definition.booleanobjType;
        }

        if (sort0.numeric && sort1.numeric) {
            if (sort0 == Sort.DOUBLE || sort0 == Sort.DOUBLE_OBJ || sort1 == Sort.DOUBLE || sort1 == Sort.DOUBLE_OBJ) {
                return primitive ? definition.doubleType : definition.doubleobjType;
            } else if (sort0 == Sort.FLOAT || sort0 == Sort.FLOAT_OBJ || sort1 == Sort.FLOAT || sort1 == Sort.FLOAT_OBJ) {
                return primitive ? definition.floatType : definition.floatobjType;
            } else if (sort0 == Sort.LONG || sort0 == Sort.LONG_OBJ || sort1 == Sort.LONG || sort1 == Sort.LONG_OBJ) {
                return sort0.primitive && sort1.primitive ? definition.longType : definition.longobjType;
            } else {
                if (sort0 == Sort.BYTE || sort0 == Sort.BYTE_OBJ) {
                    if (sort1 == Sort.BYTE || sort1 == Sort.BYTE_OBJ) {
                        return primitive ? definition.byteType : definition.byteobjType;
                    } else if (sort1 == Sort.SHORT || sort1 == Sort.SHORT_OBJ) {
                        if (const1 != null) {
                            final short constant = (short)const1;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return primitive ? definition.byteType : definition.byteobjType;
                            }
                        }

                        return primitive ? definition.shortType : definition.shortobjType;
                    } else if (sort1 == Sort.CHAR || sort1 == Sort.CHAR_OBJ) {
                        return primitive ? definition.intType : definition.intobjType;
                    } else if (sort1 == Sort.INT || sort1 == Sort.INT_OBJ) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return primitive ? definition.byteType : definition.byteobjType;
                            }
                        }

                        return primitive ? definition.intType : definition.intobjType;
                    }
                } else if (sort0 == Sort.SHORT || sort0 == Sort.SHORT_OBJ) {
                    if (sort1 == Sort.BYTE || sort1 == Sort.BYTE_OBJ) {
                        if (const0 != null) {
                            final short constant = (short)const0;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return primitive ? definition.byteType : definition.byteobjType;
                            }
                        }

                        return primitive ? definition.shortType : definition.shortobjType;
                    } else if (sort1 == Sort.SHORT || sort1 == Sort.SHORT_OBJ) {
                        return primitive ? definition.shortType : definition.shortobjType;
                    } else if (sort1 == Sort.CHAR || sort1 == Sort.CHAR_OBJ) {
                        return primitive ? definition.intType : definition.intobjType;
                    } else if (sort1 == Sort.INT || sort1 == Sort.INT_OBJ) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Short.MAX_VALUE && constant >= Short.MIN_VALUE) {
                                return primitive ? definition.shortType : definition.shortobjType;
                            }
                        }

                        return primitive ? definition.intType : definition.intobjType;
                    }
                } else if (sort0 == Sort.CHAR || sort0 == Sort.CHAR_OBJ) {
                    if (sort1 == Sort.BYTE || sort1 == Sort.BYTE_OBJ) {
                        return primitive ? definition.intType : definition.intobjType;
                    } else if (sort1 == Sort.SHORT || sort1 == Sort.SHORT_OBJ) {
                        return primitive ? definition.intType : definition.intobjType;
                    } else if (sort1 == Sort.CHAR || sort1 == Sort.CHAR_OBJ) {
                        return primitive ? definition.charType : definition.charobjType;
                    } else if (sort1 == Sort.INT || sort1 == Sort.INT_OBJ) {
                        if (const1 != null) {
                            final int constant = (int)const1;

                            if (constant <= Character.MAX_VALUE && constant >= Character.MIN_VALUE) {
                                return primitive ? definition.byteType : definition.byteobjType;
                            }
                        }

                        return primitive ? definition.intType : definition.intobjType;
                    }
                } else if (sort0 == Sort.INT || sort0 == Sort.INT_OBJ) {
                    if (sort1 == Sort.BYTE || sort1 == Sort.BYTE_OBJ) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Byte.MAX_VALUE && constant >= Byte.MIN_VALUE) {
                                return primitive ? definition.byteType : definition.byteobjType;
                            }
                        }

                        return primitive ? definition.intType : definition.intobjType;
                    } else if (sort1 == Sort.SHORT || sort1 == Sort.SHORT_OBJ) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Short.MAX_VALUE && constant >= Short.MIN_VALUE) {
                                return primitive ? definition.byteType : definition.byteobjType;
                            }
                        }

                        return primitive ? definition.intType : definition.intobjType;
                    } else if (sort1 == Sort.CHAR || sort1 == Sort.CHAR_OBJ) {
                        if (const0 != null) {
                            final int constant = (int)const0;

                            if (constant <= Character.MAX_VALUE && constant >= Character.MIN_VALUE) {
                                return primitive ? definition.byteType : definition.byteobjType;
                            }
                        }

                        return primitive ? definition.intType : definition.intobjType;
                    } else if (sort1 == Sort.INT || sort1 == Sort.INT_OBJ) {
                        return primitive ? definition.intType : definition.intobjType;
                    }
                }
            }
        }

        final Pair pair = new Pair(from0, from1);
        final Type bound = definition.bounds.get(pair);

        return bound == null ? definition.objectType : bound;
    }
}
