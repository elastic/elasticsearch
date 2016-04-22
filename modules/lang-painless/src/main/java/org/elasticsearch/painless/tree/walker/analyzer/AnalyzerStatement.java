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

package org.elasticsearch.painless.tree.walker.analyzer;

import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.tree.node.Node;
import org.elasticsearch.painless.tree.utility.Variables;

import static org.elasticsearch.painless.tree.node.Type.ACONSTANT;
import static org.elasticsearch.painless.tree.node.Type.DECLARATION;
import static org.elasticsearch.painless.tree.node.Type.EXPRESSION;

class AnalyzerStatement {
    private final Definition definition;
    private final Variables variables;
    private final Analyzer analyzer;
    private final AnalyzerCaster caster;

    AnalyzerStatement(final Definition definition, final Variables variables, final Analyzer analyzer, final AnalyzerCaster caster) {
        this.definition = definition;
        this.variables = variables;
        this.analyzer = analyzer;
        this.caster = caster;
    }

    void visitIf(final Node ifelse, final MetadataStatement ifelsems) {
        final Node condition = ifelse.children.get(0);
        final MetadataExpression conditionme = new MetadataExpression();

        conditionme.expected = definition.booleanType;
        analyzer.visit(condition, conditionme);
        final Node cast = caster.markCast(condition, conditionme);
        ifelse.children.set(0, cast);

        if (cast.type == ACONSTANT) {
            throw new IllegalArgumentException(ifelse.error("Extraneous if statement."));
        }

        final Node ifblock = ifelse.children.get(1);
        final MetadataStatement ifblockms = new MetadataStatement();

        ifblockms.lastSource = ifelsems.lastSource;
        ifblockms.inLoop = ifelsems.inLoop;
        ifblockms.lastLoop = ifelsems.lastLoop;

        variables.incrementScope();
        analyzer.visit(ifblock, ifblockms);
        variables.decrementScope();

        ifblockms.anyContinue = ifelsems.anyContinue;
        ifblockms.anyBreak = ifelsems.anyBreak;
        ifblockms.statementCount = ifelsems.statementCount;

        final Node elseblock = ifelse.children.get(2);

        if (elseblock != null) {
            final MetadataStatement elseblockms = new MetadataStatement();

            elseblockms.lastSource = ifelsems.lastSource;
            elseblockms.inLoop = ifelsems.inLoop;
            elseblockms.lastLoop = ifelsems.lastLoop;

            variables.incrementScope();
            analyzer.visit(elseblock, elseblockms);
            variables.decrementScope();

            ifblockms.methodEscape = ifblockms.methodEscape && elseblockms.methodEscape;
            ifblockms.loopEscape = ifblockms.loopEscape && elseblockms.loopEscape;
            ifblockms.allEscape = ifblockms.allEscape && elseblockms.allEscape;
            ifblockms.anyContinue |= elseblockms.anyContinue;
            ifblockms.anyBreak |= elseblockms.anyBreak;
            ifblockms.statementCount = Math.max(ifblockms.statementCount, elseblockms.statementCount);
        }
    }

    void visitWhile(final Node whil, final MetadataStatement whilems) {
        variables.incrementScope();

        final Node condition = whil.children.get(0);
        final MetadataExpression conditionme = new MetadataExpression();

        conditionme.expected = definition.booleanType;
        analyzer.visit(condition, conditionme);
        final Node cast = caster.markCast(condition, conditionme);
        whil.children.set(0, cast);

        final Node whilblock = whil.children.get(1);

        boolean continuous = false;

        if (condition.type == ACONSTANT) {
            continuous = (boolean)condition.data.get("constant");

            if (!continuous) {
                throw new IllegalArgumentException(whil.error("Extraneous while loop."));
            }

            if (whilblock == null) {
                throw new IllegalArgumentException(whil.error("While loop has no escape."));
            }
        }

        if (whilblock != null) {
            final MetadataStatement whilblockms = new MetadataStatement();

            whilblockms.beginLoop = true;
            whilblockms.inLoop = true;

            analyzer.visit(whilblock, whilblockms);

            if (whilblockms.loopEscape && !whilblockms.anyContinue) {
                throw new IllegalArgumentException(whil.error("Extranous while loop."));
            }

            if (continuous && !whilblockms.anyBreak) {
                whilems.methodEscape = true;
                whilems.allEscape = true;
            }

            whil.data.put("count", whilblockms.statementCount);
        } else {
            whil.data.put("count", 1);
        }

        whilems.statementCount = 1;

        variables.decrementScope();
    }

    void visitDo(final Node dowhile, final MetadataStatement dowhilems) {
        variables.incrementScope();

        final Node dowhileblock = dowhile.children.get(0);
        final MetadataStatement dowhileblockms = new MetadataStatement();

        dowhileblockms.beginLoop = true;
        dowhileblockms.inLoop = true;

        analyzer.visit(dowhileblock, dowhileblockms);

        if (dowhileblockms.loopEscape && !dowhileblockms.anyContinue) {
            throw new IllegalArgumentException(dowhile.error("Extraneous do while loop."));
        }

        dowhile.data.put("count", dowhileblockms.statementCount);

        final Node condition = dowhile.children.get(0);
        final MetadataExpression conditionme = new MetadataExpression();

        conditionme.expected = definition.booleanType;
        analyzer.visit(condition, conditionme);
        final Node cast = caster.markCast(condition, conditionme);
        dowhile.children.set(0, cast);

        if (cast.type == ACONSTANT) {
            final boolean continuous = (boolean)cast.data.get("constant");

            if (!continuous) {
                throw new IllegalArgumentException(dowhile.error("Extraneous do while loop."));
            }

            if (!dowhileblockms.anyBreak) {
                dowhilems.methodEscape = true;
                dowhilems.allEscape = true;
            }
        }

        dowhilems.statementCount = 1;

        variables.decrementScope();
    }

    void visitFor(final Node fr, final MetadataStatement frms) {
        variables.incrementScope();

        final Node initializer = fr.children.get(0);
        final Node condition = fr.children.get(1);
        final Node afterthought = fr.children.get(2);
        final Node frblock = fr.children.get(3);

        boolean continuous = false;

        if (initializer != null) {
            if (initializer.type == DECLARATION) {
                analyzer.visit(initializer, new MetadataStatement());
            } else if (initializer.type == EXPRESSION) {
                final MetadataExpression initializerme = new MetadataExpression();

                initializerme.read = false;

                analyzer.visit(initializer, initializerme);

                initializer.data.put("type", initializerme.actual);

                if (!initializerme.statement) {
                    throw new IllegalArgumentException(initializer.error("Not a statement."));
                }
            }
        }

        if (condition != null) {
            final MetadataExpression conditionme = new MetadataExpression();

            conditionme.expected = definition.booleanType;
            analyzer.visit(condition, conditionme);
            final Node cast = caster.markCast(condition, conditionme);
            fr.children.set(1, cast);

            if (cast.type == ACONSTANT) {
                continuous = (boolean)cast.data.get("constant");

                if (!continuous) {
                    throw new IllegalArgumentException(fr.error("Extraneous for loop."));
                }

                if (frblock == null) {
                    throw new IllegalArgumentException(fr.error("For loop has no escape."));
                }
            }
        } else {
            continuous = true;
        }

        if (afterthought != null) {
            final MetadataExpression afterthoughtme = new MetadataExpression();

            afterthoughtme.read = false;

            analyzer.visit(initializer, afterthoughtme);

            afterthought.data.put("type", afterthoughtme.actual);

            if (!afterthoughtme.statement) {
                throw new IllegalArgumentException(afterthought.error("Not a statement."));
            }
        }

        if (frblock != null) {
            final MetadataStatement frblockms = new MetadataStatement();

            frblockms.beginLoop = true;
            frblockms.inLoop = true;

            analyzer.visit(frblock, frblockms);

            if (frblockms.loopEscape && !frblockms.anyContinue) {
                throw new IllegalArgumentException(fr.error("Extraneous for loop."));
            }

            if (continuous && !frblockms.anyBreak) {
                frms.methodEscape = true;
                frms.allEscape = true;
            }

            fr.data.put("count", frblockms.statementCount);
        } else {
            fr.data.put("count", 1);
        }

        frms.statementCount = 1;

        variables.decrementScope();
    }

    void visitContinue(final Node continu, final MetadataStatement continums) {
        if (!continums.inLoop) {
            throw new IllegalArgumentException(continu.error("Continue statement outside of a loop."));
        }

        if (continums.lastLoop) {
            throw new IllegalArgumentException(continu.error("Extraneous continue statement."));
        }

        continums.allEscape = true;
        continums.anyContinue = true;
        continums.statementCount = 1;
    }

    void visitBreak(final Node brake, final MetadataStatement brakems) {
        if (!brakems.inLoop) {
            throw new IllegalArgumentException(brake.error("Break statement outside of a loop."));
        }

        brakems.loopEscape = true;
        brakems.allEscape = true;
        brakems.anyBreak = true;
        brakems.statementCount = 1;
    }

    void visitReturn(final Node rtn, final MetadataStatement rtnms) {
        final Node expression = rtn.children.get(0);
        final MetadataExpression expressionme = new MetadataExpression();

        expressionme.expected = definition.booleanType;
        analyzer.visit(expression, expressionme);
        rtn.children.set(0, caster.markCast(expression, expressionme));

        rtnms.methodEscape = true;
        rtnms.loopEscape = true;
        rtnms.allEscape = true;
        rtnms.statementCount = 1;
    }

    void visitTry(final Node ty, final MetadataStatement tyms) {
        final BlockContext blockctx = ctx.block();
        final MetadataStatement blocksmd = metadata.createStatementMetadata(blockctx);
        blocksmd.lastSource = trysmd.lastSource;
        blocksmd.inLoop = trysmd.inLoop;
        blocksmd.lastLoop = trysmd.lastLoop;
        utility.incrementScope();
        analyzer.visit(blockctx);
        utility.decrementScope();

        trysmd.methodEscape = blocksmd.methodEscape;
        trysmd.loopEscape = blocksmd.loopEscape;
        trysmd.allLast = blocksmd.allLast;
        trysmd.anyContinue = blocksmd.anyContinue;
        trysmd.anyBreak = blocksmd.anyBreak;

        int trapcount = 0;

        for (final TrapContext trapctx : ctx.trap()) {
            final MetadataStatement trapsmd = metadata.createStatementMetadata(trapctx);
            trapsmd.lastSource = trysmd.lastSource;
            trapsmd.inLoop = trysmd.inLoop;
            trapsmd.lastLoop = trysmd.lastLoop;
            utility.incrementScope();
            analyzer.visit(trapctx);
            utility.decrementScope();

            trysmd.methodEscape &= trapsmd.methodEscape;
            trysmd.loopEscape &= trapsmd.loopEscape;
            trysmd.allLast &= trapsmd.allLast;
            trysmd.anyContinue |= trapsmd.anyContinue;
            trysmd.anyBreak |= trapsmd.anyBreak;

            trapcount = Math.max(trapcount, trapsmd.count);
        }

        trysmd.count = blocksmd.count + trapcount;
    }

    void visitThrow(final ThrowContext ctx) {
        final MetadataStatement throwsmd = metadata.getStatementMetadata(ctx);

        final ExpressionContext exprctx = AnalyzerUtility.updateExpressionTree(ctx.expression());
        final ExpressionMetadata expremd = metadata.createExpressionMetadata(exprctx);
        expremd.to = definition.exceptionType;
        analyzer.visit(exprctx);
        caster.markCast(expremd);

        throwsmd.methodEscape = true;
        throwsmd.loopEscape = true;
        throwsmd.allLast = true;

        throwsmd.count = 1;
    }

    void visitExpr(final ExprContext ctx) {
        final MetadataStatement exprsmd = metadata.getStatementMetadata(ctx);
        final ExpressionContext exprctx = AnalyzerUtility.updateExpressionTree(ctx.expression());
        final ExpressionMetadata expremd = metadata.createExpressionMetadata(exprctx);
        expremd.read = exprsmd.lastSource;
        analyzer.visit(exprctx);

        if (!expremd.statement && !exprsmd.lastSource) {
            throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Not a statement.");
        }

        final boolean rtn = exprsmd.lastSource && expremd.from.sort != Sort.VOID;
        exprsmd.methodEscape = rtn;
        exprsmd.loopEscape = rtn;
        exprsmd.allLast = rtn;
        expremd.to = rtn ? definition.objectType : expremd.from;
        caster.markCast(expremd);

        exprsmd.count = 1;
    }

    void visitMultiple(final MultipleContext ctx) {
        final MetadataStatement multiplesmd = metadata.getStatementMetadata(ctx);
        final List<StatementContext> statectxs = ctx.statement();
        final StatementContext lastctx = statectxs.get(statectxs.size() - 1);

        for (StatementContext statectx : statectxs) {
            if (multiplesmd.allLast) {
                throw new IllegalArgumentException(AnalyzerUtility.error(statectx) +
                    "Statement will never be executed because all prior paths escape.");
            }

            final MetadataStatement statesmd = metadata.createStatementMetadata(statectx);
            statesmd.lastSource = multiplesmd.lastSource && statectx == lastctx;
            statesmd.inLoop = multiplesmd.inLoop;
            statesmd.lastLoop = (multiplesmd.beginLoop || multiplesmd.lastLoop) && statectx == lastctx;
            analyzer.visit(statectx);

            multiplesmd.methodEscape = statesmd.methodEscape;
            multiplesmd.loopEscape = statesmd.loopEscape;
            multiplesmd.allLast = statesmd.allLast;
            multiplesmd.anyContinue |= statesmd.anyContinue;
            multiplesmd.anyBreak |= statesmd.anyBreak;

            multiplesmd.count += statesmd.count;
        }
    }

    void visitSingle(final SingleContext ctx) {
        final MetadataStatement singlesmd = metadata.getStatementMetadata(ctx);

        final StatementContext statectx = ctx.statement();
        final MetadataStatement statesmd = metadata.createStatementMetadata(statectx);
        statesmd.lastSource = singlesmd.lastSource;
        statesmd.inLoop = singlesmd.inLoop;
        statesmd.lastLoop = singlesmd.beginLoop || singlesmd.lastLoop;
        analyzer.visit(statectx);

        singlesmd.methodEscape = statesmd.methodEscape;
        singlesmd.loopEscape = statesmd.loopEscape;
        singlesmd.allLast = statesmd.allLast;
        singlesmd.anyContinue = statesmd.anyContinue;
        singlesmd.anyBreak = statesmd.anyBreak;

        singlesmd.count = statesmd.count;
    }

    void visitDeclaration(final DeclarationContext ctx) {
        final DecltypeContext decltypectx = ctx.decltype();
        final ExpressionMetadata decltypeemd = metadata.createExpressionMetadata(decltypectx);
        analyzer.visit(decltypectx);

        for (final DeclvarContext declvarctx : ctx.declvar()) {
            final ExpressionMetadata declvaremd = metadata.createExpressionMetadata(declvarctx);
            declvaremd.to = decltypeemd.from;
            analyzer.visit(declvarctx);
        }
    }

    void visitDecltype(final DecltypeContext ctx) {
        final ExpressionMetadata decltypeemd = metadata.getExpressionMetadata(ctx);
        final IdentifierContext idctx = ctx.identifier();
        final String type = ctx.getText();

        utility.isValidType(idctx, true);
        decltypeemd.from = definition.getType(type);
    }

    void visitDeclvar(final DeclvarContext ctx) {
        final ExpressionMetadata declvaremd = metadata.getExpressionMetadata(ctx);
        final IdentifierContext idctx = ctx.identifier();
        final String identifier = idctx.getText();

        utility.isValidIdentifier(idctx, true);
        declvaremd.postConst = utility.addVariable(ctx, identifier, declvaremd.to).slot;

        final ExpressionContext exprctx = AnalyzerUtility.updateExpressionTree(ctx.expression());

        if (exprctx != null) {
            final ExpressionMetadata expremd = metadata.createExpressionMetadata(exprctx);
            expremd.to = declvaremd.to;
            analyzer.visit(exprctx);
            caster.markCast(expremd);
        }
    }

    void visitTrap(final TrapContext ctx) {
        final MetadataStatement trapsmd = metadata.getStatementMetadata(ctx);

        final IdentifierContext idctx0 = ctx.identifier(0);
        final String type = idctx0.getText();
        utility.isValidType(idctx0, true);
        trapsmd.exception = definition.getType(type);

        try {
            trapsmd.exception.clazz.asSubclass(Exception.class);
        } catch (final ClassCastException exception) {
            throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Invalid exception type [" + trapsmd.exception.name + "].");
        }

        final IdentifierContext idctx1 = ctx.identifier(1);
        final String identifier = idctx1.getText();
        utility.isValidIdentifier(idctx1, true);
        trapsmd.slot = utility.addVariable(ctx, identifier, trapsmd.exception).slot;

        final BlockContext blockctx = ctx.block();

        if (blockctx != null) {
            final MetadataStatement blocksmd = metadata.createStatementMetadata(blockctx);
            blocksmd.lastSource = trapsmd.lastSource;
            blocksmd.inLoop = trapsmd.inLoop;
            blocksmd.lastLoop = trapsmd.lastLoop;
            analyzer.visit(blockctx);

            trapsmd.methodEscape = blocksmd.methodEscape;
            trapsmd.loopEscape = blocksmd.loopEscape;
            trapsmd.allLast = blocksmd.allLast;
            trapsmd.anyContinue = blocksmd.anyContinue;
            trapsmd.anyBreak = blocksmd.anyBreak;
        } else if (ctx.emptyscope() == null) {
            throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
        }
    }
}
