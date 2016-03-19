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

package org.elasticsearch.painless;

import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Metadata.ExpressionMetadata;
import org.elasticsearch.painless.Metadata.StatementMetadata;
import org.elasticsearch.painless.PainlessParser.AfterthoughtContext;
import org.elasticsearch.painless.PainlessParser.BlockContext;
import org.elasticsearch.painless.PainlessParser.BreakContext;
import org.elasticsearch.painless.PainlessParser.ContinueContext;
import org.elasticsearch.painless.PainlessParser.DeclContext;
import org.elasticsearch.painless.PainlessParser.DeclarationContext;
import org.elasticsearch.painless.PainlessParser.DecltypeContext;
import org.elasticsearch.painless.PainlessParser.DeclvarContext;
import org.elasticsearch.painless.PainlessParser.DoContext;
import org.elasticsearch.painless.PainlessParser.ExprContext;
import org.elasticsearch.painless.PainlessParser.ExpressionContext;
import org.elasticsearch.painless.PainlessParser.ForContext;
import org.elasticsearch.painless.PainlessParser.IfContext;
import org.elasticsearch.painless.PainlessParser.InitializerContext;
import org.elasticsearch.painless.PainlessParser.MultipleContext;
import org.elasticsearch.painless.PainlessParser.ReturnContext;
import org.elasticsearch.painless.PainlessParser.SingleContext;
import org.elasticsearch.painless.PainlessParser.SourceContext;
import org.elasticsearch.painless.PainlessParser.StatementContext;
import org.elasticsearch.painless.PainlessParser.ThrowContext;
import org.elasticsearch.painless.PainlessParser.TrapContext;
import org.elasticsearch.painless.PainlessParser.TryContext;
import org.elasticsearch.painless.PainlessParser.WhileContext;

import java.util.List;

class AnalyzerStatement {
    private final Metadata metadata;
    private final Definition definition;

    private final Analyzer analyzer;
    private final AnalyzerUtility utility;
    private final AnalyzerCaster caster;

    AnalyzerStatement(final Metadata metadata, final Analyzer analyzer,
                      final AnalyzerUtility utility, final AnalyzerCaster caster) {
        this.metadata = metadata;
        this.definition = metadata.definition;

        this.analyzer = analyzer;
        this.utility = utility;
        this.caster = caster;
    }

    void processSource(final SourceContext ctx) {
        final StatementMetadata sourcesmd = metadata.getStatementMetadata(ctx);
        final List<StatementContext> statectxs = ctx.statement();
        final StatementContext lastctx = statectxs.get(statectxs.size() - 1);

        utility.incrementScope();

        for (final StatementContext statectx : statectxs) {
            if (sourcesmd.allLast) {
                throw new IllegalArgumentException(AnalyzerUtility.error(statectx) +
                    "Statement will never be executed because all prior paths escape.");
            }

            final StatementMetadata statesmd = metadata.createStatementMetadata(statectx);
            statesmd.lastSource = statectx == lastctx;
            analyzer.visit(statectx);

            sourcesmd.methodEscape = statesmd.methodEscape;
            sourcesmd.allLast = statesmd.allLast;
        }

        utility.decrementScope();
    }

    void processIf(final IfContext ctx) {
        final StatementMetadata ifsmd = metadata.getStatementMetadata(ctx);

        final ExpressionContext exprctx = AnalyzerUtility.updateExpressionTree(ctx.expression());
        final ExpressionMetadata expremd = metadata.createExpressionMetadata(exprctx);
        expremd.to = definition.booleanType;
        analyzer.visit(exprctx);
        caster.markCast(expremd);

        if (expremd.postConst != null) {
            throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "If statement is not necessary.");
        }

        final BlockContext blockctx0 = ctx.block(0);
        final StatementMetadata blocksmd0 = metadata.createStatementMetadata(blockctx0);
        blocksmd0.lastSource = ifsmd.lastSource;
        blocksmd0.inLoop = ifsmd.inLoop;
        blocksmd0.lastLoop = ifsmd.lastLoop;
        utility.incrementScope();
        analyzer.visit(blockctx0);
        utility.decrementScope();

        ifsmd.anyContinue = blocksmd0.anyContinue;
        ifsmd.anyBreak = blocksmd0.anyBreak;

        ifsmd.count = blocksmd0.count;

        if (ctx.ELSE() != null) {
            final BlockContext blockctx1 = ctx.block(1);
            final StatementMetadata blocksmd1 = metadata.createStatementMetadata(blockctx1);
            blocksmd1.lastSource = ifsmd.lastSource;
            utility.incrementScope();
            analyzer.visit(blockctx1);
            utility.decrementScope();

            ifsmd.methodEscape = blocksmd0.methodEscape && blocksmd1.methodEscape;
            ifsmd.loopEscape = blocksmd0.loopEscape && blocksmd1.loopEscape;
            ifsmd.allLast = blocksmd0.allLast && blocksmd1.allLast;
            ifsmd.anyContinue |= blocksmd1.anyContinue;
            ifsmd.anyBreak |= blocksmd1.anyBreak;

            ifsmd.count = Math.max(ifsmd.count, blocksmd1.count);
        }
    }

    void processWhile(final WhileContext ctx) {
        final StatementMetadata whilesmd = metadata.getStatementMetadata(ctx);

        utility.incrementScope();

        final ExpressionContext exprctx = AnalyzerUtility.updateExpressionTree(ctx.expression());
        final ExpressionMetadata expremd = metadata.createExpressionMetadata(exprctx);
        expremd.to = definition.booleanType;
        analyzer.visit(exprctx);
        caster.markCast(expremd);

        boolean continuous = false;

        if (expremd.postConst != null) {
            continuous = (boolean)expremd.postConst;

            if (!continuous) {
                throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "The loop will never be executed.");
            }

            if (ctx.empty() != null) {
                throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "The loop will never exit.");
            }
        }

        final BlockContext blockctx = ctx.block();

        if (blockctx != null) {
            final StatementMetadata blocksmd = metadata.createStatementMetadata(blockctx);
            blocksmd.beginLoop = true;
            blocksmd.inLoop = true;
            analyzer.visit(blockctx);

            if (blocksmd.loopEscape && !blocksmd.anyContinue) {
                throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "All paths escape so the loop is not necessary.");
            }

            if (continuous && !blocksmd.anyBreak) {
                whilesmd.methodEscape = true;
                whilesmd.allLast = true;
            }
        }

        whilesmd.count = 1;

        utility.decrementScope();
    }

    void processDo(final DoContext ctx) {
        final StatementMetadata dosmd = metadata.getStatementMetadata(ctx);

        utility.incrementScope();

        final BlockContext blockctx = ctx.block();
        final StatementMetadata blocksmd = metadata.createStatementMetadata(blockctx);
        blocksmd.beginLoop = true;
        blocksmd.inLoop = true;
        analyzer.visit(blockctx);

        if (blocksmd.loopEscape && !blocksmd.anyContinue) {
            throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "All paths escape so the loop is not necessary.");
        }

        final ExpressionContext exprctx = AnalyzerUtility.updateExpressionTree(ctx.expression());
        final ExpressionMetadata expremd = metadata.createExpressionMetadata(exprctx);
        expremd.to = definition.booleanType;
        analyzer.visit(exprctx);
        caster.markCast(expremd);

        if (expremd.postConst != null) {
            final boolean continuous = (boolean)expremd.postConst;

            if (!continuous) {
                throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "All paths escape so the loop is not necessary.");
            }

            if (!blocksmd.anyBreak) {
                dosmd.methodEscape = true;
                dosmd.allLast = true;
            }
        }

        dosmd.count = 1;

        utility.decrementScope();
    }

    void processFor(final ForContext ctx) {
        final StatementMetadata forsmd = metadata.getStatementMetadata(ctx);
        boolean continuous = false;

        utility.incrementScope();

        final InitializerContext initctx = ctx.initializer();

        if (initctx != null) {
            metadata.createStatementMetadata(initctx);
            analyzer.visit(initctx);
        }

        final ExpressionContext exprctx = AnalyzerUtility.updateExpressionTree(ctx.expression());

        if (exprctx != null) {
            final ExpressionMetadata expremd = metadata.createExpressionMetadata(exprctx);
            expremd.to = definition.booleanType;
            analyzer.visit(exprctx);
            caster.markCast(expremd);

            if (expremd.postConst != null) {
                continuous = (boolean)expremd.postConst;

                if (!continuous) {
                    throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "The loop will never be executed.");
                }

                if (ctx.empty() != null) {
                    throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "The loop is continuous.");
                }
            }
        } else {
            continuous = true;
        }

        final AfterthoughtContext atctx = ctx.afterthought();

        if (atctx != null) {
            metadata.createStatementMetadata(atctx);
            analyzer.visit(atctx);
        }

        final BlockContext blockctx = ctx.block();

        if (blockctx != null) {
            final StatementMetadata blocksmd = metadata.createStatementMetadata(blockctx);
            blocksmd.beginLoop = true;
            blocksmd.inLoop = true;
            analyzer.visit(blockctx);

            if (blocksmd.loopEscape && !blocksmd.anyContinue) {
                throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "All paths escape so the loop is not necessary.");
            }

            if (continuous && !blocksmd.anyBreak) {
                forsmd.methodEscape = true;
                forsmd.allLast = true;
            }
        }

        forsmd.count = 1;

        utility.decrementScope();
    }

    void processDecl(final DeclContext ctx) {
        final StatementMetadata declsmd = metadata.getStatementMetadata(ctx);

        final DeclarationContext declctx = ctx.declaration();
        metadata.createStatementMetadata(declctx);
        analyzer.visit(declctx);

        declsmd.count = 1;
    }

    void processContinue(final ContinueContext ctx) {
        final StatementMetadata continuesmd = metadata.getStatementMetadata(ctx);

        if (!continuesmd.inLoop) {
            throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Cannot have a continue statement outside of a loop.");
        }

        if (continuesmd.lastLoop) {
            throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Unnecessary continue statement at the end of a loop.");
        }

        continuesmd.allLast = true;
        continuesmd.anyContinue = true;

        continuesmd.count = 1;
    }

    void processBreak(final BreakContext ctx) {
        final StatementMetadata breaksmd = metadata.getStatementMetadata(ctx);

        if (!breaksmd.inLoop) {
            throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Cannot have a break statement outside of a loop.");
        }

        breaksmd.loopEscape = true;
        breaksmd.allLast = true;
        breaksmd.anyBreak = true;

        breaksmd.count = 1;
    }

    void processReturn(final ReturnContext ctx) {
        final StatementMetadata returnsmd = metadata.getStatementMetadata(ctx);

        final ExpressionContext exprctx = AnalyzerUtility.updateExpressionTree(ctx.expression());
        final ExpressionMetadata expremd = metadata.createExpressionMetadata(exprctx);
        expremd.to = definition.objectType;
        analyzer.visit(exprctx);
        caster.markCast(expremd);

        returnsmd.methodEscape = true;
        returnsmd.loopEscape = true;
        returnsmd.allLast = true;

        returnsmd.count = 1;
    }

    void processTry(final TryContext ctx) {
        final StatementMetadata trysmd = metadata.getStatementMetadata(ctx);

        final BlockContext blockctx = ctx.block();
        final StatementMetadata blocksmd = metadata.createStatementMetadata(blockctx);
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
            final StatementMetadata trapsmd = metadata.createStatementMetadata(trapctx);
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

    void processThrow(final ThrowContext ctx) {
        final StatementMetadata throwsmd = metadata.getStatementMetadata(ctx);

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

    void processExpr(final ExprContext ctx) {
        final StatementMetadata exprsmd = metadata.getStatementMetadata(ctx);
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

    void processMultiple(final MultipleContext ctx) {
        final StatementMetadata multiplesmd = metadata.getStatementMetadata(ctx);
        final List<StatementContext> statectxs = ctx.statement();
        final StatementContext lastctx = statectxs.get(statectxs.size() - 1);

        for (StatementContext statectx : statectxs) {
            if (multiplesmd.allLast) {
                throw new IllegalArgumentException(AnalyzerUtility.error(statectx) +
                    "Statement will never be executed because all prior paths escape.");
            }

            final StatementMetadata statesmd = metadata.createStatementMetadata(statectx);
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

    void processSingle(final SingleContext ctx) {
        final StatementMetadata singlesmd = metadata.getStatementMetadata(ctx);

        final StatementContext statectx = ctx.statement();
        final StatementMetadata statesmd = metadata.createStatementMetadata(statectx);
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

    void processInitializer(InitializerContext ctx) {
        final DeclarationContext declctx = ctx.declaration();
        final ExpressionContext exprctx = AnalyzerUtility.updateExpressionTree(ctx.expression());

        if (declctx != null) {
            metadata.createStatementMetadata(declctx);
            analyzer.visit(declctx);
        } else if (exprctx != null) {
            final ExpressionMetadata expremd = metadata.createExpressionMetadata(exprctx);
            expremd.read = false;
            analyzer.visit(exprctx);

            expremd.to = expremd.from;
            caster.markCast(expremd);

            if (!expremd.statement) {
                throw new IllegalArgumentException(AnalyzerUtility.error(exprctx) +
                    "The initializer of a for loop must be a statement.");
            }
        } else {
            throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
        }
    }

    void processAfterthought(AfterthoughtContext ctx) {
        final ExpressionContext exprctx = AnalyzerUtility.updateExpressionTree(ctx.expression());

        if (exprctx != null) {
            final ExpressionMetadata expremd = metadata.createExpressionMetadata(exprctx);
            expremd.read = false;
            analyzer.visit(exprctx);

            expremd.to = expremd.from;
            caster.markCast(expremd);

            if (!expremd.statement) {
                throw new IllegalArgumentException(AnalyzerUtility.error(exprctx) +
                    "The afterthought of a for loop must be a statement.");
            }
        }
    }

    void processDeclaration(final DeclarationContext ctx) {
        final DecltypeContext decltypectx = ctx.decltype();
        final ExpressionMetadata decltypeemd = metadata.createExpressionMetadata(decltypectx);
        analyzer.visit(decltypectx);

        for (final DeclvarContext declvarctx : ctx.declvar()) {
            final ExpressionMetadata declvaremd = metadata.createExpressionMetadata(declvarctx);
            declvaremd.to = decltypeemd.from;
            analyzer.visit(declvarctx);
        }
    }

    void processDecltype(final DecltypeContext ctx) {
        final ExpressionMetadata decltypeemd = metadata.getExpressionMetadata(ctx);
        final String name = ctx.getText();
        decltypeemd.from = definition.getType(name);
    }

    void processDeclvar(final DeclvarContext ctx) {
        final ExpressionMetadata declvaremd = metadata.getExpressionMetadata(ctx);

        final String name = ctx.ID().getText();
        declvaremd.postConst = utility.addVariable(ctx, name, declvaremd.to).slot;

        final ExpressionContext exprctx = AnalyzerUtility.updateExpressionTree(ctx.expression());

        if (exprctx != null) {
            final ExpressionMetadata expremd = metadata.createExpressionMetadata(exprctx);
            expremd.to = declvaremd.to;
            analyzer.visit(exprctx);
            caster.markCast(expremd);
        }
    }

    void processTrap(final TrapContext ctx) {
        final StatementMetadata trapsmd = metadata.getStatementMetadata(ctx);

        final String type = ctx.TYPE().getText();
        trapsmd.exception = definition.getType(type);

        try {
            trapsmd.exception.clazz.asSubclass(Exception.class);
        } catch (final ClassCastException exception) {
            throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Invalid exception type [" + trapsmd.exception.name + "].");
        }

        final String id = ctx.ID().getText();
        trapsmd.slot = utility.addVariable(ctx, id, trapsmd.exception).slot;

        final BlockContext blockctx = ctx.block();

        if (blockctx != null) {
            final StatementMetadata blocksmd = metadata.createStatementMetadata(blockctx);
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
