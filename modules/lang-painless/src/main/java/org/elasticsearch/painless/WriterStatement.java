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
import org.elasticsearch.painless.PainlessParser.DeclContext;
import org.elasticsearch.painless.PainlessParser.DeclarationContext;
import org.elasticsearch.painless.PainlessParser.DecltypeContext;
import org.elasticsearch.painless.PainlessParser.DeclvarContext;
import org.elasticsearch.painless.PainlessParser.DoContext;
import org.elasticsearch.painless.PainlessParser.EmptyscopeContext;
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
import org.elasticsearch.painless.WriterUtility.Branch;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.GeneratorAdapter;

import static org.elasticsearch.painless.WriterConstants.PAINLESS_ERROR_TYPE;

class WriterStatement {
    private final Metadata metadata;

    private final GeneratorAdapter execute;

    private final Writer writer;
    private final WriterUtility utility;

    WriterStatement(final Metadata metadata, final GeneratorAdapter execute,
                    final Writer writer, final WriterUtility utility) {
        this.metadata = metadata;

        this.execute = execute;

        this.writer = writer;
        this.utility = utility;
    }

    void processSource(final SourceContext ctx) {
        final StatementMetadata sourcesmd = metadata.getStatementMetadata(ctx);

        for (final StatementContext sctx : ctx.statement()) {
            writer.visit(sctx);
        }

        if (!sourcesmd.methodEscape) {
            execute.visitInsn(Opcodes.ACONST_NULL);
            execute.returnValue();
        }
    }

    void processIf(final IfContext ctx) {
        final ExpressionContext exprctx = ctx.expression();
        final boolean els = ctx.ELSE() != null;
        final Branch branch = utility.markBranch(ctx, exprctx);
        branch.end = new Label();
        branch.fals = els ? new Label() : branch.end;

        writer.visit(exprctx);

        final BlockContext blockctx0 = ctx.block(0);
        final StatementMetadata blockmd0 = metadata.getStatementMetadata(blockctx0);
        writer.visit(blockctx0);

        if (els) {
            if (!blockmd0.allLast) {
                execute.goTo(branch.end);
            }

            execute.mark(branch.fals);
            writer.visit(ctx.block(1));
        }

        execute.mark(branch.end);
    }

    void processWhile(final WhileContext ctx) {
        final ExpressionContext exprctx = ctx.expression();
        final Branch branch = utility.markBranch(ctx, exprctx);
        branch.begin = new Label();
        branch.end = new Label();
        branch.fals = branch.end;

        utility.pushJump(branch);
        execute.mark(branch.begin);
        writer.visit(exprctx);

        final BlockContext blockctx = ctx.block();
        boolean allLast = false;

        if (blockctx != null) {
            final StatementMetadata blocksmd = metadata.getStatementMetadata(blockctx);
            allLast = blocksmd.allLast;
            writeLoopCounter(blocksmd.count > 0 ? blocksmd.count : 1);
            writer.visit(blockctx);
        } else if (ctx.empty() != null) {
            writeLoopCounter(1);
        } else {
            throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
        }

        if (!allLast) {
            execute.goTo(branch.begin);
        }

        execute.mark(branch.end);
        utility.popJump();
    }

    void processDo(final DoContext ctx) {
        final ExpressionContext exprctx = ctx.expression();
        final Branch branch = utility.markBranch(ctx, exprctx);
        Label start = new Label();
        branch.begin = new Label();
        branch.end = new Label();
        branch.fals = branch.end;

        final BlockContext blockctx = ctx.block();
        final StatementMetadata blocksmd = metadata.getStatementMetadata(blockctx);

        utility.pushJump(branch);
        execute.mark(start);
        writer.visit(blockctx);
        execute.mark(branch.begin);
        writer.visit(exprctx);
        writeLoopCounter(blocksmd.count > 0 ? blocksmd.count : 1);
        execute.goTo(start);
        execute.mark(branch.end);
        utility.popJump();
    }

    void processFor(final ForContext ctx) {
        final ExpressionContext exprctx = ctx.expression();
        final AfterthoughtContext atctx = ctx.afterthought();
        final Branch branch = utility.markBranch(ctx, exprctx);
        final Label start = new Label();
        branch.begin = atctx == null ? start : new Label();
        branch.end = new Label();
        branch.fals = branch.end;

        utility.pushJump(branch);

        if (ctx.initializer() != null) {
            writer.visit(ctx.initializer());
        }

        execute.mark(start);

        if (exprctx != null) {
            writer.visit(exprctx);
        }

        final BlockContext blockctx = ctx.block();
        boolean allLast = false;

        if (blockctx != null) {
            StatementMetadata blocksmd = metadata.getStatementMetadata(blockctx);
            allLast = blocksmd.allLast;

            int count = blocksmd.count > 0 ? blocksmd.count : 1;

            if (atctx != null) {
                ++count;
            }

            writeLoopCounter(count);
            writer.visit(blockctx);
        } else if (ctx.empty() != null) {
            writeLoopCounter(1);
        } else {
            throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
        }

        if (atctx != null) {
            execute.mark(branch.begin);
            writer.visit(atctx);
        }

        if (atctx != null || !allLast) {
            execute.goTo(start);
        }

        execute.mark(branch.end);
        utility.popJump();
    }

    void processDecl(final DeclContext ctx) {
        writer.visit(ctx.declaration());
    }

    void processContinue() {
        final Branch jump = utility.peekJump();
        execute.goTo(jump.begin);
    }

    void processBreak() {
        final Branch jump = utility.peekJump();
        execute.goTo(jump.end);
    }

    void processReturn(final ReturnContext ctx) {
        writer.visit(ctx.expression());
        execute.returnValue();
    }

    void processTry(final TryContext ctx) {
        final TrapContext[] trapctxs = new TrapContext[ctx.trap().size()];
        ctx.trap().toArray(trapctxs);
        final Branch branch = utility.markBranch(ctx, trapctxs);

        Label end = new Label();
        branch.begin = new Label();
        branch.end = new Label();
        branch.tru = trapctxs.length > 1 ? end : null;

        execute.mark(branch.begin);

        final BlockContext blockctx = ctx.block();
        final StatementMetadata blocksmd = metadata.getStatementMetadata(blockctx);
        writer.visit(blockctx);

        if (!blocksmd.allLast) {
            execute.goTo(end);
        }

        execute.mark(branch.end);

        for (final TrapContext trapctx : trapctxs) {
            writer.visit(trapctx);
        }

        if (!blocksmd.allLast || trapctxs.length > 1) {
            execute.mark(end);
        }
    }

    void processThrow(final ThrowContext ctx) {
        writer.visit(ctx.expression());
        execute.throwException();
    }

    void processExpr(final ExprContext ctx) {
        final StatementMetadata exprsmd = metadata.getStatementMetadata(ctx);
        final ExpressionContext exprctx = ctx.expression();
        final ExpressionMetadata expremd = metadata.getExpressionMetadata(exprctx);
        writer.visit(exprctx);

        if (exprsmd.methodEscape) {
            execute.returnValue();
        } else {
            utility.writePop(expremd.to.type.getSize());
        }
    }

    void processMultiple(final MultipleContext ctx) {
        for (final StatementContext sctx : ctx.statement()) {
            writer.visit(sctx);
        }
    }

    void processSingle(final SingleContext ctx) {
        writer.visit(ctx.statement());
    }

    void processInitializer(InitializerContext ctx) {
        final DeclarationContext declctx = ctx.declaration();
        final ExpressionContext exprctx = ctx.expression();

        if (declctx != null) {
            writer.visit(declctx);
        } else if (exprctx != null) {
            final ExpressionMetadata expremd = metadata.getExpressionMetadata(exprctx);
            writer.visit(exprctx);
            utility.writePop(expremd.to.type.getSize());
        } else {
            throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
        }
    }

    void processAfterthought(AfterthoughtContext ctx) {
        final ExpressionContext exprctx = ctx.expression();
        final ExpressionMetadata expremd = metadata.getExpressionMetadata(exprctx);
        writer.visit(ctx.expression());
        utility.writePop(expremd.to.type.getSize());
    }

    void processDeclaration(DeclarationContext ctx) {
        for (final DeclvarContext declctx : ctx.declvar()) {
            writer.visit(declctx);
        }
    }

    void processDeclvar(final DeclvarContext ctx) {
        final ExpressionMetadata declvaremd = metadata.getExpressionMetadata(ctx);
        final org.objectweb.asm.Type type = declvaremd.to.type;
        final Sort sort = declvaremd.to.sort;
        int slot = (int)declvaremd.postConst;

        if (!metadata.scoreValueUsed && slot > metadata.scoreValueSlot) {
            --slot;
        }

        final ExpressionContext exprctx = ctx.expression();
        final boolean initialize = exprctx == null;

        if (!initialize) {
            writer.visit(exprctx);
        }

        switch (sort) {
            case VOID:   throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
            case BOOL:
            case BYTE:
            case SHORT:
            case CHAR:
            case INT:    if (initialize) execute.push(0);    break;
            case LONG:   if (initialize) execute.push(0L);   break;
            case FLOAT:  if (initialize) execute.push(0.0F); break;
            case DOUBLE: if (initialize) execute.push(0.0);  break;
            default:     if (initialize) execute.visitInsn(Opcodes.ACONST_NULL);
        }

        execute.visitVarInsn(type.getOpcode(Opcodes.ISTORE), slot);
    }

    void processTrap(final TrapContext ctx) {
        final StatementMetadata trapsmd = metadata.getStatementMetadata(ctx);

        final Branch branch = utility.getBranch(ctx);
        final Label jump = new Label();

        final BlockContext blockctx = ctx.block();
        final EmptyscopeContext emptyctx = ctx.emptyscope();

        execute.mark(jump);
        execute.visitVarInsn(trapsmd.exception.type.getOpcode(Opcodes.ISTORE), trapsmd.slot);

        if (blockctx != null) {
            writer.visit(ctx.block());
        } else if (emptyctx == null) {
            throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
        }

        execute.visitTryCatchBlock(branch.begin, branch.end, jump, trapsmd.exception.type.getInternalName());

        if (branch.tru != null && !trapsmd.allLast) {
            execute.goTo(branch.tru);
        }
    }

    private void writeLoopCounter(final int count) {
        final Label end = new Label();

        execute.iinc(metadata.loopCounterSlot, -count);
        execute.visitVarInsn(Opcodes.ILOAD, metadata.loopCounterSlot);
        execute.push(0);
        execute.ifICmp(GeneratorAdapter.GT, end);
        execute.throwException(PAINLESS_ERROR_TYPE,
            "The maximum number of statements that can be executed in a loop has been reached.");
        execute.mark(end);
    }
}
