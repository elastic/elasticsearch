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
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Metadata.ExpressionMetadata;
import org.elasticsearch.painless.PainlessParser.AssignmentContext;
import org.elasticsearch.painless.PainlessParser.BinaryContext;
import org.elasticsearch.painless.PainlessParser.BoolContext;
import org.elasticsearch.painless.PainlessParser.CastContext;
import org.elasticsearch.painless.PainlessParser.CharContext;
import org.elasticsearch.painless.PainlessParser.CompContext;
import org.elasticsearch.painless.PainlessParser.ConditionalContext;
import org.elasticsearch.painless.PainlessParser.ExpressionContext;
import org.elasticsearch.painless.PainlessParser.ExternalContext;
import org.elasticsearch.painless.PainlessParser.FalseContext;
import org.elasticsearch.painless.PainlessParser.IncrementContext;
import org.elasticsearch.painless.PainlessParser.NullContext;
import org.elasticsearch.painless.PainlessParser.NumericContext;
import org.elasticsearch.painless.PainlessParser.PostincContext;
import org.elasticsearch.painless.PainlessParser.PreincContext;
import org.elasticsearch.painless.PainlessParser.TrueContext;
import org.elasticsearch.painless.PainlessParser.UnaryContext;
import org.elasticsearch.painless.WriterUtility.Branch;
import org.objectweb.asm.Label;
import org.objectweb.asm.Opcodes;
import org.objectweb.asm.commons.GeneratorAdapter;

import static org.elasticsearch.painless.PainlessParser.ADD;
import static org.elasticsearch.painless.PainlessParser.BWAND;
import static org.elasticsearch.painless.PainlessParser.BWOR;
import static org.elasticsearch.painless.PainlessParser.BWXOR;
import static org.elasticsearch.painless.PainlessParser.DIV;
import static org.elasticsearch.painless.PainlessParser.LSH;
import static org.elasticsearch.painless.PainlessParser.MUL;
import static org.elasticsearch.painless.PainlessParser.REM;
import static org.elasticsearch.painless.PainlessParser.RSH;
import static org.elasticsearch.painless.PainlessParser.SUB;
import static org.elasticsearch.painless.PainlessParser.USH;
import static org.elasticsearch.painless.WriterConstants.CHECKEQUALS;
import static org.elasticsearch.painless.WriterConstants.DEF_EQ_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_GTE_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_GT_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_LTE_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_LT_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_NEG_CALL;
import static org.elasticsearch.painless.WriterConstants.DEF_NOT_CALL;
import static org.elasticsearch.painless.WriterConstants.NEGATEEXACT_INT;
import static org.elasticsearch.painless.WriterConstants.NEGATEEXACT_LONG;

class WriterExpression {
    private final Metadata metadata;
    private final Definition definition;
    private final CompilerSettings settings;

    private final GeneratorAdapter execute;

    private final Writer writer;
    private final WriterUtility utility;
    private final WriterCaster caster;

    WriterExpression(final Metadata metadata, final GeneratorAdapter execute, final Writer writer,
                     final WriterUtility utility, final WriterCaster caster) {
        this.metadata = metadata;
        definition = metadata.definition;
        settings = metadata.settings;

        this.execute = execute;

        this.writer = writer;
        this.utility = utility;
        this.caster = caster;
    }

    void processNumeric(final NumericContext ctx) {
        final ExpressionMetadata numericemd = metadata.getExpressionMetadata(ctx);
        final Object postConst = numericemd.postConst;

        if (postConst == null) {
            utility.writeNumeric(ctx, numericemd.preConst);
            caster.checkWriteCast(numericemd);
        } else {
            utility.writeConstant(ctx, postConst);
        }

        utility.checkWriteBranch(ctx);
    }

    void processChar(final CharContext ctx) {
        final ExpressionMetadata charemd = metadata.getExpressionMetadata(ctx);
        final Object postConst = charemd.postConst;

        if (postConst == null) {
            utility.writeNumeric(ctx, (int)(char)charemd.preConst);
            caster.checkWriteCast(charemd);
        } else {
            utility.writeConstant(ctx, postConst);
        }

        utility.checkWriteBranch(ctx);
    }

    void processTrue(final TrueContext ctx) {
        final ExpressionMetadata trueemd = metadata.getExpressionMetadata(ctx);
        final Object postConst = trueemd.postConst;
        final Branch branch = utility.getBranch(ctx);

        if (branch == null) {
            if (postConst == null) {
                utility.writeBoolean(ctx, true);
                caster.checkWriteCast(trueemd);
            } else {
                utility.writeConstant(ctx, postConst);
            }
        } else if (branch.tru != null) {
            execute.goTo(branch.tru);
        }
    }

    void processFalse(final FalseContext ctx) {
        final ExpressionMetadata falseemd = metadata.getExpressionMetadata(ctx);
        final Object postConst = falseemd.postConst;
        final Branch branch = utility.getBranch(ctx);

        if (branch == null) {
            if (postConst == null) {
                utility.writeBoolean(ctx, false);
                caster.checkWriteCast(falseemd);
            } else {
                utility.writeConstant(ctx, postConst);
            }
        } else if (branch.fals != null) {
            execute.goTo(branch.fals);
        }
    }

    void processNull(final NullContext ctx) {
        final ExpressionMetadata nullemd = metadata.getExpressionMetadata(ctx);

        execute.visitInsn(Opcodes.ACONST_NULL);
        caster.checkWriteCast(nullemd);
        utility.checkWriteBranch(ctx);
    }

    void processExternal(final ExternalContext ctx) {
        final ExpressionMetadata expremd = metadata.getExpressionMetadata(ctx);
        writer.visit(ctx.extstart());
        caster.checkWriteCast(expremd);
        utility.checkWriteBranch(ctx);
    }


    void processPostinc(final PostincContext ctx) {
        final ExpressionMetadata expremd = metadata.getExpressionMetadata(ctx);
        writer.visit(ctx.extstart());
        caster.checkWriteCast(expremd);
        utility.checkWriteBranch(ctx);
    }

    void processPreinc(final PreincContext ctx) {
        final ExpressionMetadata expremd = metadata.getExpressionMetadata(ctx);
        writer.visit(ctx.extstart());
        caster.checkWriteCast(expremd);
        utility.checkWriteBranch(ctx);
    }

    void processUnary(final UnaryContext ctx) {
        final ExpressionMetadata unaryemd = metadata.getExpressionMetadata(ctx);
        final Object postConst = unaryemd.postConst;
        final Object preConst = unaryemd.preConst;
        final Branch branch = utility.getBranch(ctx);

        if (postConst != null) {
            if (ctx.BOOLNOT() != null) {
                if (branch == null) {
                    utility.writeConstant(ctx, postConst);
                } else {
                    if ((boolean)postConst && branch.tru != null) {
                        execute.goTo(branch.tru);
                    } else if (!(boolean)postConst && branch.fals != null) {
                        execute.goTo(branch.fals);
                    }
                }
            } else {
                utility.writeConstant(ctx, postConst);
                utility.checkWriteBranch(ctx);
            }
        } else if (preConst != null) {
            if (branch == null) {
                utility.writeConstant(ctx, preConst);
                caster.checkWriteCast(unaryemd);
            } else {
                throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
            }
        } else {
            final ExpressionContext exprctx = ctx.expression();

            if (ctx.BOOLNOT() != null) {
                final Branch local = utility.markBranch(ctx, exprctx);

                if (branch == null) {
                    local.fals = new Label();
                    final Label aend = new Label();

                    writer.visit(exprctx);

                    execute.push(false);
                    execute.goTo(aend);
                    execute.mark(local.fals);
                    execute.push(true);
                    execute.mark(aend);

                    caster.checkWriteCast(unaryemd);
                } else {
                    local.tru = branch.fals;
                    local.fals = branch.tru;

                    writer.visit(exprctx);
                }
            } else {
                final org.objectweb.asm.Type type = unaryemd.from.type;
                final Sort sort = unaryemd.from.sort;

                writer.visit(exprctx);

                if (ctx.BWNOT() != null) {
                    if (sort == Sort.DEF) {
                        execute.invokeStatic(definition.defobjType.type, DEF_NOT_CALL);
                    } else {
                        if (sort == Sort.INT) {
                            utility.writeConstant(ctx, -1);
                        } else if (sort == Sort.LONG) {
                            utility.writeConstant(ctx, -1L);
                        } else {
                            throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
                        }

                        execute.math(GeneratorAdapter.XOR, type);
                    }
                } else if (ctx.SUB() != null) {
                    if (sort == Sort.DEF) {
                        execute.invokeStatic(definition.defobjType.type, DEF_NEG_CALL);
                    } else {
                        if (settings.getNumericOverflow()) {
                            execute.math(GeneratorAdapter.NEG, type);
                        } else {
                            if (sort == Sort.INT) {
                                execute.invokeStatic(definition.mathType.type, NEGATEEXACT_INT);
                            } else if (sort == Sort.LONG) {
                                execute.invokeStatic(definition.mathType.type, NEGATEEXACT_LONG);
                            } else {
                                throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
                            }
                        }
                    }
                } else if (ctx.ADD() == null) {
                    throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
                }

                caster.checkWriteCast(unaryemd);
                utility.checkWriteBranch(ctx);
            }
        }
    }

    void processCast(final CastContext ctx) {
        final ExpressionMetadata castemd = metadata.getExpressionMetadata(ctx);
        final Object postConst = castemd.postConst;

        if (postConst == null) {
            writer.visit(ctx.expression());
            caster.checkWriteCast(castemd);
        } else {
            utility.writeConstant(ctx, postConst);
        }

        utility.checkWriteBranch(ctx);
    }

    void processBinary(final BinaryContext ctx) {
        final ExpressionMetadata binaryemd = metadata.getExpressionMetadata(ctx);
        final Object postConst = binaryemd.postConst;
        final Object preConst = binaryemd.preConst;
        final Branch branch = utility.getBranch(ctx);

        if (postConst != null) {
            utility.writeConstant(ctx, postConst);
        } else if (preConst != null) {
            if (branch == null) {
                utility.writeConstant(ctx, preConst);
                caster.checkWriteCast(binaryemd);
            } else {
                throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
            }
        } else if (binaryemd.from.sort == Sort.STRING) {
            final boolean marked = utility.containsStrings(ctx);

            if (!marked) {
                utility.writeNewStrings();
            }

            final ExpressionContext exprctx0 = ctx.expression(0);
            final ExpressionMetadata expremd0 = metadata.getExpressionMetadata(exprctx0);
            utility.addStrings(exprctx0);
            writer.visit(exprctx0);

            if (utility.containsStrings(exprctx0)) {
                utility.writeAppendStrings(expremd0.from.sort);
                utility.removeStrings(exprctx0);
            }

            final ExpressionContext exprctx1 = ctx.expression(1);
            final ExpressionMetadata expremd1 = metadata.getExpressionMetadata(exprctx1);
            utility.addStrings(exprctx1);
            writer.visit(exprctx1);

            if (utility.containsStrings(exprctx1)) {
                utility.writeAppendStrings(expremd1.from.sort);
                utility.removeStrings(exprctx1);
            }

            if (marked) {
                utility.removeStrings(ctx);
            } else {
                utility.writeToStrings();
            }

            caster.checkWriteCast(binaryemd);
        } else {
            final ExpressionContext exprctx0 = ctx.expression(0);
            final ExpressionContext exprctx1 = ctx.expression(1);

            writer.visit(exprctx0);
            writer.visit(exprctx1);

            final Type type = binaryemd.from;

            if      (ctx.MUL()   != null) utility.writeBinaryInstruction(ctx, type, MUL);
            else if (ctx.DIV()   != null) utility.writeBinaryInstruction(ctx, type, DIV);
            else if (ctx.REM()   != null) utility.writeBinaryInstruction(ctx, type, REM);
            else if (ctx.ADD()   != null) utility.writeBinaryInstruction(ctx, type, ADD);
            else if (ctx.SUB()   != null) utility.writeBinaryInstruction(ctx, type, SUB);
            else if (ctx.LSH()   != null) utility.writeBinaryInstruction(ctx, type, LSH);
            else if (ctx.USH()   != null) utility.writeBinaryInstruction(ctx, type, USH);
            else if (ctx.RSH()   != null) utility.writeBinaryInstruction(ctx, type, RSH);
            else if (ctx.BWAND() != null) utility.writeBinaryInstruction(ctx, type, BWAND);
            else if (ctx.BWXOR() != null) utility.writeBinaryInstruction(ctx, type, BWXOR);
            else if (ctx.BWOR()  != null) utility.writeBinaryInstruction(ctx, type, BWOR);
            else {
                throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
            }

            caster.checkWriteCast(binaryemd);
        }

        utility.checkWriteBranch(ctx);
    }

    void processComp(final CompContext ctx) {
        final ExpressionMetadata compemd = metadata.getExpressionMetadata(ctx);
        final Object postConst = compemd.postConst;
        final Object preConst = compemd.preConst;
        final Branch branch = utility.getBranch(ctx);

        if (postConst != null) {
            if (branch == null) {
                utility.writeConstant(ctx, postConst);
            } else {
                if ((boolean)postConst && branch.tru != null) {
                    execute.mark(branch.tru);
                } else if (!(boolean)postConst && branch.fals != null) {
                    execute.mark(branch.fals);
                }
            }
        } else if (preConst != null) {
            if (branch == null) {
                utility.writeConstant(ctx, preConst);
                caster.checkWriteCast(compemd);
            } else {
                throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
            }
        } else {
            final ExpressionContext exprctx0 = ctx.expression(0);
            final ExpressionMetadata expremd0 = metadata.getExpressionMetadata(exprctx0);

            final ExpressionContext exprctx1 = ctx.expression(1);
            final ExpressionMetadata expremd1 = metadata.getExpressionMetadata(exprctx1);
            final org.objectweb.asm.Type type = expremd1.to.type;
            final Sort sort1 = expremd1.to.sort;

            writer.visit(exprctx0);

            if (!expremd1.isNull) {
                writer.visit(exprctx1);
            }

            final boolean tru = branch != null && branch.tru != null;
            final boolean fals = branch != null && branch.fals != null;
            final Label jump = tru ? branch.tru : fals ? branch.fals : new Label();
            final Label end = new Label();

            final boolean eq = (ctx.EQ() != null || ctx.EQR() != null) && (tru || !fals) ||
                (ctx.NE() != null || ctx.NER() != null) && fals;
            final boolean ne = (ctx.NE() != null || ctx.NER() != null) && (tru || !fals) ||
                (ctx.EQ() != null || ctx.EQR() != null) && fals;
            final boolean lt  = ctx.LT()  != null && (tru || !fals) || ctx.GTE() != null && fals;
            final boolean lte = ctx.LTE() != null && (tru || !fals) || ctx.GT()  != null && fals;
            final boolean gt  = ctx.GT()  != null && (tru || !fals) || ctx.LTE() != null && fals;
            final boolean gte = ctx.GTE() != null && (tru || !fals) || ctx.LT()  != null && fals;

            boolean writejump = true;

            switch (sort1) {
                case VOID:
                case BYTE:
                case SHORT:
                case CHAR:
                    throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
                case BOOL:
                    if      (eq) execute.ifZCmp(GeneratorAdapter.EQ, jump);
                    else if (ne) execute.ifZCmp(GeneratorAdapter.NE, jump);
                    else {
                        throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
                    }

                    break;
                case INT:
                case LONG:
                case FLOAT:
                case DOUBLE:
                    if      (eq)  execute.ifCmp(type, GeneratorAdapter.EQ, jump);
                    else if (ne)  execute.ifCmp(type, GeneratorAdapter.NE, jump);
                    else if (lt)  execute.ifCmp(type, GeneratorAdapter.LT, jump);
                    else if (lte) execute.ifCmp(type, GeneratorAdapter.LE, jump);
                    else if (gt)  execute.ifCmp(type, GeneratorAdapter.GT, jump);
                    else if (gte) execute.ifCmp(type, GeneratorAdapter.GE, jump);
                    else {
                        throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
                    }

                    break;
                case DEF:
                    if (eq) {
                        if (expremd1.isNull) {
                            execute.ifNull(jump);
                        } else if (!expremd0.isNull && ctx.EQ() != null) {
                            execute.invokeStatic(definition.defobjType.type, DEF_EQ_CALL);
                        } else {
                            execute.ifCmp(type, GeneratorAdapter.EQ, jump);
                        }
                    } else if (ne) {
                        if (expremd1.isNull) {
                            execute.ifNonNull(jump);
                        } else if (!expremd0.isNull && ctx.NE() != null) {
                            execute.invokeStatic(definition.defobjType.type, DEF_EQ_CALL);
                            execute.ifZCmp(GeneratorAdapter.EQ, jump);
                        } else {
                            execute.ifCmp(type, GeneratorAdapter.NE, jump);
                        }
                    } else if (lt) {
                        execute.invokeStatic(definition.defobjType.type, DEF_LT_CALL);
                    } else if (lte) {
                        execute.invokeStatic(definition.defobjType.type, DEF_LTE_CALL);
                    } else if (gt) {
                        execute.invokeStatic(definition.defobjType.type, DEF_GT_CALL);
                    } else if (gte) {
                        execute.invokeStatic(definition.defobjType.type, DEF_GTE_CALL);
                    } else {
                        throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
                    }

                    writejump = expremd1.isNull || ne || ctx.EQR() != null;

                    if (branch != null && !writejump) {
                        execute.ifZCmp(GeneratorAdapter.NE, jump);
                    }

                    break;
                default:
                    if (eq) {
                        if (expremd1.isNull) {
                            execute.ifNull(jump);
                        } else if (ctx.EQ() != null) {
                            execute.invokeStatic(definition.utilityType.type, CHECKEQUALS);

                            if (branch != null) {
                                execute.ifZCmp(GeneratorAdapter.NE, jump);
                            }

                            writejump = false;
                        } else {
                            execute.ifCmp(type, GeneratorAdapter.EQ, jump);
                        }
                    } else if (ne) {
                        if (expremd1.isNull) {
                            execute.ifNonNull(jump);
                        } else if (ctx.NE() != null) {
                            execute.invokeStatic(definition.utilityType.type, CHECKEQUALS);
                            execute.ifZCmp(GeneratorAdapter.EQ, jump);
                        } else {
                            execute.ifCmp(type, GeneratorAdapter.NE, jump);
                        }
                    } else {
                        throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
                    }
            }

            if (branch == null) {
                if (writejump) {
                    execute.push(false);
                    execute.goTo(end);
                    execute.mark(jump);
                    execute.push(true);
                    execute.mark(end);
                }

                caster.checkWriteCast(compemd);
            }
        }
    }

    void processBool(final BoolContext ctx) {
        final ExpressionMetadata boolemd = metadata.getExpressionMetadata(ctx);
        final Object postConst = boolemd.postConst;
        final Object preConst = boolemd.preConst;
        final Branch branch = utility.getBranch(ctx);

        if (postConst != null) {
            if (branch == null) {
                utility.writeConstant(ctx, postConst);
            } else {
                if ((boolean)postConst && branch.tru != null) {
                    execute.mark(branch.tru);
                } else if (!(boolean)postConst && branch.fals != null) {
                    execute.mark(branch.fals);
                }
            }
        } else if (preConst != null) {
            if (branch == null) {
                utility.writeConstant(ctx, preConst);
                caster.checkWriteCast(boolemd);
            } else {
                throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
            }
        } else {
            final ExpressionContext exprctx0 = ctx.expression(0);
            final ExpressionContext exprctx1 = ctx.expression(1);

            if (branch == null) {
                if (ctx.BOOLAND() != null) {
                    final Branch local = utility.markBranch(ctx, exprctx0, exprctx1);
                    local.fals = new Label();
                    final Label end = new Label();

                    writer.visit(exprctx0);
                    writer.visit(exprctx1);

                    execute.push(true);
                    execute.goTo(end);
                    execute.mark(local.fals);
                    execute.push(false);
                    execute.mark(end);
                } else if (ctx.BOOLOR() != null) {
                    final Branch branch0 = utility.markBranch(ctx, exprctx0);
                    branch0.tru = new Label();
                    final Branch branch1 = utility.markBranch(ctx, exprctx1);
                    branch1.fals = new Label();
                    final Label aend = new Label();

                    writer.visit(exprctx0);
                    writer.visit(exprctx1);

                    execute.mark(branch0.tru);
                    execute.push(true);
                    execute.goTo(aend);
                    execute.mark(branch1.fals);
                    execute.push(false);
                    execute.mark(aend);
                } else {
                    throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
                }

                caster.checkWriteCast(boolemd);
            } else {
                if (ctx.BOOLAND() != null) {
                    final Branch branch0 = utility.markBranch(ctx, exprctx0);
                    branch0.fals = branch.fals == null ? new Label() : branch.fals;
                    final Branch branch1 = utility.markBranch(ctx, exprctx1);
                    branch1.tru = branch.tru;
                    branch1.fals = branch.fals;

                    writer.visit(exprctx0);
                    writer.visit(exprctx1);

                    if (branch.fals == null) {
                        execute.mark(branch0.fals);
                    }
                } else if (ctx.BOOLOR() != null) {
                    final Branch branch0 = utility.markBranch(ctx, exprctx0);
                    branch0.tru = branch.tru == null ? new Label() : branch.tru;
                    final Branch branch1 = utility.markBranch(ctx, exprctx1);
                    branch1.tru = branch.tru;
                    branch1.fals = branch.fals;

                    writer.visit(exprctx0);
                    writer.visit(exprctx1);

                    if (branch.tru == null) {
                        execute.mark(branch0.tru);
                    }
                } else {
                    throw new IllegalStateException(WriterUtility.error(ctx) + "Unexpected state.");
                }
            }
        }
    }

    void processConditional(final ConditionalContext ctx) {
        final ExpressionMetadata condemd = metadata.getExpressionMetadata(ctx);
        final Branch branch = utility.getBranch(ctx);

        final ExpressionContext expr0 = ctx.expression(0);
        final ExpressionContext expr1 = ctx.expression(1);
        final ExpressionContext expr2 = ctx.expression(2);

        final Branch local = utility.markBranch(ctx, expr0);
        local.fals = new Label();
        local.end = new Label();

        if (branch != null) {
            utility.copyBranch(branch, expr1, expr2);
        }

        writer.visit(expr0);
        writer.visit(expr1);
        execute.goTo(local.end);
        execute.mark(local.fals);
        writer.visit(expr2);
        execute.mark(local.end);

        if (branch == null) {
            caster.checkWriteCast(condemd);
        }
    }

    void processAssignment(final AssignmentContext ctx) {
        final ExpressionMetadata expremd = metadata.getExpressionMetadata(ctx);
        writer.visit(ctx.extstart());
        caster.checkWriteCast(expremd);
        utility.checkWriteBranch(ctx);
    }

    void processIncrement(final IncrementContext ctx) {
        final ExpressionMetadata incremd = metadata.getExpressionMetadata(ctx);
        final Object postConst = incremd.postConst;

        if (postConst == null) {
            utility.writeNumeric(ctx, incremd.preConst);
            caster.checkWriteCast(incremd);
        } else {
            utility.writeConstant(ctx, postConst);
        }

        utility.checkWriteBranch(ctx);
    }
}
