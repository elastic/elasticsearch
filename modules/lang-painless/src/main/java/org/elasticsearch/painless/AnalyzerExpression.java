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
import org.elasticsearch.painless.Metadata.ExternalMetadata;
import org.elasticsearch.painless.PainlessParser.AssignmentContext;
import org.elasticsearch.painless.PainlessParser.BinaryContext;
import org.elasticsearch.painless.PainlessParser.BoolContext;
import org.elasticsearch.painless.PainlessParser.CastContext;
import org.elasticsearch.painless.PainlessParser.CharContext;
import org.elasticsearch.painless.PainlessParser.CompContext;
import org.elasticsearch.painless.PainlessParser.ConditionalContext;
import org.elasticsearch.painless.PainlessParser.DecltypeContext;
import org.elasticsearch.painless.PainlessParser.ExpressionContext;
import org.elasticsearch.painless.PainlessParser.ExternalContext;
import org.elasticsearch.painless.PainlessParser.ExtstartContext;
import org.elasticsearch.painless.PainlessParser.FalseContext;
import org.elasticsearch.painless.PainlessParser.IncrementContext;
import org.elasticsearch.painless.PainlessParser.NullContext;
import org.elasticsearch.painless.PainlessParser.NumericContext;
import org.elasticsearch.painless.PainlessParser.PostincContext;
import org.elasticsearch.painless.PainlessParser.PreincContext;
import org.elasticsearch.painless.PainlessParser.TrueContext;
import org.elasticsearch.painless.PainlessParser.UnaryContext;

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

class AnalyzerExpression {
    private final Metadata metadata;
    private final Definition definition;
    private final CompilerSettings settings;

    private final Analyzer analyzer;
    private final AnalyzerCaster caster;
    private final AnalyzerPromoter promoter;

    AnalyzerExpression(final Metadata metadata, final Analyzer analyzer,
                       final AnalyzerCaster caster, final AnalyzerPromoter promoter) {
        this.metadata = metadata;
        this.definition = metadata.definition;
        this.settings = metadata.settings;

        this.analyzer = analyzer;
        this.caster = caster;
        this.promoter = promoter;
    }

    void processNumeric(final NumericContext ctx) {
        final ExpressionMetadata numericemd = metadata.getExpressionMetadata(ctx);
        final boolean negate = ctx.parent instanceof UnaryContext && ((UnaryContext)ctx.parent).SUB() != null;

        if (ctx.DECIMAL() != null) {
            final String svalue = (negate ? "-" : "") + ctx.DECIMAL().getText();

            if (svalue.endsWith("f") || svalue.endsWith("F")) {
                try {
                    numericemd.from = definition.floatType;
                    numericemd.preConst = Float.parseFloat(svalue.substring(0, svalue.length() - 1));
                } catch (NumberFormatException exception) {
                    throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Invalid float constant [" + svalue + "].");
                }
            } else {
                try {
                    numericemd.from = definition.doubleType;
                    numericemd.preConst = Double.parseDouble(svalue);
                } catch (NumberFormatException exception) {
                    throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Invalid double constant [" + svalue + "].");
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
                throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
            }

            if (svalue.endsWith("d") || svalue.endsWith("D")) {
                try {
                    numericemd.from = definition.doubleType;
                    numericemd.preConst = Double.parseDouble(svalue.substring(0, svalue.length() - 1));
                } catch (NumberFormatException exception) {
                    throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Invalid float constant [" + svalue + "].");
                }
            } else if (svalue.endsWith("f") || svalue.endsWith("F")) {
                try {
                    numericemd.from = definition.floatType;
                    numericemd.preConst = Float.parseFloat(svalue.substring(0, svalue.length() - 1));
                } catch (NumberFormatException exception) {
                    throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Invalid float constant [" + svalue + "].");
                }
            } else if (svalue.endsWith("l") || svalue.endsWith("L")) {
                try {
                    numericemd.from = definition.longType;
                    numericemd.preConst = Long.parseLong(svalue.substring(0, svalue.length() - 1), radix);
                } catch (NumberFormatException exception) {
                    throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Invalid long constant [" + svalue + "].");
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
                    throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Invalid int constant [" + svalue + "].");
                }
            }
        }
    }

    void processChar(final CharContext ctx) {
        final ExpressionMetadata charemd = metadata.getExpressionMetadata(ctx);

        if (ctx.CHAR() == null) {
            throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
        }

        charemd.preConst = ctx.CHAR().getText().charAt(0);
        charemd.from = definition.charType;
    }

    void processTrue(final TrueContext ctx) {
        final ExpressionMetadata trueemd = metadata.getExpressionMetadata(ctx);

        if (ctx.TRUE() == null) {
            throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
        }

        trueemd.preConst = true;
        trueemd.from = definition.booleanType;
    }

    void processFalse(final FalseContext ctx) {
        final ExpressionMetadata falseemd = metadata.getExpressionMetadata(ctx);

        if (ctx.FALSE() == null) {
            throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
        }

        falseemd.preConst = false;
        falseemd.from = definition.booleanType;
    }

    void processNull(final NullContext ctx) {
        final ExpressionMetadata nullemd = metadata.getExpressionMetadata(ctx);

        if (ctx.NULL() == null) {
            throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
        }

        nullemd.isNull = true;

        if (nullemd.to != null) {
            if (nullemd.to.sort.primitive) {
                throw new IllegalArgumentException(AnalyzerUtility.error(ctx) +
                    "Cannot cast null to a primitive type [" + nullemd.to.name + "].");
            }

            nullemd.from = nullemd.to;
        } else {
            nullemd.from = definition.objectType;
        }
    }

    void processExternal(final ExternalContext ctx) {
        final ExpressionMetadata extemd = metadata.getExpressionMetadata(ctx);

        final ExtstartContext extstartctx = ctx.extstart();
        final ExternalMetadata extstartemd = metadata.createExternalMetadata(extstartctx);
        extstartemd.read = extemd.read;
        analyzer.visit(extstartctx);

        extemd.statement = extstartemd.statement;
        extemd.preConst = extstartemd.constant;
        extemd.from = extstartemd.current;
        extemd.typesafe = extstartemd.current.sort != Sort.DEF;
    }

    void processPostinc(final PostincContext ctx) {
        final ExpressionMetadata postincemd = metadata.getExpressionMetadata(ctx);

        final ExtstartContext extstartctx = ctx.extstart();
        final ExternalMetadata extstartemd = metadata.createExternalMetadata(extstartctx);
        extstartemd.read = postincemd.read;
        extstartemd.storeExpr = ctx.increment();
        extstartemd.token = ADD;
        extstartemd.post = true;
        analyzer.visit(extstartctx);

        postincemd.statement = true;
        postincemd.from = extstartemd.read ? extstartemd.current : definition.voidType;
        postincemd.typesafe = extstartemd.current.sort != Sort.DEF;
    }

    void processPreinc(final PreincContext ctx) {
        final ExpressionMetadata preincemd = metadata.getExpressionMetadata(ctx);

        final ExtstartContext extstartctx = ctx.extstart();
        final ExternalMetadata extstartemd = metadata.createExternalMetadata(extstartctx);
        extstartemd.read = preincemd.read;
        extstartemd.storeExpr = ctx.increment();
        extstartemd.token = ADD;
        extstartemd.pre = true;
        analyzer.visit(extstartctx);

        preincemd.statement = true;
        preincemd.from = extstartemd.read ? extstartemd.current : definition.voidType;
        preincemd.typesafe = extstartemd.current.sort != Sort.DEF;
    }

    void processUnary(final UnaryContext ctx) {
        final ExpressionMetadata unaryemd = metadata.getExpressionMetadata(ctx);

        final ExpressionContext exprctx = AnalyzerUtility.updateExpressionTree(ctx.expression());
        final ExpressionMetadata expremd = metadata.createExpressionMetadata(exprctx);

        if (ctx.BOOLNOT() != null) {
            expremd.to = definition.booleanType;
            analyzer.visit(exprctx);
            caster.markCast(expremd);

            if (expremd.postConst != null) {
                unaryemd.preConst = !(boolean)expremd.postConst;
            }

            unaryemd.from = definition.booleanType;
        } else if (ctx.BWNOT() != null || ctx.ADD() != null || ctx.SUB() != null) {
            analyzer.visit(exprctx);

            final Type promote = promoter.promoteNumeric(expremd.from, ctx.BWNOT() == null, true);

            if (promote == null) {
                throw new ClassCastException(AnalyzerUtility.error(ctx) + "Cannot apply [" + ctx.getChild(0).getText() + "] " +
                    "operation to type [" + expremd.from.name + "].");
            }

            expremd.to = promote;
            caster.markCast(expremd);

            if (expremd.postConst != null) {
                final Sort sort = promote.sort;

                if (ctx.BWNOT() != null) {
                    if (sort == Sort.INT) {
                        unaryemd.preConst = ~(int)expremd.postConst;
                    } else if (sort == Sort.LONG) {
                        unaryemd.preConst = ~(long)expremd.postConst;
                    } else {
                        throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
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
                            throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
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
                        throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
                    }
                } else {
                    throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
                }
            }

            unaryemd.from = promote;
            unaryemd.typesafe = expremd.typesafe;
        } else {
            throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
        }
    }

    void processCast(final CastContext ctx) {
        final ExpressionMetadata castemd = metadata.getExpressionMetadata(ctx);

        final DecltypeContext decltypectx = ctx.decltype();
        final ExpressionMetadata decltypemd = metadata.createExpressionMetadata(decltypectx);
        analyzer.visit(decltypectx);

        final Type type = decltypemd.from;
        castemd.from = type;

        final ExpressionContext exprctx = AnalyzerUtility.updateExpressionTree(ctx.expression());
        final ExpressionMetadata expremd = metadata.createExpressionMetadata(exprctx);
        expremd.to = type;
        expremd.explicit = true;
        analyzer.visit(exprctx);
        caster.markCast(expremd);

        if (expremd.postConst != null) {
            castemd.preConst = expremd.postConst;
        }

        castemd.typesafe = expremd.typesafe && castemd.from.sort != Sort.DEF;
    }

    void processBinary(final BinaryContext ctx) {
        final ExpressionMetadata binaryemd = metadata.getExpressionMetadata(ctx);

        final ExpressionContext exprctx0 = AnalyzerUtility.updateExpressionTree(ctx.expression(0));
        final ExpressionMetadata expremd0 = metadata.createExpressionMetadata(exprctx0);
        analyzer.visit(exprctx0);

        final ExpressionContext exprctx1 = AnalyzerUtility.updateExpressionTree(ctx.expression(1));
        final ExpressionMetadata expremd1 = metadata.createExpressionMetadata(exprctx1);
        analyzer.visit(exprctx1);

        final boolean decimal = ctx.MUL() != null || ctx.DIV() != null || ctx.REM() != null || ctx.SUB() != null;
        final boolean add = ctx.ADD() != null;
        final boolean xor = ctx.BWXOR() != null;
        final Type promote = add ? promoter.promoteAdd(expremd0.from, expremd1.from) :
            xor ? promoter.promoteXor(expremd0.from, expremd1.from) :
                promoter.promoteNumeric(expremd0.from, expremd1.from, decimal, true);

        if (promote == null) {
            throw new ClassCastException(AnalyzerUtility.error(ctx) + "Cannot apply [" + ctx.getChild(1).getText() + "] " +
                "operation to types [" + expremd0.from.name + "] and [" + expremd1.from.name + "].");
        }

        final Sort sort = promote.sort;
        expremd0.to = add && sort == Sort.STRING ? expremd0.from : promote;
        expremd1.to = add && sort == Sort.STRING ? expremd1.from : promote;
        caster.markCast(expremd0);
        caster.markCast(expremd1);

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
                    throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
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
                    throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
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
                    throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
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
                    throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
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
                    throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
                }
            } else if (ctx.LSH() != null) {
                if (sort == Sort.INT) {
                    binaryemd.preConst = (int)expremd0.postConst << (int)expremd1.postConst;
                } else if (sort == Sort.LONG) {
                    binaryemd.preConst = (long)expremd0.postConst << (long)expremd1.postConst;
                } else {
                    throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
                }
            } else if (ctx.RSH() != null) {
                if (sort == Sort.INT) {
                    binaryemd.preConst = (int)expremd0.postConst >> (int)expremd1.postConst;
                } else if (sort == Sort.LONG) {
                    binaryemd.preConst = (long)expremd0.postConst >> (long)expremd1.postConst;
                } else {
                    throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
                }
            } else if (ctx.USH() != null) {
                if (sort == Sort.INT) {
                    binaryemd.preConst = (int)expremd0.postConst >>> (int)expremd1.postConst;
                } else if (sort == Sort.LONG) {
                    binaryemd.preConst = (long)expremd0.postConst >>> (long)expremd1.postConst;
                } else {
                    throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
                }
            } else if (ctx.BWAND() != null) {
                if (sort == Sort.INT) {
                    binaryemd.preConst = (int)expremd0.postConst & (int)expremd1.postConst;
                } else if (sort == Sort.LONG) {
                    binaryemd.preConst = (long)expremd0.postConst & (long)expremd1.postConst;
                } else {
                    throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
                }
            } else if (ctx.BWXOR() != null) {
                if (sort == Sort.BOOL) {
                    binaryemd.preConst = (boolean)expremd0.postConst ^ (boolean)expremd1.postConst;
                } else if (sort == Sort.INT) {
                    binaryemd.preConst = (int)expremd0.postConst ^ (int)expremd1.postConst;
                } else if (sort == Sort.LONG) {
                    binaryemd.preConst = (long)expremd0.postConst ^ (long)expremd1.postConst;
                } else {
                    throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
                }
            } else if (ctx.BWOR() != null) {
                if (sort == Sort.INT) {
                    binaryemd.preConst = (int)expremd0.postConst | (int)expremd1.postConst;
                } else if (sort == Sort.LONG) {
                    binaryemd.preConst = (long)expremd0.postConst | (long)expremd1.postConst;
                } else {
                    throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
                }
            } else {
                throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
            }
        }

        binaryemd.from = promote;
        binaryemd.typesafe = expremd0.typesafe && expremd1.typesafe;
    }

    void processComp(final CompContext ctx) {
        final ExpressionMetadata compemd = metadata.getExpressionMetadata(ctx);
        final boolean equality = ctx.EQ() != null || ctx.NE() != null;
        final boolean reference = ctx.EQR() != null || ctx.NER() != null;

        final ExpressionContext exprctx0 = AnalyzerUtility.updateExpressionTree(ctx.expression(0));
        final ExpressionMetadata expremd0 = metadata.createExpressionMetadata(exprctx0);
        analyzer.visit(exprctx0);

        final ExpressionContext exprctx1 = AnalyzerUtility.updateExpressionTree(ctx.expression(1));
        final ExpressionMetadata expremd1 = metadata.createExpressionMetadata(exprctx1);
        analyzer.visit(exprctx1);

        if (expremd0.isNull && expremd1.isNull) {
            throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Unnecessary comparison of null constants.");
        }

        final Type promote = equality ? promoter.promoteEquality(expremd0.from, expremd1.from) :
            reference ? promoter.promoteReference(expremd0.from, expremd1.from) :
                promoter.promoteNumeric(expremd0.from, expremd1.from, true, true);

        if (promote == null) {
            throw new ClassCastException(AnalyzerUtility.error(ctx) + "Cannot apply [" + ctx.getChild(1).getText() + "] " +
                "operation to types [" + expremd0.from.name + "] and [" + expremd1.from.name + "].");
        }

        expremd0.to = promote;
        expremd1.to = promote;
        caster.markCast(expremd0);
        caster.markCast(expremd1);

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
                    } else {
                        throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
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
                    } else {
                        throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
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
                } else {
                    throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
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
                } else {
                    throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
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
                } else {
                    throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
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
                } else {
                    throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
                }
            } else {
                throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
            }
        }

        compemd.from = definition.booleanType;
        compemd.typesafe = expremd0.typesafe && expremd1.typesafe;
    }

    void processBool(final BoolContext ctx) {
        final ExpressionMetadata boolemd = metadata.getExpressionMetadata(ctx);

        final ExpressionContext exprctx0 = AnalyzerUtility.updateExpressionTree(ctx.expression(0));
        final ExpressionMetadata expremd0 = metadata.createExpressionMetadata(exprctx0);
        expremd0.to = definition.booleanType;
        analyzer.visit(exprctx0);
        caster.markCast(expremd0);

        final ExpressionContext exprctx1 = AnalyzerUtility.updateExpressionTree(ctx.expression(1));
        final ExpressionMetadata expremd1 = metadata.createExpressionMetadata(exprctx1);
        expremd1.to = definition.booleanType;
        analyzer.visit(exprctx1);
        caster.markCast(expremd1);

        if (expremd0.postConst != null && expremd1.postConst != null) {
            if (ctx.BOOLAND() != null) {
                boolemd.preConst = (boolean)expremd0.postConst && (boolean)expremd1.postConst;
            } else if (ctx.BOOLOR() != null) {
                boolemd.preConst = (boolean)expremd0.postConst || (boolean)expremd1.postConst;
            } else {
                throw new IllegalStateException(AnalyzerUtility.error(ctx) + "Unexpected state.");
            }
        }

        boolemd.from = definition.booleanType;
        boolemd.typesafe = expremd0.typesafe && expremd1.typesafe;
    }

    void processConditional(final ConditionalContext ctx) {
        final ExpressionMetadata condemd = metadata.getExpressionMetadata(ctx);

        final ExpressionContext exprctx0 = AnalyzerUtility.updateExpressionTree(ctx.expression(0));
        final ExpressionMetadata expremd0 = metadata.createExpressionMetadata(exprctx0);
        expremd0.to = definition.booleanType;
        analyzer.visit(exprctx0);
        caster.markCast(expremd0);

        if (expremd0.postConst != null) {
            throw new IllegalArgumentException(AnalyzerUtility.error(ctx) + "Unnecessary conditional statement.");
        }

        final ExpressionContext exprctx1 = AnalyzerUtility.updateExpressionTree(ctx.expression(1));
        final ExpressionMetadata expremd1 = metadata.createExpressionMetadata(exprctx1);
        expremd1.to = condemd.to;
        expremd1.explicit = condemd.explicit;
        analyzer.visit(exprctx1);

        final ExpressionContext exprctx2 = AnalyzerUtility.updateExpressionTree(ctx.expression(2));
        final ExpressionMetadata expremd2 = metadata.createExpressionMetadata(exprctx2);
        expremd2.to = condemd.to;
        expremd2.explicit = condemd.explicit;
        analyzer.visit(exprctx2);

        if (condemd.to == null) {
            final Type promote = promoter.promoteConditional(expremd1.from, expremd2.from, expremd1.preConst, expremd2.preConst);

            expremd1.to = promote;
            expremd2.to = promote;
            condemd.from = promote;
        } else {
            condemd.from = condemd.to;
        }

        caster.markCast(expremd1);
        caster.markCast(expremd2);

        condemd.typesafe = expremd0.typesafe && expremd1.typesafe;
    }

    void processAssignment(final AssignmentContext ctx) {
        final ExpressionMetadata assignemd = metadata.getExpressionMetadata(ctx);

        final ExtstartContext extstartctx = ctx.extstart();
        final ExternalMetadata extstartemd = metadata.createExternalMetadata(extstartctx);

        extstartemd.read = assignemd.read;
        extstartemd.storeExpr = AnalyzerUtility.updateExpressionTree(ctx.expression());

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

        analyzer.visit(extstartctx);

        assignemd.statement = true;
        assignemd.from = extstartemd.read ? extstartemd.current : definition.voidType;
        assignemd.typesafe = extstartemd.current.sort != Sort.DEF;
    }

    void processIncrement(final IncrementContext ctx) {
        final ExpressionMetadata incremd = metadata.getExpressionMetadata(ctx);
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
                    break;
                case FLOAT:
                    incremd.preConst = positive ? 1.0F : -1.0F;
                    incremd.from = definition.floatType;
                    break;
                case DOUBLE:
                    incremd.preConst = positive ? 1.0 : -1.0;
                    incremd.from = definition.doubleType;
                    break;
                default:
                    incremd.preConst = positive ? 1 : -1;
                    incremd.from = definition.intType;
            }
        }
    }
}
