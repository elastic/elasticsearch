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

import org.elasticsearch.painless.CompilerSettings;
import org.elasticsearch.painless.Definition;
import org.elasticsearch.painless.Definition.Sort;
import org.elasticsearch.painless.Definition.Type;
import org.elasticsearch.painless.Utility;
import org.elasticsearch.painless.tree.node.Node;

import static org.elasticsearch.painless.tree.node.Type.ACONSTANT;

class AnalyzerExpression {
    private final Definition definition;
    private final CompilerSettings settings;
    private final Analyzer analyzer;
    private final AnalyzerCaster caster;
    private final AnalyzerPromoter promoter;

    AnalyzerExpression(final Definition definition, final CompilerSettings settings,
                       final Analyzer analyzer, final AnalyzerCaster caster, final AnalyzerPromoter promoter) {
        this.definition = definition;
        this.settings = settings;
        this.analyzer = analyzer;
        this.caster = caster;
        this.promoter = promoter;
    }

    void visitNumeric(final Node numeric, final MetadataExpression numericme) {
        if (numeric.data.containsKey("decimal")) {
            final String svalue = (String)numeric.data.get("decimal");

            if (svalue.endsWith("f") || svalue.endsWith("F")) {
                try {
                    numericme.constant = Float.parseFloat(svalue.substring(0, svalue.length() - 1));
                    numericme.actual = definition.floatType;
                } catch (final NumberFormatException exception) {
                    throw new IllegalArgumentException(numeric.error("Invalid float constant [" + svalue + "]."));
                }
            } else {
                try {
                    numericme.constant = Double.parseDouble(svalue);
                    numericme.actual = definition.doubleType;
                } catch (final NumberFormatException exception) {
                    throw new IllegalArgumentException(numeric.error("Invalid double constant [" + svalue + "]."));
                }
            }
        } else {
            final String svalue;
            final int radix;

            if (numeric.data.containsKey("octal")) {
                svalue = (String)numeric.data.get("octal");
                radix = 8;
            } else if (numeric.data.containsKey("integer")) {
                svalue = (String)numeric.data.get("integer");
                radix = 10;
            } else if (numeric.data.containsKey("hex")) {
                svalue = (String)numeric.data.get("hex");
                radix = 16;
            } else {
                throw new IllegalStateException(numeric.error("Unexpected state."));
            }

            if (svalue.endsWith("d") || svalue.endsWith("D")) {
                try {
                    numericme.constant = Double.parseDouble(svalue.substring(0, svalue.length() - 1));
                    numericme.actual = definition.doubleType;
                } catch (final NumberFormatException exception) {
                    throw new IllegalArgumentException(numeric.error("Invalid double constant [" + svalue + "]."));
                }
            } else if (svalue.endsWith("f") || svalue.endsWith("F")) {
                try {
                    numericme.constant = Float.parseFloat(svalue.substring(0, svalue.length() - 1));
                    numericme.actual = definition.floatType;
                } catch (final NumberFormatException exception) {
                    throw new IllegalArgumentException(numeric.error("Invalid float constant [" + svalue + "]."));
                }
            } else if (svalue.endsWith("l") || svalue.endsWith("L")) {
                try {
                    numericme.constant = Long.parseLong(svalue.substring(0, svalue.length() - 1), radix);
                    numericme.actual = definition.longType;
                } catch (final NumberFormatException exception) {
                    throw new IllegalArgumentException(numeric.error("Invalid long constant [" + svalue + "]."));
                }
            } else {
                try {
                    final Sort sort = numericme.expected == null ? Sort.INT : numericme.expected.sort;
                    final int value = Integer.parseInt(svalue, radix);

                    if (sort == Sort.BYTE && value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
                        numericme.constant = (byte)value;
                        numericme.actual = definition.byteType;
                    } else if (sort == Sort.CHAR && value >= Character.MIN_VALUE && value <= Character.MAX_VALUE) {
                        numericme.constant = (char)value;
                        numericme.actual = definition.charType;
                    } else if (sort == Sort.SHORT && value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
                        numericme.constant = (short)value;
                        numericme.actual = definition.shortType;
                    } else {
                        numericme.constant = value;
                        numericme.actual = definition.intType;
                    }
                } catch (final NumberFormatException exception) {
                    throw new IllegalArgumentException(numeric.error("Invalid int constant [" + svalue + "]."));
                }
            }
        }
    }

    void visitChar(final Node chr, final MetadataExpression chrme) {
        if (!chr.data.containsKey("char")) {
            throw new IllegalStateException(chr.error("Unexpected state."));
        }

        chrme.constant = chr.data.get("char");
        chrme.actual = definition.charType;
    }

    void visitTrue(final Node tru, final MetadataExpression trume) {
        trume.constant = true;
        trume.actual = definition.booleanType;
    }

    void visitFalse(final Node fals, final MetadataExpression falsme) {
        falsme.constant = false;
        falsme.actual = definition.booleanType;
    }

    void visitNull(final Node nul, final MetadataExpression nulme) {
        nulme.isNull = true;

        if (nulme.expected != null) {
            if (nulme.expected.sort.primitive) {
                throw new IllegalArgumentException(nul.error("Cannot cast null to a primitive type [" + nulme.expected.name + "]."));
            }

            nulme.actual = nulme.expected;
        } else {
            nulme.actual = definition.objectType;
        }
    }

    void visitCast(final Node node, final MetadataExpression nodeme) {
        final String typestr = (String)node.data.get("type");
        final Type type;

        try {
            type = definition.getType(typestr);
        } catch (final IllegalArgumentException exception) {
            throw new IllegalArgumentException(node.error("Not a type [" + typestr + "]."));
        }

        final Node expression = node.children.get(0);
        final MetadataExpression expressionme = new MetadataExpression();

        expressionme.expected = type;
        expressionme.explicit = true;
        analyzer.visit(expression, expressionme);
        final Node cast = caster.markCast(expression, expressionme);

        if (cast.type == ACONSTANT) {
            nodeme.constant = cast.data.get("constant");
        } else {
            node.children.set(0, cast);
        }

        nodeme.actual = type;
        nodeme.typesafe = expressionme.typesafe && type.sort != Sort.DEF;
    }

    void visitUnaryBoolNot(final Node unary, final MetadataExpression unaryme) {
        final Node expression = unary.children.get(0);
        final MetadataExpression expressionme = new MetadataExpression();

        expressionme.actual = definition.booleanType;
        analyzer.visit(expression, expressionme);
        final Node cast = caster.markCast(expression, expressionme);

        if (cast.type == ACONSTANT) {
            unaryme.constant = !(boolean)cast.data.get("constant");
        } else {
            unary.children.set(0, cast);
        }

        unaryme.actual = definition.booleanType;
    }

    void visitUnaryBwNot(final Node unary, final MetadataExpression unaryme) {
        final Node expression = unary.children.get(0);
        final MetadataExpression expressionme = new MetadataExpression();

        analyzer.visit(expression, expressionme);

        final Type promote = promoter.promoteNumeric(expressionme.actual, false, true);

        if (promote == null) {
            throw new ClassCastException(unary.error("Cannot apply bitwise not [~] to type [" + expressionme.actual.name + "]."));
        }

        expressionme.expected = promote;
        final Node cast = caster.markCast(expression, expressionme);

        if (cast.type == ACONSTANT) {
            final Sort sort = promote.sort;
            final Object constant = cast.data.get("constant");

            if (sort == Sort.INT) {
                unaryme.constant = ~(int)constant;
            } else if (sort == Sort.LONG) {
                unaryme.constant = ~(long)constant;
            } else {
                throw new IllegalStateException(unary.error("Unexpected state."));
            }
        } else {
            unary.children.set(0, cast);
        }

        unaryme.actual = promote;
        unaryme.typesafe = expressionme.typesafe;
    }

    void visitUnaryAdd(final Node unary, final MetadataExpression unaryme) {
        final Node expression = unary.children.get(0);
        final MetadataExpression expressionme = new MetadataExpression();

        analyzer.visit(expression, expressionme);

        final Type promote = promoter.promoteNumeric(expressionme.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(unary.error("Cannot apply positive [+] to type [" + expressionme.actual.name + "]."));
        }

        expressionme.expected = promote;
        final Node cast = caster.markCast(expression, expressionme);

        if (cast.type == ACONSTANT) {
            final Sort sort = promote.sort;
            final Object constant = cast.data.get("constant");

            if (sort == Sort.INT) {
                unaryme.constant = +(int)constant;
            } else if (sort == Sort.LONG) {
                unaryme.constant = +(long)constant;
            } else if (sort == Sort.FLOAT) {
                unaryme.constant = +(float)constant;
            } else if (sort == Sort.DOUBLE) {
                unaryme.constant = +(double)constant;
            } else {
                throw new IllegalStateException(unary.error("Unexpected state."));
            }
        } else {
            unary.children.set(0, cast);
        }

        unaryme.actual = promote;
        unaryme.typesafe = expressionme.typesafe;
    }

    void visitUnarySub(final Node unary, final MetadataExpression unaryme) {
        final Node expression = unary.children.get(0);
        final MetadataExpression expressionme = new MetadataExpression();

        analyzer.visit(expression, expressionme);

        final Type promote = promoter.promoteNumeric(expressionme.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(unary.error("Cannot apply negate [-] to type [" + expressionme.actual.name + "]."));
        }

        expressionme.expected = promote;
        final Node cast = caster.markCast(expression, expressionme);

        if (cast.type == ACONSTANT) {
            final boolean overflow = settings.getNumericOverflow();
            final Sort sort = promote.sort;
            final Object constant = cast.data.get("constant");

            if (sort == Sort.INT) {
                unaryme.constant = overflow ? -(int)constant : Math.negateExact((int)constant);
            } else if (sort == Sort.LONG) {
                unaryme.constant = overflow ? -(long)constant : Math.negateExact((long)constant);
            } else if (sort == Sort.FLOAT) {
                unaryme.constant = -(float)constant;
            } else if (sort == Sort.DOUBLE) {
                unaryme.constant = -(double)constant;
            } else {
                throw new IllegalStateException(unary.error("Unexpected state."));
            }
        } else {
            unary.children.set(0, cast);
        }

        unaryme.actual = promote;
        unaryme.typesafe = expressionme.typesafe;
    }

    void visitBinaryMul(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        analyzer.visit(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        analyzer.visit(right, rightme);

        final Type promote = promoter.promoteNumeric(leftme.actual, rightme.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(binary.error("Cannot apply multiply [*] to types " +
                "[" + leftme.actual.name + "] and [" + rightme.actual.name + "]."));
        }

        leftme.expected = promote;
        rightme.expected = promote;

        final Node lcast = caster.markCast(left, leftme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            final boolean overflow = settings.getNumericOverflow();
            final Sort sort = promote.sort;
            final Object lconstant = left.data.get("constant");
            final Object rconstant = right.data.get("constant");

            if (sort == Sort.INT) {
                binaryme.constant = overflow ? (int)lconstant * (int)rconstant : Math.multiplyExact((int)lconstant, (int)rconstant);
            } else if (sort == Sort.LONG) {
                binaryme.constant = overflow ? (long)lconstant * (long)rconstant : Math.multiplyExact((long)lconstant, (long)rconstant);
            } else if (sort == Sort.FLOAT) {
                binaryme.constant = overflow ? (float)lconstant * (float)rconstant :
                    Utility.multiplyWithoutOverflow((float)lconstant, (float)rconstant);
            } else if (sort == Sort.DOUBLE) {
                binaryme.constant = overflow ? (double)lconstant * (double)rconstant :
                    Utility.multiplyWithoutOverflow((double)lconstant, (double)rconstant);
            } else {
                throw new IllegalStateException(binary.error("Unexpected state."));
            }
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = promote;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitBinaryDiv(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        analyzer.visit(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        analyzer.visit(right, rightme);

        final Type promote = promoter.promoteNumeric(leftme.actual, rightme.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(binary.error("Cannot apply divide [/] to types " +
                "[" + leftme.actual.name + "] and [" + rightme.actual.name + "]."));
        }

        leftme.expected = promote;
        rightme.expected = promote;

        final Node lcast = caster.markCast(left, leftme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            final boolean overflow = settings.getNumericOverflow();
            final Sort sort = promote.sort;
            final Object lconstant = left.data.get("constant");
            final Object rconstant = right.data.get("constant");

            if (sort == Sort.INT) {
                binaryme.constant = overflow ? (int)lconstant / (int)rconstant :
                    Utility.divideWithoutOverflow((int)lconstant, (int)rconstant);
            } else if (sort == Sort.LONG) {
                binaryme.constant = overflow ? (long)lconstant / (long)rconstant :
                    Utility.divideWithoutOverflow((long)lconstant, (long)rconstant);
            } else if (sort == Sort.FLOAT) {
                binaryme.constant = overflow ? (float)lconstant / (float)rconstant :
                    Utility.divideWithoutOverflow((float)lconstant, (float)rconstant);
            } else if (sort == Sort.DOUBLE) {
                binaryme.constant = overflow ? (double)lconstant / (double)rconstant :
                    Utility.divideWithoutOverflow((double)lconstant, (double)rconstant);
            } else {
                throw new IllegalStateException(binary.error("Unexpected state."));
            }
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = promote;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitBinaryRem(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        analyzer.visit(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        analyzer.visit(right, rightme);

        final Type promote = promoter.promoteNumeric(leftme.actual, rightme.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(binary.error("Cannot apply remainder [%] to types " +
                "[" + leftme.actual.name + "] and [" + rightme.actual.name + "]."));
        }

        leftme.expected = promote;
        rightme.expected = promote;

        final Node lcast = caster.markCast(left, leftme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            final boolean overflow = settings.getNumericOverflow();
            final Sort sort = promote.sort;
            final Object lconstant = left.data.get("constant");
            final Object rconstant = right.data.get("constant");

            if (sort == Sort.INT) {
                binaryme.constant = (int)lconstant % (int)rconstant;
            } else if (sort == Sort.LONG) {
                binaryme.constant = (long)lconstant % (long)rconstant;
            } else if (sort == Sort.FLOAT) {
                binaryme.constant = overflow ? (float)lconstant % (float)rconstant :
                    Utility.remainderWithoutOverflow((float)lconstant, (float)rconstant);
            } else if (sort == Sort.DOUBLE) {
                binaryme.constant = overflow ? (double)lconstant % (double)rconstant :
                    Utility.remainderWithoutOverflow((double)lconstant, (double)rconstant);
            } else {
                throw new IllegalStateException(binary.error("Unexpected state."));
            }
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = promote;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitBinaryAdd(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        analyzer.visit(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        analyzer.visit(right, rightme);

        final Type promote = promoter.promoteAdd(leftme.actual, rightme.actual);

        if (promote == null) {
            throw new ClassCastException(binary.error("Cannot apply add [+] to types " +
                "[" + leftme.actual.name + "] and [" + rightme.actual.name + "]."));
        }

        final Sort sort = promote.sort;

        leftme.expected = sort == Sort.STRING ? leftme.actual : promote;
        rightme.expected = sort == Sort.STRING ? rightme.actual : promote;

        final Node lcast = caster.markCast(left, leftme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            final boolean overflow = settings.getNumericOverflow();
            final Object lconstant = left.data.get("constant");
            final Object rconstant = right.data.get("constant");

            if (sort == Sort.INT) {
                binaryme.constant = overflow ? (int)lconstant + (int)rconstant : Math.addExact((int)lconstant, (int)rconstant);
            } else if (sort == Sort.LONG) {
                binaryme.constant = overflow ? (long)lconstant + (long)rconstant : Math.addExact((long)lconstant, (long)rconstant);
            } else if (sort == Sort.FLOAT) {
                binaryme.constant = overflow ? (float)lconstant + (float)rconstant :
                    Utility.addWithoutOverflow((float)lconstant, (float)rconstant);
            } else if (sort == Sort.DOUBLE) {
                binaryme.constant = overflow ? (double)lconstant + (double)rconstant :
                    Utility.addWithoutOverflow((double)lconstant, (double)rconstant);
            } else if (sort == Sort.STRING) {
                binaryme.constant = "" + lconstant + rconstant;
            } else {
                throw new IllegalStateException(binary.error("Unexpected state."));
            }
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = promote;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitBinarySub(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        analyzer.visit(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        analyzer.visit(right, rightme);

        final Type promote = promoter.promoteNumeric(leftme.actual, rightme.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(binary.error("Cannot apply subtract [-] to types " +
                "[" + leftme.actual.name + "] and [" + rightme.actual.name + "]."));
        }

        leftme.expected = promote;
        rightme.expected = promote;

        final Node lcast = caster.markCast(left, leftme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            final boolean overflow = settings.getNumericOverflow();
            final Sort sort = promote.sort;
            final Object lconstant = left.data.get("constant");
            final Object rconstant = right.data.get("constant");

            if (sort == Sort.INT) {
                binaryme.constant = overflow ? (int)lconstant - (int)rconstant : Math.subtractExact((int)lconstant, (int)rconstant);
            } else if (sort == Sort.LONG) {
                binaryme.constant = overflow ? (long)lconstant - (long)rconstant : Math.subtractExact((long)lconstant, (long)rconstant);
            } else if (sort == Sort.FLOAT) {
                binaryme.constant = overflow ? (float)lconstant - (float)rconstant :
                    Utility.subtractWithoutOverflow((float)lconstant, (float)rconstant);
            } else if (sort == Sort.DOUBLE) {
                binaryme.constant = overflow ? (double)lconstant - (double)rconstant :
                    Utility.subtractWithoutOverflow((double)lconstant, (double)rconstant);
            } else {
                throw new IllegalStateException(binary.error("Unexpected state."));
            }
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = promote;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitBinaryLeftShift(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        analyzer.visit(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        analyzer.visit(right, rightme);

        final Type promote = promoter.promoteNumeric(leftme.actual, false, true);

        if (promote == null) {
            throw new ClassCastException(binary.error("Cannot apply left shift [<<] to types " +
                "[" + leftme.actual.name + "] and [" + rightme.actual.name + "]."));
        }

        leftme.expected = promote;
        rightme.explicit = true;
        rightme.expected = definition.intType;

        final Node lcast = caster.markCast(left, leftme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            final Sort sort = promote.sort;
            final Object lconstant = left.data.get("constant");
            final Object rconstant = right.data.get("constant");

            if (sort == Sort.INT) {
                binaryme.constant = (int)lconstant << (int)rconstant;
            } else if (sort == Sort.LONG) {
                binaryme.constant = (long)lconstant << (long)rconstant;
            } else {
                throw new IllegalStateException(binary.error("Unexpected state."));
            }
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = promote;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitBinaryRightShift(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        analyzer.visit(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        analyzer.visit(right, rightme);

        final Type promote = promoter.promoteNumeric(leftme.actual, false, true);

        if (promote == null) {
            throw new ClassCastException(binary.error("Cannot apply right shift [>>] to types " +
                "[" + leftme.actual.name + "] and [" + rightme.actual.name + "]."));
        }

        leftme.expected = promote;
        rightme.explicit = true;
        rightme.expected = definition.intType;

        final Node lcast = caster.markCast(left, leftme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            final Sort sort = promote.sort;
            final Object lconstant = left.data.get("constant");
            final Object rconstant = right.data.get("constant");

            if (sort == Sort.INT) {
                binaryme.constant = (int)lconstant >> (int)rconstant;
            } else if (sort == Sort.LONG) {
                binaryme.constant = (long)lconstant >> (long)rconstant;
            } else {
                throw new IllegalStateException(binary.error("Unexpected state."));
            }
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = promote;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitBinaryUnsignedShift(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        analyzer.visit(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        analyzer.visit(right, rightme);

        final Type promote = promoter.promoteNumeric(leftme.actual, false, true);

        if (promote == null) {
            throw new ClassCastException(binary.error("Cannot apply unsigned shift [>>>] to types " +
                "[" + leftme.actual.name + "] and [" + rightme.actual.name + "]."));
        }

        leftme.expected = promote;
        rightme.explicit = true;
        rightme.expected = definition.intType;

        final Node lcast = caster.markCast(left, leftme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            final Sort sort = promote.sort;
            final Object lconstant = left.data.get("constant");
            final Object rconstant = right.data.get("constant");

            if (sort == Sort.INT) {
                binaryme.constant = (int)lconstant >>> (int)rconstant;
            } else if (sort == Sort.LONG) {
                binaryme.constant = (long)lconstant >>> (long)rconstant;
            } else {
                throw new IllegalStateException(binary.error("Unexpected state."));
            }
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = promote;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitBinaryBitwiseAnd(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        analyzer.visit(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        analyzer.visit(right, rightme);

        final Type promote = promoter.promoteNumeric(leftme.actual, rightme.actual, false, true);

        if (promote == null) {
            throw new ClassCastException(binary.error("Cannot apply bitwise and [&] to types " +
                "[" + leftme.actual.name + "] and [" + rightme.actual.name + "]."));
        }

        leftme.expected = promote;
        rightme.expected = promote;

        final Node lcast = caster.markCast(left, leftme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            final Sort sort = promote.sort;
            final Object lconstant = left.data.get("constant");
            final Object rconstant = right.data.get("constant");

            if (sort == Sort.INT) {
                binaryme.constant = (int)lconstant & (int)rconstant;
            } else if (sort == Sort.LONG) {
                binaryme.constant = (long)lconstant & (long)rconstant;
            } else {
                throw new IllegalStateException(binary.error("Unexpected state."));
            }
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = promote;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitBinaryXor(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        analyzer.visit(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        analyzer.visit(right, rightme);

        final Type promote = promoter.promoteXor(leftme.actual, rightme.actual);

        if (promote == null) {
            throw new ClassCastException(binary.error("Cannot apply bitwise xor [^] to types " +
                "[" + leftme.actual.name + "] and [" + rightme.actual.name + "]."));
        }

        leftme.expected = promote;
        rightme.expected = promote;

        final Node lcast = caster.markCast(left, leftme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            final Sort sort = promote.sort;
            final Object lconstant = left.data.get("constant");
            final Object rconstant = right.data.get("constant");

            if (sort == Sort.BOOL) {
                binaryme.constant = (boolean)lconstant ^ (boolean)rconstant;
            } else if (sort == Sort.INT) {
                binaryme.constant = (int)lconstant ^ (int)rconstant;
            } else if (sort == Sort.LONG) {
                binaryme.constant = (long)lconstant ^ (long)rconstant;
            } else {
                throw new IllegalStateException(binary.error("Unexpected state."));
            }
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = promote;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitBinaryBitwiseOr(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        analyzer.visit(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        analyzer.visit(right, rightme);

        final Type promote = promoter.promoteNumeric(leftme.actual, rightme.actual, false, true);

        if (promote == null) {
            throw new ClassCastException(binary.error("Cannot apply bitwise or [|] to types " +
                "[" + leftme.actual.name + "] and [" + rightme.actual.name + "]."));
        }

        leftme.expected = promote;
        rightme.expected = promote;

        final Node lcast = caster.markCast(left, leftme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            final Sort sort = promote.sort;
            final Object lconstant = left.data.get("constant");
            final Object rconstant = right.data.get("constant");

            if (sort == Sort.INT) {
                binaryme.constant = (int)lconstant | (int)rconstant;
            } else if (sort == Sort.LONG) {
                binaryme.constant = (long)lconstant | (long)rconstant;
            } else {
                throw new IllegalStateException(binary.error("Unexpected state."));
            }
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = promote;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitBinaryEquals(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        analyzer.visit(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        analyzer.visit(right, rightme);

        if (leftme.isNull && rightme.isNull) {
            throw new IllegalArgumentException(binary.error("Extraneous comparison of null constants."));
        }

        final Type promote = promoter.promoteEquality(leftme.actual, rightme.actual);

        if (promote == null) {
            throw new ClassCastException(binary.error("Cannot apply equals [==] to types " +
                "[" + leftme.actual.name + "] and [" + rightme.actual.name + "]."));
        }

        leftme.expected = promote;
        rightme.expected = promote;

        final Node lcast = caster.markCast(left, leftme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            final Sort sort = promote.sort;
            final Object lconstant = left.data.get("constant");
            final Object rconstant = right.data.get("constant");

            if (sort == Sort.BOOL) {
                binaryme.constant = (boolean)lconstant == (boolean)rconstant;
            } else if (sort == Sort.INT) {
                binaryme.constant = (int)lconstant == (int)rconstant;
            } else if (sort == Sort.LONG) {
                binaryme.constant = (long)lconstant == (long)rconstant;
            } else if (sort == Sort.FLOAT) {
                binaryme.constant = (float)lconstant == (float)rconstant;
            } else if (sort == Sort.DOUBLE) {
                binaryme.constant = (double)lconstant == (double)rconstant;
            } else if (!leftme.isNull) {
                binaryme.constant = lconstant.equals(rconstant);
            } else if (!rightme.isNull) {
                binaryme.constant = rconstant.equals(null);
            } else {
                throw new IllegalStateException(binary.error("Unexpected state."));
            }
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = promote;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitBinaryRefEquals(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        analyzer.visit(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        analyzer.visit(right, rightme);

        if (leftme.isNull && rightme.isNull) {
            throw new IllegalArgumentException(binary.error("Extraneous comparison of null constants."));
        }

        final Type promote = promoter.promoteReference(leftme.actual, rightme.actual);

        if (promote == null) {
            throw new ClassCastException(binary.error("Cannot apply reference equals [===] to types " +
                "[" + leftme.actual.name + "] and [" + rightme.actual.name + "]."));
        }

        leftme.expected = promote;
        rightme.expected = promote;

        final Node lcast = caster.markCast(left, leftme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            final Sort sort = promote.sort;
            final Object lconstant = left.data.get("constant");
            final Object rconstant = right.data.get("constant");

            if (sort == Sort.BOOL) {
                binaryme.constant = (boolean)lconstant == (boolean)rconstant;
            } else if (sort == Sort.INT) {
                binaryme.constant = (int)lconstant == (int)rconstant;
            } else if (sort == Sort.LONG) {
                binaryme.constant = (long)lconstant == (long)rconstant;
            } else if (sort == Sort.FLOAT) {
                binaryme.constant = (float)lconstant == (float)rconstant;
            } else if (sort == Sort.DOUBLE) {
                binaryme.constant = (double)lconstant == (double)rconstant;
            } else {
                binaryme.constant = lconstant == rconstant;
            }
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = promote;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitBinaryNotEquals(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        analyzer.visit(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        analyzer.visit(right, rightme);

        if (leftme.isNull && rightme.isNull) {
            throw new IllegalArgumentException(binary.error("Extraneous comparison of null constants."));
        }

        final Type promote = promoter.promoteEquality(leftme.actual, rightme.actual);

        if (promote == null) {
            throw new ClassCastException(binary.error("Cannot apply not equals [!=] to types " +
                "[" + leftme.actual.name + "] and [" + rightme.actual.name + "]."));
        }

        leftme.expected = promote;
        rightme.expected = promote;

        final Node lcast = caster.markCast(left, leftme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            final Sort sort = promote.sort;
            final Object lconstant = left.data.get("constant");
            final Object rconstant = right.data.get("constant");

            if (sort == Sort.BOOL) {
                binaryme.constant = (boolean)lconstant != (boolean)rconstant;
            } else if (sort == Sort.INT) {
                binaryme.constant = (int)lconstant != (int)rconstant;
            } else if (sort == Sort.LONG) {
                binaryme.constant = (long)lconstant != (long)rconstant;
            } else if (sort == Sort.FLOAT) {
                binaryme.constant = (float)lconstant != (float)rconstant;
            } else if (sort == Sort.DOUBLE) {
                binaryme.constant = (double)lconstant != (double)rconstant;
            } else if (!leftme.isNull) {
                binaryme.constant = !lconstant.equals(rconstant);
            } else if (!rightme.isNull) {
                binaryme.constant = !rconstant.equals(null);
            } else {
                throw new IllegalStateException(binary.error("Unexpected state."));
            }
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = promote;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitBinaryRefNotEquals(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        analyzer.visit(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        analyzer.visit(right, rightme);

        if (leftme.isNull && rightme.isNull) {
            throw new IllegalArgumentException(binary.error("Extraneous comparison of null constants."));
        }

        final Type promote = promoter.promoteReference(leftme.actual, rightme.actual);

        if (promote == null) {
            throw new ClassCastException(binary.error("Cannot apply not reference equals [!==] to types " +
                "[" + leftme.actual.name + "] and [" + rightme.actual.name + "]."));
        }

        leftme.expected = promote;
        rightme.expected = promote;

        final Node lcast = caster.markCast(left, leftme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            final Sort sort = promote.sort;
            final Object lconstant = left.data.get("constant");
            final Object rconstant = right.data.get("constant");

            if (sort == Sort.BOOL) {
                binaryme.constant = (boolean)lconstant != (boolean)rconstant;
            } else if (sort == Sort.INT) {
                binaryme.constant = (int)lconstant != (int)rconstant;
            } else if (sort == Sort.LONG) {
                binaryme.constant = (long)lconstant != (long)rconstant;
            } else if (sort == Sort.FLOAT) {
                binaryme.constant = (float)lconstant != (float)rconstant;
            } else if (sort == Sort.DOUBLE) {
                binaryme.constant = (double)lconstant != (double)rconstant;
            } else {
                binaryme.constant = lconstant != rconstant;
            }
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = promote;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitBinaryGreaterEquals(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        analyzer.visit(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        analyzer.visit(right, rightme);

        final Type promote = promoter.promoteNumeric(leftme.actual, rightme.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(binary.error("Cannot apply greater equals [>=] to types " +
                "[" + leftme.actual.name + "] and [" + rightme.actual.name + "]."));
        }

        leftme.expected = promote;
        rightme.expected = promote;

        final Node lcast = caster.markCast(left, leftme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            final Sort sort = promote.sort;
            final Object lconstant = left.data.get("constant");
            final Object rconstant = right.data.get("constant");

            if (sort == Sort.INT) {
                binaryme.constant = (int)lconstant >= (int)rconstant;
            } else if (sort == Sort.LONG) {
                binaryme.constant = (long)lconstant >= (long)rconstant;
            } else if (sort == Sort.FLOAT) {
                binaryme.constant = (float)lconstant >= (float)rconstant;
            } else if (sort == Sort.DOUBLE) {
                binaryme.constant = (double)lconstant >= (double)rconstant;
            } else {
                throw new IllegalStateException(binary.error("Unexpected state."));
            }
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = promote;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitBinaryGreater(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        analyzer.visit(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        analyzer.visit(right, rightme);

        final Type promote = promoter.promoteNumeric(leftme.actual, rightme.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(binary.error("Cannot apply greater [>] to types " +
                "[" + leftme.actual.name + "] and [" + rightme.actual.name + "]."));
        }

        leftme.expected = promote;
        rightme.expected = promote;

        final Node lcast = caster.markCast(left, leftme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            final Sort sort = promote.sort;
            final Object lconstant = left.data.get("constant");
            final Object rconstant = right.data.get("constant");

            if (sort == Sort.INT) {
                binaryme.constant = (int)lconstant > (int)rconstant;
            } else if (sort == Sort.LONG) {
                binaryme.constant = (long)lconstant > (long)rconstant;
            } else if (sort == Sort.FLOAT) {
                binaryme.constant = (float)lconstant > (float)rconstant;
            } else if (sort == Sort.DOUBLE) {
                binaryme.constant = (double)lconstant > (double)rconstant;
            } else {
                throw new IllegalStateException(binary.error("Unexpected state."));
            }
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = promote;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitBinaryLessEquals(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        analyzer.visit(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        analyzer.visit(right, rightme);

        final Type promote = promoter.promoteNumeric(leftme.actual, rightme.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(binary.error("Cannot apply less equals [<=] to types " +
                "[" + leftme.actual.name + "] and [" + rightme.actual.name + "]."));
        }

        leftme.expected = promote;
        rightme.expected = promote;

        final Node lcast = caster.markCast(left, leftme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            final Sort sort = promote.sort;
            final Object lconstant = left.data.get("constant");
            final Object rconstant = right.data.get("constant");

            if (sort == Sort.INT) {
                binaryme.constant = (int)lconstant <= (int)rconstant;
            } else if (sort == Sort.LONG) {
                binaryme.constant = (long)lconstant <= (long)rconstant;
            } else if (sort == Sort.FLOAT) {
                binaryme.constant = (float)lconstant <= (float)rconstant;
            } else if (sort == Sort.DOUBLE) {
                binaryme.constant = (double)lconstant <= (double)rconstant;
            } else {
                throw new IllegalStateException(binary.error("Unexpected state."));
            }
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = promote;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitBinaryLess(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        analyzer.visit(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        analyzer.visit(right, rightme);

        final Type promote = promoter.promoteNumeric(leftme.actual, rightme.actual, true, true);

        if (promote == null) {
            throw new ClassCastException(binary.error("Cannot apply less [<] to types " +
                "[" + leftme.actual.name + "] and [" + rightme.actual.name + "]."));
        }

        leftme.expected = promote;
        rightme.expected = promote;

        final Node lcast = caster.markCast(left, leftme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            final Sort sort = promote.sort;
            final Object lconstant = left.data.get("constant");
            final Object rconstant = right.data.get("constant");

            if (sort == Sort.INT) {
                binaryme.constant = (int)lconstant < (int)rconstant;
            } else if (sort == Sort.LONG) {
                binaryme.constant = (long)lconstant < (long)rconstant;
            } else if (sort == Sort.FLOAT) {
                binaryme.constant = (float)lconstant < (float)rconstant;
            } else if (sort == Sort.DOUBLE) {
                binaryme.constant = (double)lconstant < (double)rconstant;
            } else {
                throw new IllegalStateException(binary.error("Unexpected state."));
            }
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = promote;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitBinaryBoolAnd(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        leftme.expected = definition.booleanType;
        analyzer.visit(left, leftme);
        final Node lcast = caster.markCast(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        rightme.expected = definition.booleanType;
        analyzer.visit(right, rightme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            binaryme.constant = (boolean)lcast.data.get("constant") && (boolean)rcast.data.get("constant");
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = definition.booleanType;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitBinaryBoolOr(final Node binary, final MetadataExpression binaryme) {
        final Node left = binary.children.get(0);
        final MetadataExpression leftme = new MetadataExpression();

        leftme.expected = definition.booleanType;
        analyzer.visit(left, leftme);
        final Node lcast = caster.markCast(left, leftme);

        final Node right = binary.children.get(1);
        final MetadataExpression rightme = new MetadataExpression();

        rightme.expected = definition.booleanType;
        analyzer.visit(right, rightme);
        final Node rcast = caster.markCast(right, rightme);

        if (lcast.type == ACONSTANT && rcast.type == ACONSTANT) {
            binaryme.constant = (boolean)lcast.data.get("constant") && (boolean)rcast.data.get("constant");
        } else {
            binary.children.set(0, lcast);
            binary.children.set(1, rcast);
        }

        binaryme.actual = definition.booleanType;
        binaryme.typesafe = leftme.typesafe && rightme.typesafe;
    }

    void visitConditional(final Node conditional, final MetadataExpression conditionalme) {
        final Node expression = conditional.children.get(0);
        final MetadataExpression expressionme = new MetadataExpression();

        expressionme.expected = definition.booleanType;
        analyzer.visit(expression, expressionme);
        final Node cast = caster.markCast(expression, expressionme);

        if (cast.type == ACONSTANT) {
            throw new IllegalArgumentException(conditional.error("Extraneous conditional statement."));
        } else {
            conditional.children.set(0, cast);
        }

        final Node left = conditional.children.get(1);
        final MetadataExpression leftme = new MetadataExpression();

        leftme.expected = conditionalme.expected;
        leftme.explicit = conditionalme.explicit;
        analyzer.visit(left, leftme);

        final Node right = conditional.children.get(2);
        final MetadataExpression rightme = new MetadataExpression();

        rightme.expected = conditionalme.expected;
        rightme.explicit = conditionalme.explicit;
        analyzer.visit(right, rightme);

        if (conditionalme.expected == null) {
            final Type promote = promoter.promoteConditional(leftme.actual, rightme.actual, leftme.constant, rightme.constant);

            leftme.expected = promote;
            rightme.expected = promote;
            conditionalme.actual = promote;
        } else {
            conditionalme.actual = conditionalme.expected;
        }

        conditional.children.set(1, caster.markCast(left, leftme));
        conditional.children.set(2, caster.markCast(right, rightme));

        conditionalme.typesafe = leftme.typesafe && rightme.typesafe;
    }
}
