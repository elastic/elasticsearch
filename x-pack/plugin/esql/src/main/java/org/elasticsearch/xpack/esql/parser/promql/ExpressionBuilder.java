/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
import org.elasticsearch.common.time.DateUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.VectorBinaryOperator;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.VectorMatch;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.arithmetic.VectorBinaryArithmetic;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.arithmetic.VectorBinaryArithmetic.ArithmeticOp;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.comparison.VectorBinaryComparison;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.comparison.VectorBinaryComparison.ComparisonOp;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.set.VectorBinarySet;
import org.elasticsearch.xpack.esql.expression.promql.predicate.operator.set.VectorBinarySet.SetOp;
import org.elasticsearch.xpack.esql.expression.promql.types.PromqlDataTypes;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.ArithmeticBinaryContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.ArithmeticUnaryContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.HexLiteralContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.IntegerLiteralContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.LabelListContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.LabelNameContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.ModifierContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.StringContext;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Evaluation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.source;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.typedParsing;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.visitList;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.AND;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.ASTERISK;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.AtContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.CARET;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.DecimalLiteralContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.DurationContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.EQ;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.EvaluationContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.GT;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.GTE;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.LT;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.LTE;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.MINUS;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.NEQ;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.OR;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.OffsetContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.PERCENT;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.PLUS;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.SLASH;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.TimeValueContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.UNLESS;

class ExpressionBuilder extends IdentifierBuilder {

    protected final Instant start, stop;

    ExpressionBuilder() {
        this(null, null);
    }

    ExpressionBuilder(Instant start, Instant stop) {
        Instant now = null;
        if (start == null || stop == null) {
            now = DateUtils.nowWithMillisResolution().toInstant();
        }

        this.start = start != null ? start : now;
        this.stop = stop != null ? stop : now;
    }

    protected Expression expression(ParseTree ctx) {
        return typedParsing(this, ctx, Expression.class);
    }

    protected List<Expression> expressions(List<? extends ParserRuleContext> contexts) {
        return visitList(this, contexts, Expression.class);
    }

    @Override
    public Expression visitArithmeticUnary(ArithmeticUnaryContext ctx) {
        Source source = source(ctx);
        Expression expression = expression(ctx.expression());
        DataType dataType = expression.dataType();
        if ((PromqlDataTypes.isScalar(dataType) || PromqlDataTypes.isInstantVector(dataType)) == false) {
            throw new ParsingException(
                source,
                "Unary expression only allowed on expressions of type scalar or instance vector, got [{}]",
                dataType.typeName()
            );
        }
        // convert - into a binary operator
        if (ctx.operator.getType() == MINUS) {
            expression = new VectorBinaryArithmetic(source, Literal.fromDouble(source, 0.0), expression, VectorMatch.NONE, ArithmeticOp.SUB);
        }

        return expression;
    }

    @Override
    public Expression visitArithmeticBinary(ArithmeticBinaryContext ctx) {
        Expression le = expression(ctx.left);
        Expression re = expression(ctx.right);
        Source source = source(ctx);

        boolean bool = ctx.BOOL() != null;
        int opType = ctx.op.getType();
        String opText = ctx.op.getText();

        // validate operation against expression types
        boolean leftIsScalar = PromqlDataTypes.isScalar(le.dataType());
        boolean rightIsScalar = PromqlDataTypes.isScalar(re.dataType());

        // comparisons against scalars require bool
        if (bool == false && leftIsScalar && rightIsScalar) {
            switch (opType) {
                case EQ:
                case NEQ:
                case LT:
                case LTE:
                case GT:
                case GTE:
                    throw new ParsingException(source, "Comparisons [{}] between scalars must use the BOOL modifier", opText);
            }
        }
        // set operations are not allowed on scalars
        if (leftIsScalar || rightIsScalar) {
            switch (opType) {
                case AND:
                case UNLESS:
                case OR:
                    throw new ParsingException(source, "Set operator [{}] not allowed in binary scalar expression", opText);
            }
        }

        VectorMatch modifier = VectorMatch.NONE;

        ModifierContext modifierCtx = ctx.modifier();
        if (modifierCtx != null) {
            // modifiers work only on vectors
            if (PromqlDataTypes.isInstantVector(le.dataType()) == false || PromqlDataTypes.isInstantVector(re.dataType()) == false) {
                throw new ParsingException(source, "Vector matching allowed only between instant vectors");
            }

            VectorMatch.Filter filter = modifierCtx.ON() != null ? VectorMatch.Filter.ON : VectorMatch.Filter.IGNORING;
            List<String> filterList = visitLabelList(modifierCtx.modifierLabels);
            VectorMatch.Joining joining = VectorMatch.Joining.NONE;
            List<String> groupingList = visitLabelList(modifierCtx.groupLabels);
            if (modifierCtx.joining != null) {
                joining = modifierCtx.GROUP_LEFT() != null ? VectorMatch.Joining.LEFT : VectorMatch.Joining.RIGHT;

                // grouping not allowed with logic operators
                switch (opType) {
                    case AND:
                    case UNLESS:
                    case OR:
                        throw new ParsingException(source(modifierCtx), "No grouping [{}] allowed for [{}] operator", joining, opText);
                }

                // label declared in ON cannot appear in grouping
                if (modifierCtx.ON() != null) {
                    List<String> repeatedLabels = new ArrayList<>(groupingList);
                    if (filterList.isEmpty() == false && repeatedLabels.retainAll(filterList) && repeatedLabels.isEmpty() == false) {
                        throw new ParsingException(
                            source(modifierCtx.ON()),
                            "Label{} {} must not occur in ON and GROUP clause at once",
                            repeatedLabels.size() > 1 ? "s" : "",
                            repeatedLabels
                        );
                    }

                }
            }

            modifier = new VectorMatch(filter, new LinkedHashSet<>(filterList), joining, new LinkedHashSet<>(groupingList));
        }

        VectorBinaryOperator.BinaryOp binaryOperator = switch (opType) {
            // arithmetic
            case CARET -> ArithmeticOp.POW;
            case ASTERISK -> ArithmeticOp.MUL;
            case PERCENT -> ArithmeticOp.MOD;
            case SLASH -> ArithmeticOp.DIV;
            case MINUS -> ArithmeticOp.SUB;
            case PLUS -> ArithmeticOp.ADD;
            // comparison
            case EQ -> ComparisonOp.EQ;
            case NEQ -> ComparisonOp.NEQ;
            case LT -> ComparisonOp.LT;
            case LTE -> ComparisonOp.LTE;
            case GT -> ComparisonOp.GT;
            case GTE -> ComparisonOp.GTE;
            // set
            case AND -> SetOp.INTERSECT;
            case UNLESS -> SetOp.SUBTRACT;
            case OR -> SetOp.UNION;
            default -> throw new ParsingException(source(ctx.op), "Unknown arithmetic {}", opText);
        };

        return switch (binaryOperator) {
            case ArithmeticOp arithmeticOp -> new VectorBinaryArithmetic(source, le, re, modifier, arithmeticOp);
            case ComparisonOp comparisonOp -> new VectorBinaryComparison(source, le, re, modifier, bool, comparisonOp);
            case SetOp setOp -> new VectorBinarySet(source, le, re, modifier, setOp);
            default -> throw new ParsingException(source(ctx.op), "Unknown arithmetic {}", opText);
        };
    }

    TimeValue expressionToTimeValue(Expression timeValueAsExpression) {
        if (timeValueAsExpression instanceof Literal literal
            && literal.foldable()
            && literal.fold(FoldContext.small()) instanceof TimeValue timeValue) {
            return timeValue;
        } else {
            throw new ParsingException(
                timeValueAsExpression.source(),
                "Expected a duration, got [{}]",
                timeValueAsExpression.source().text()
            );
        }
    }

    @Override
    public List<String> visitLabelList(LabelListContext ctx) {
        return ctx != null ? visitList(this, ctx.labelName(), String.class) : emptyList();
    }

    @Override
    public String visitLabelName(LabelNameContext ctx) {
        if (ctx.identifier() != null) {
            return visitIdentifier(ctx.identifier());
        }
        if (ctx.STRING() != null) {
            return string(ctx.STRING());
        }
        if (ctx.number() != null) {
            Literal l = typedParsing(this, ctx.number(), Literal.class);
            return String.valueOf(l.value());
        }
        throw new ParsingException(source(ctx), "Expected label name, got [{}]", source(ctx).text());
    }

    @Override
    public Evaluation visitEvaluation(EvaluationContext ctx) {
        if (ctx == null) {
            return null;
        }

        TimeValue offset = null;
        boolean negativeOffset = false;
        Instant at = null;

        AtContext atCtx = ctx.at();
        if (atCtx != null) {
            Source source = source(atCtx);
            if (atCtx.AT_START() != null) {
                at = start;
            } else if (atCtx.AT_END() != null) {
                at = stop;
            } else {
                TimeValue timeValue = visitTimeValue(atCtx.timeValue());
                // the value can have a floating point
                long seconds = timeValue.seconds();
                double secondFrac = timeValue.secondsFrac(); // convert to nanoseconds
                long nanos = 0;

                if (secondFrac >= Long.MAX_VALUE / 1_000_000 || secondFrac <= Long.MIN_VALUE / 1_000_000) {
                    throw new ParsingException(source, "Decimal value [{}] is too large/precise", secondFrac);
                }
                nanos = (long) (secondFrac * 1_000_000_000);

                if (atCtx.MINUS() != null) {
                    if (seconds == Long.MIN_VALUE) {
                        throw new ParsingException(source, "Value [{}] cannot be negated due to underflow", seconds);
                    }
                    seconds = -seconds;
                    nanos = -nanos;
                }
                at = Instant.ofEpochSecond(seconds, nanos);
            }
        }
        OffsetContext offsetContext = ctx.offset();
        if (offsetContext != null) {
            offset = visitDuration(offsetContext.duration());
            negativeOffset = offsetContext.MINUS() != null;
        }
        return new Evaluation(offset, negativeOffset, at);
    }

    @Override
    public TimeValue visitDuration(DurationContext ctx) {
        if (ctx == null) {
            return null;
        }
        Object o = visit(ctx.expression());

        return switch (o) {
            case TimeValue tv -> tv;
            case Literal l -> throw new ParsingException(
                source(ctx),
                "Expected literals to be already converted to timevalue, got [{}]",
                l.value()
            );
            case Expression e -> {
                if (e.foldable() == false) {
                    throw new ParsingException(source(ctx), "Expected a duration, got [{}]", source(ctx).text());
                }
                Object folded = e.fold(FoldContext.small());
                if (folded instanceof TimeValue timeValue) {
                    yield timeValue;
                } else {
                    throw new ParsingException(source(ctx), "Expected a duration, got [{}]", source(ctx).text());
                }
            }
            default -> throw new ParsingException(source(ctx), "Expected a duration, got [{}]", source(ctx).text());
        };
    }

    @Override
    public TimeValue visitTimeValue(TimeValueContext ctx) {
        if (ctx.number() != null) {
            var literal = typedParsing(this, ctx.number(), Literal.class);
            Number number = (Number) literal.value();
            if (number instanceof Double d) {
                double v = d.doubleValue();
                Source source = literal.source();
                if (Double.isFinite(v) == false) {
                    throw new ParsingException(source, "Invalid timestamp [{}]", v);
                }
                if (v >= Long.MAX_VALUE || v <= Long.MIN_VALUE) {
                    throw new ParsingException(source, "Timestamp out of bounds [{}]", v);
                }
                if (v - (long)v > 0) {
                    throw new ParsingException(source, "Timestamps must be in seconds precision");
                }
            }

            return new TimeValue(number.longValue(), TimeUnit.SECONDS);
        }
        String timeString = null;
        Source source;
        if (ctx.TIME_VALUE_WITH_COLON() != null) {
            // drop leading :
            timeString = ctx.TIME_VALUE_WITH_COLON().getText().substring(1).trim();
            source = source(ctx.TIME_VALUE_WITH_COLON());
        } else {
            timeString = ctx.TIME_VALUE().getText();
            source = source(ctx.TIME_VALUE());
        }

        return parseTimeValue(source, timeString);
    }

    @Override
    public Literal visitDecimalLiteral(DecimalLiteralContext ctx) {
        Source source = source(ctx);
        String text = ctx.getText();

        try {
            double value;
            String s = text.toLowerCase(Locale.ROOT);
            if ("inf".equals(s)) {
                value = Double.POSITIVE_INFINITY;
            } else if ("nan".equals(s)) {
                value = Double.NaN;
            } else {
                value = Double.parseDouble(text);
            }
            return new Literal(source, value, DataType.DOUBLE);
        } catch (NumberFormatException ne) {
            throw new ParsingException(source, "Cannot parse number [{}]", text);
        }
    }

    @Override
    public Literal visitIntegerLiteral(IntegerLiteralContext ctx) {
        Source source = source(ctx);
        String text = ctx.getText();

        Number value;

        try {
            value = StringUtils.parseIntegral(text);
        } catch (InvalidArgumentException siae) {
            // if it's too large, then quietly try to parse as a float instead
            try {
                // use DataTypes.DOUBLE for precise type
                return new Literal(source, StringUtils.parseDouble(text), DataType.DOUBLE);
            } catch (QlIllegalArgumentException ignored) {}

            throw new ParsingException(source, siae.getMessage());
        }

        // use type instead for precise type
        return new Literal(source, value, value instanceof Integer ? DataType.INTEGER : DataType.LONG);
    }

    @Override
    public Literal visitHexLiteral(HexLiteralContext ctx) {
        Source source = source(ctx);
        String text = ctx.getText();

        DataType type = DataType.LONG;
        Object val;

        // remove leading 0x
        long value;
        try {
            value = Long.parseLong(text.substring(2), 16);
        } catch (NumberFormatException e) {
            throw new ParsingException(source, "Cannot parse hexadecimal expression [{}]", text);
        }

        // try to downsize to int
        if ((int) value == value) {
            type = DataType.INTEGER;
            val = (int) value;
        } else {
            val = value;
        }
        return new Literal(source, val, type);
    }

    @Override
    public Literal visitString(StringContext ctx) {
        Source source = source(ctx);
        return new Literal(source, string(ctx.STRING()), DataType.KEYWORD);
    }

    private static TimeValue parseTimeValue(Source source, String text) {
        TimeValue timeValue = ParsingUtils.parseTimeValue(source, text);
        if (timeValue.duration() == 0) {
            throw new ParsingException(source, "Invalid time duration [{}], zero value specified", text);
        }
        return timeValue;
    }
}
