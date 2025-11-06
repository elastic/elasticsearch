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
import org.elasticsearch.xpack.esql.core.InvalidArgumentException;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.util.StringUtils;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.HexLiteralContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.IntegerLiteralContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.LabelListContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.LabelNameContext;
import org.elasticsearch.xpack.esql.parser.PromqlBaseParser.StringContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.Evaluation;
import org.elasticsearch.xpack.esql.plan.logical.promql.selector.LiteralSelector;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;

import static java.util.Collections.emptyList;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.typedParsing;
import static org.elasticsearch.xpack.esql.parser.ParserUtils.visitList;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.AtContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.DecimalLiteralContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.DurationContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.EvaluationContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.OffsetContext;
import static org.elasticsearch.xpack.esql.parser.PromqlBaseParser.TimeValueContext;

class PromqlExpressionBuilder extends PromqlIdentifierBuilder {

    protected final Instant start, end;

    PromqlExpressionBuilder() {
        this(null, null, 0, 0);
    }

    PromqlExpressionBuilder(Instant start, Instant end, int startLine, int startColumn) {
        super(startLine, startColumn);
        Instant now = null;
        if (start == null || end == null) {
            now = DateUtils.nowWithMillisResolution().toInstant();
        }

        this.start = start != null ? start : now;
        this.end = end != null ? end : now;
    }

    protected Expression expression(ParseTree ctx) {
        return typedParsing(this, ctx, Expression.class);
    }

    protected List<Expression> expressions(List<? extends ParserRuleContext> contexts) {
        return visitList(this, contexts, Expression.class);
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
            return Evaluation.NONE;
        }

        Duration offset = null;
        boolean negativeOffset = false;
        Instant at = null;

        AtContext atCtx = ctx.at();
        if (atCtx != null) {
            Source source = source(atCtx);
            if (atCtx.AT_START() != null) {
                at = start;
            } else if (atCtx.AT_END() != null) {
                at = end;
            } else {
                Duration timeValue = visitTimeValue(atCtx.timeValue());
                // the value can have a floating point
                long seconds = timeValue.getSeconds();
                int nanos = timeValue.getNano();

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
    public Duration visitDuration(DurationContext ctx) {
        if (ctx == null) {
            return null;
        }
        Object o = visit(ctx.expression());

        return switch (o) {
            case Duration duration -> duration;
            case Literal l -> throw new ParsingException(
                source(ctx),
                "Expected literals to be already converted to duration, got [{}]",
                l.value()
            );
            case LogicalPlan plan -> {
                // Scalar arithmetic has been folded and wrapped in LiteralSelector
                if (plan instanceof LiteralSelector literalSelector) {
                    Literal literal = literalSelector.literal();
                    Object value = literal.value();

                    // Handle Duration
                    if (value instanceof Duration duration) {
                        yield duration;
                    }

                    // Handle numeric scalars interpreted as seconds
                    if (value instanceof Number num) {
                        long seconds = num.longValue();
                        if (seconds <= 0) {
                            throw new ParsingException(source(ctx), "Duration must be positive, got [{}]s", seconds);
                        }
                        yield Duration.ofSeconds(seconds);
                    }

                    throw new ParsingException(
                        source(ctx),
                        "Expected duration or numeric value, got [{}]",
                        value.getClass().getSimpleName()
                    );
                }

                // Non-literal LogicalPlan
                throw new ParsingException(source(ctx), "Duration must be a constant expression");
            }
            case Expression e -> {
                // Fallback for Expression (shouldn't happen with new logic)
                if (e.foldable()) {
                    Object folded = e.fold(FoldContext.small());
                    if (folded instanceof Duration duration) {
                        yield duration;
                    }
                }
                // otherwise bail out
                throw new ParsingException(source(ctx), "Expected a duration, got [{}]", source(ctx).text());
            }
            default -> throw new ParsingException(source(ctx), "Expected a duration, got [{}]", source(ctx).text());
        };
    }

    @Override
    public Duration visitTimeValue(TimeValueContext ctx) {
        if (ctx.number() != null) {
            var literal = typedParsing(this, ctx.number(), Literal.class);
            Number number = (Number) literal.value();
            if (number instanceof Double d) {
                double v = d.doubleValue();
                Source source = literal.source();
                if (Double.isFinite(v) == false) {
                    throw new ParsingException(source, "Invalid timestamp [{}]", v);
                }
                // Convert to milliseconds (matching Prometheus behavior)
                double millisDouble = v * 1000.0;
                if (millisDouble >= Long.MAX_VALUE || millisDouble <= Long.MIN_VALUE) {
                    throw new ParsingException(source, "Timestamp out of bounds [{}]", v);
                }

                // Round to nearest millisecond, supporting sub-millisecond input
                long millis = Math.round(millisDouble);
                return Duration.ofMillis(millis);
            }

            return Duration.ofSeconds(number.longValue());
        }
        String timeString = null;
        Source source;
        var timeCtx = ctx.TIME_VALUE_WITH_COLON();
        if (timeCtx != null) {
            // drop leading :
            timeString = timeCtx.getText().substring(1).trim();
        } else {
            timeCtx = ctx.TIME_VALUE();
            timeString = timeCtx.getText();
        }
        source = source(timeCtx);

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

    private static Duration parseTimeValue(Source source, String text) {
        Duration duration = PromqlParserUtils.parseDuration(source, text);
        if (duration.isZero()) {
            throw new ParsingException(source, "Invalid time duration [{}], zero value specified", text);
        }
        return duration;
    }
}
