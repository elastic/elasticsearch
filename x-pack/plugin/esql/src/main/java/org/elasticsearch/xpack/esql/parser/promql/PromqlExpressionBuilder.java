/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.parser.promql;

import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.tree.ParseTree;
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

    /**
     * Prometheus duration limits based on Go time.Duration (int64 nanoseconds).
     * Maximum: ~292.27 years (2^63 - 1 nanoseconds = 9,223,372,036 seconds)
     * Minimum: ~-292.27 years (-(2^63) nanoseconds = -9,223,372,037 seconds)
     */
    private static final long MAX_DURATION_SECONDS = 9223372036L;
    private static final long MIN_DURATION_SECONDS = -9223372037L;

    private final Literal start, end;

    PromqlExpressionBuilder(Literal start, Literal end, int startLine, int startColumn) {
        super(startLine, startColumn);
        this.start = start;
        this.end = end;
    }

    protected Expression expression(ParseTree ctx) {
        return typedParsing(this, ctx, Expression.class);
    }

    protected List<Expression> expressions(List<? extends ParserRuleContext> contexts) {
        return visitList(this, contexts, Expression.class);
    }

    /**
     * Validates that a duration is within Prometheus's acceptable range (~292 years).
     * Prometheus uses Go's time.Duration internally, which is an int64 of nanoseconds.
     *
     * @param source the source location for error reporting
     * @param duration the duration to validate
     * @throws ParsingException if the duration is out of range
     */
    private static void validateDurationRange(Source source, Duration duration) {
        long seconds = duration.getSeconds();
        if (seconds > MAX_DURATION_SECONDS || seconds < MIN_DURATION_SECONDS) {
            throw new ParsingException(source, "Duration out of range");
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
            return Evaluation.NONE;
        }

        Literal offset = Literal.NULL;
        boolean negativeOffset = false;
        Literal at = Literal.NULL;

        AtContext atCtx = ctx.at();
        if (atCtx != null) {
            Source source = source(atCtx);
            if (atCtx.AT_START() != null) {
                if (start == null) {
                    throw new ParsingException(source, "@ start() can only be used if parameter [start] or [time] is provided");
                }
                at = start;
            } else if (atCtx.AT_END() != null) {
                if (end == null) {
                    throw new ParsingException(source, "@ end() can only be used if parameter [end] or [time] is provided");
                }
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
                at = Literal.dateTime(source, Instant.ofEpochSecond(seconds, nanos));
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
    public Literal visitDuration(DurationContext ctx) {
        if (ctx == null) {
            return Literal.NULL;
        }
        Object o = visit(ctx.expression());

        // unwrap expressions to get the underlying value
        o = switch (o) {
            case LogicalPlan plan -> {
                // Scalar arithmetic has been folded and wrapped in LiteralSelector
                if (plan instanceof LiteralSelector literalSelector) {
                    yield literalSelector.literal().value();
                }
                throw new ParsingException(source(ctx), "Expected duration or numeric value, got [{}]", plan.getClass().getSimpleName());
            }
            case Literal l -> l.value();
            case Expression e -> {
                // Fallback for Expression (shouldn't happen with new logic)
                if (e.foldable()) {
                    yield e.fold(FoldContext.small());
                }
                // otherwise bail out
                throw new ParsingException(source(ctx), "Expected a duration, got [{}]", source(ctx).text());
            }
            default -> o;
        };

        Duration d = switch (o) {
            case Duration duration -> duration;
            // Handle numeric scalars interpreted as seconds
            case Number num -> {
                long seconds = num.longValue();
                if (seconds <= 0) {
                    throw new ParsingException(source(ctx), "Duration must be positive, got [{}]s", seconds);
                }
                Duration duration = Duration.ofSeconds(seconds);
                // Validate the resulting duration is within acceptable range
                validateDurationRange(source(ctx), duration);
                yield duration;
            }
            default -> throw new ParsingException(source(ctx), "Expected a duration, got [{}]", source(ctx).text());
        };
        return Literal.timeDuration(source(ctx), d);
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

                // Check if the value exceeds duration range before conversion
                if (v > MAX_DURATION_SECONDS || v < MIN_DURATION_SECONDS) {
                    throw new ParsingException(source, "Duration out of range");
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

            // Handle integer/long values
            long longValue = number.longValue();
            if (longValue > MAX_DURATION_SECONDS || longValue < MIN_DURATION_SECONDS) {
                throw new ParsingException(literal.source(), "Duration out of range");
            }
            return Duration.ofSeconds(longValue);
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
            return Literal.fromDouble(source, value);
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
                return Literal.fromDouble(source, StringUtils.parseDouble(text));
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
        return Literal.keyword(source, string(ctx.STRING()));
    }

    private static Duration parseTimeValue(Source source, String text) {
        Duration duration = PromqlParserUtils.parseDuration(source, text);
        if (duration.isZero()) {
            throw new ParsingException(source, "Invalid time duration [{}], zero value specified", text);
        }
        validateDurationRange(source, duration);
        return duration;
    }
}
