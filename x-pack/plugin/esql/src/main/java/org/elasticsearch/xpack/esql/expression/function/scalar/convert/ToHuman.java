/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.convert;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.expression.ConstantEvaluators;
import org.elasticsearch.compute.expression.ExpressionEvaluator;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.AnyNullIsNull;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesTo;
import org.elasticsearch.xpack.esql.expression.function.FunctionAppliesToLifecycle;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.OptionalArgument;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isFoldable;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isNumeric;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isString;
import static org.elasticsearch.xpack.esql.core.type.DataType.DOUBLE;
import static org.elasticsearch.xpack.esql.core.type.DataType.INTEGER;
import static org.elasticsearch.xpack.esql.core.type.DataType.KEYWORD;
import static org.elasticsearch.xpack.esql.core.type.DataType.LONG;
import static org.elasticsearch.xpack.esql.core.type.DataType.NULL;
import static org.elasticsearch.xpack.esql.core.type.DataType.UNSIGNED_LONG;

/**
 * Renders a numeric value in a human readable form, for example {@code 1.5 GB} or {@code 42 ms}.
 * <p>
 * Implements the {@code to_human(field, unit)} and {@code to_human(field, unit, target_unit)}
 * signatures requested in <a href="https://github.com/elastic/elasticsearch/issues/136533">#136533</a>.
 * The {@code unit} selects how the input value is interpreted: {@code duration} (input in nanoseconds),
 * {@code bits} (input in bits, base 1000), {@code bytes} (input in bytes, base 1024) or {@code percent}
 * (input as a ratio between 0 and 1). Without {@code target_unit} the display unit is picked
 * automatically; with it the value is always rendered in the given unit, e.g. {@code GB}.
 */
public class ToHuman extends EsqlScalarFunction implements OptionalArgument, AnyNullIsNull {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "ToHuman",
        ToHuman::new
    );
    public static final FunctionDefinition DEFINITION = FunctionDefinition.def(ToHuman.class)
        .ternary(ToHuman::new)
        .name("to_human");

    private final Expression field;
    private final Expression unit;
    private final Expression targetUnit;

    @FunctionInfo(
        appliesTo = { @FunctionAppliesTo(lifeCycle = FunctionAppliesToLifecycle.PREVIEW, version = "9.7.0") },
        returnType = "keyword",
        preview = true,
        briefSummary = "Renders a numeric value in a human readable form with units.",
        description = """
            Renders a numeric value in a human readable form, automatically picking a suitable unit
            (for example `1.5 GB` or `42 ms`). The `unit` argument selects how the input is interpreted:
            `duration` (input in nanoseconds), `bits` (input in bits, scaled in steps of 1000),
            `bytes` (input in bytes, scaled in steps of 1024) or `percent` (input as a ratio between 0 and 1).
            The optional `target_unit` pins the output to one unit, for example `GB`, instead of picking one
            automatically. If `null`, the function returns `null`.""",
        examples = {
            @Example(
                file = "convert",
                tag = "toHumanBytes",
                description = "Render a byte count, letting the function pick the unit:"
            ),
            @Example(
                file = "convert",
                tag = "toHumanDurationTargetUnit",
                description = "Render a duration in nanoseconds, pinned to milliseconds:"
            ) }
    )
    public ToHuman(
        Source source,
        @Param(
            name = "field",
            type = { "double", "integer", "long", "unsigned_long" },
            description = "Numeric value to render. If `null`, the function returns `null`."
        ) Expression field,
        @Param(
            name = "unit",
            type = { "keyword", "text" },
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT),
            description = "How to interpret the input: `duration` (nanoseconds), `bits` (bits, base 1000), "
                + "`bytes` (bytes, base 1024) or `percent` (ratio between 0 and 1). Must be a constant."
        ) Expression unit,
        @Param(
            optional = true,
            name = "target_unit",
            type = { "keyword", "text" },
            hint = @Param.Hint(kind = Param.Hint.Kind.CONSTANT),
            description = "Unit to render the value in, for example `GB` or `ms`. Must be a constant. "
                + "Optional; if omitted, the unit is picked automatically. Not supported with `percent`."
        ) Expression targetUnit
    ) {
        super(source, targetUnit == null ? List.of(field, unit) : List.of(field, unit, targetUnit));
        this.field = field;
        this.unit = unit;
        this.targetUnit = targetUnit;
    }

    private ToHuman(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readOptionalNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(field);
        out.writeNamedWriteable(unit);
        out.writeOptionalNamedWriteable(targetUnit);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public DataType dataType() {
        return KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        TypeResolution resolution = isNumeric(field, sourceText(), FIRST);
        if (resolution.unresolved()) {
            return resolution;
        }

        resolution = isString(unit, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }
        resolution = isFoldable(unit, sourceText(), SECOND);
        if (resolution.unresolved()) {
            return resolution;
        }

        if (targetUnit != null) {
            resolution = isString(targetUnit, sourceText(), THIRD);
            if (resolution.unresolved()) {
                return resolution;
            }
            resolution = isFoldable(targetUnit, sourceText(), THIRD);
            if (resolution.unresolved()) {
                return resolution;
            }
        }

        return TypeResolution.TYPE_RESOLVED;
    }

    @Override
    public boolean foldable() {
        return field.foldable() && unit.foldable() && (targetUnit == null || targetUnit.foldable());
    }

    @Evaluator(extraName = "Long")
    static BytesRef process(long value, @Fixed(includeInToString = false) Humanizer humanizer) {
        return humanizer.render((double) value);
    }

    @Evaluator(extraName = "Int")
    static BytesRef process(int value, @Fixed(includeInToString = false) Humanizer humanizer) {
        return humanizer.render((double) value);
    }

    @Evaluator(extraName = "Double")
    static BytesRef process(double value, @Fixed(includeInToString = false) Humanizer humanizer) {
        return humanizer.render(value);
    }

    @Override
    public ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        if (field.dataType() == NULL) {
            return ConstantEvaluators.CONSTANT_NULL_FACTORY;
        }
        // Both arguments are constants; fold them once here so invalid values fail the query
        // at plan time with a clear error instead of producing per-row warnings.
        BytesRef unitValue = (BytesRef) unit.fold(toEvaluator.foldCtx());
        if (unitValue == null) {
            return ConstantEvaluators.CONSTANT_NULL_FACTORY;
        }
        BytesRef targetValue = targetUnit == null ? null : (BytesRef) targetUnit.fold(toEvaluator.foldCtx());
        Humanizer humanizer = Humanizer.of(
            unitValue.utf8ToString(),
            targetValue == null ? null : targetValue.utf8ToString()
        );
        Function<DriverContext, Humanizer> fixedHumanizer = context -> humanizer;

        ExpressionEvaluator.Factory fieldEval = toEvaluator.apply(field);
        DataType fieldType = field.dataType();
        if (fieldType == UNSIGNED_LONG) {
            // Same widening as the other conversion functions: unsigned longs are humanized as doubles.
            fieldEval = new ToDoubleFromUnsignedLongEvaluator.Factory(source(), fieldEval);
            fieldType = DOUBLE;
        } else {
            fieldType = fieldType.widenSmallNumeric();
        }
        return switch (fieldType) {
            case INTEGER -> new ToHumanIntEvaluator.Factory(source(), fieldEval, fixedHumanizer);
            case LONG -> new ToHumanLongEvaluator.Factory(source(), fieldEval, fixedHumanizer);
            case DOUBLE -> new ToHumanDoubleEvaluator.Factory(source(), fieldEval, fixedHumanizer);
            default -> throw EsqlIllegalArgumentException.illegalDataType(field.dataType());
        };
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new ToHuman(source(), newChildren.get(0), newChildren.get(1), targetUnit == null ? null : newChildren.get(2));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, ToHuman::new, field, unit, targetUnit);
    }

    Expression field() {
        return field;
    }

    Expression unit() {
        return unit;
    }

    Expression targetUnit() {
        return targetUnit;
    }

    /** How the input value is interpreted. */
    enum UnitCategory {
        DURATION,
        BITS,
        BYTES,
        PERCENT
    }

    /** A display unit: the label to render and the factor converting input values into it. */
    private record UnitDef(String label, double factor) {}

    /**
     * Parsed and validated {@code unit} / {@code target_unit} arguments. Built once per query
     * from the (constant) arguments and shared with the generated evaluators via {@link Fixed}.
     */
    static final class Humanizer {
        // Ordered smallest to largest; auto mode picks the largest unit rendering a value >= 1.
        private static final List<UnitDef> DURATION_UNITS = List.of(
            new UnitDef("ns", 1.0),
            new UnitDef("us", 1e-3),
            new UnitDef("ms", 1e-6),
            new UnitDef("s", 1e-9),
            new UnitDef("m", 1.0 / 60e9),
            new UnitDef("h", 1.0 / 3600e9),
            new UnitDef("d", 1.0 / 86400e9)
        );
        private static final List<UnitDef> BIT_UNITS = List.of(
            new UnitDef("bit", 1.0),
            new UnitDef("kbit", 1e-3),
            new UnitDef("Mbit", 1e-6),
            new UnitDef("Gbit", 1e-9),
            new UnitDef("Tbit", 1e-12),
            new UnitDef("Pbit", 1e-15)
        );
        private static final List<UnitDef> BYTE_UNITS = List.of(
            new UnitDef("B", 1.0),
            new UnitDef("KB", 1.0 / 1024),
            new UnitDef("MB", 1.0 / (1024 * 1024)),
            new UnitDef("GB", 1.0 / (1024 * 1024 * 1024)),
            new UnitDef("TB", 1.0 / (1024.0 * 1024 * 1024 * 1024)),
            new UnitDef("PB", 1.0 / (1024.0 * 1024 * 1024 * 1024 * 1024))
        );

        private final UnitCategory category;
        private final String targetLabel;
        private final double targetFactor;

        private Humanizer(UnitCategory category, String targetLabel, double targetFactor) {
            this.category = category;
            this.targetLabel = targetLabel;
            this.targetFactor = targetFactor;
        }

        static Humanizer of(String unit, String targetUnit) {
            UnitCategory category = parseCategory(unit);
            if (targetUnit == null) {
                return new Humanizer(category, null, Double.NaN);
            }
            UnitDef target = parseTargetUnit(targetUnit, category);
            return new Humanizer(category, target.label(), target.factor());
        }

        private static UnitCategory parseCategory(String unit) {
            return switch (unit.trim().toLowerCase(Locale.ROOT)) {
                case "duration" -> UnitCategory.DURATION;
                case "bits" -> UnitCategory.BITS;
                case "bytes" -> UnitCategory.BYTES;
                case "percent" -> UnitCategory.PERCENT;
                default -> throw new IllegalArgumentException(
                    "Invalid unit [" + unit + "] for TO_HUMAN; expected one of [duration, bits, bytes, percent]"
                );
            };
        }

        private static UnitDef parseTargetUnit(String targetUnit, UnitCategory category) {
            String normalized = targetUnit.trim().toLowerCase(Locale.ROOT);
            return switch (category) {
                case DURATION -> switch (normalized) {
                    case "ns", "nano", "nanos", "nanosecond", "nanoseconds" -> new UnitDef("ns", 1.0);
                    case "us", "µs", "micro", "micros", "microsecond", "microseconds" -> new UnitDef("us", 1e-3);
                    case "ms", "milli", "millis", "millisecond", "milliseconds" -> new UnitDef("ms", 1e-6);
                    case "s", "sec", "secs", "second", "seconds" -> new UnitDef("s", 1e-9);
                    case "m", "min", "mins", "minute", "minutes" -> new UnitDef("m", 1.0 / 60e9);
                    case "h", "hr", "hrs", "hour", "hours" -> new UnitDef("h", 1.0 / 3600e9);
                    case "d", "day", "days" -> new UnitDef("d", 1.0 / 86400e9);
                    default -> throw invalidTarget(targetUnit, category, "[ns, us, ms, s, m, h, d]");
                };
                case BITS -> switch (normalized) {
                    case "b", "bit", "bits" -> new UnitDef("bit", 1.0);
                    case "kbit", "kbits" -> new UnitDef("kbit", 1e-3);
                    case "mbit", "mbits" -> new UnitDef("Mbit", 1e-6);
                    case "gbit", "gbits" -> new UnitDef("Gbit", 1e-9);
                    case "tbit", "tbits" -> new UnitDef("Tbit", 1e-12);
                    case "pbit", "pbits" -> new UnitDef("Pbit", 1e-15);
                    default -> throw invalidTarget(targetUnit, category, "[bit, kbit, Mbit, Gbit, Tbit, Pbit]");
                };
                case BYTES -> switch (normalized) {
                    case "b", "byte", "bytes" -> new UnitDef("B", 1.0);
                    case "kb", "kib" -> new UnitDef("KB", 1.0 / 1024);
                    case "mb", "mib" -> new UnitDef("MB", 1.0 / (1024 * 1024));
                    case "gb", "gib" -> new UnitDef("GB", 1.0 / (1024 * 1024 * 1024));
                    case "tb", "tib" -> new UnitDef("TB", 1.0 / (1024.0 * 1024 * 1024 * 1024));
                    case "pb", "pib" -> new UnitDef("PB", 1.0 / (1024.0 * 1024 * 1024 * 1024 * 1024));
                    default -> throw invalidTarget(targetUnit, category, "[B, KB, MB, GB, TB, PB]");
                };
                case PERCENT -> throw new IllegalArgumentException("target_unit is not supported with unit [percent]");
            };
        }

        private static IllegalArgumentException invalidTarget(String targetUnit, UnitCategory category, String expected) {
            return new IllegalArgumentException(
                "Invalid target_unit [" + targetUnit + "] for unit [" + category.name().toLowerCase(Locale.ROOT)
                    + "]; expected one of " + expected
            );
        }

        BytesRef render(double value) {
            if (Double.isNaN(value) || Double.isInfinite(value)) {
                return new BytesRef(Double.toString(value));
            }
            String label;
            double scaled;
            if (targetLabel != null) {
                label = targetLabel;
                scaled = Math.abs(value) * targetFactor;
            } else if (category == UnitCategory.PERCENT) {
                label = "%";
                scaled = Math.abs(value) * 100.0;
            } else {
                UnitDef auto = pickAutoUnit(Math.abs(value));
                label = auto.label();
                scaled = Math.abs(value) * auto.factor();
            }
            String number = formatNumber(scaled);
            boolean negative = value < 0.0 && number.equals("0") == false;
            String suffix = category == UnitCategory.PERCENT ? "%" : " " + label;
            return new BytesRef((negative ? "-" : "") + number + suffix);
        }

        private UnitDef pickAutoUnit(double absValue) {
            List<UnitDef> units = switch (category) {
                case DURATION -> DURATION_UNITS;
                case BITS -> BIT_UNITS;
                case BYTES -> BYTE_UNITS;
                case PERCENT -> throw new IllegalArgumentException("percent does not support automatic unit selection");
            };
            for (int i = units.size() - 1; i >= 0; i--) {
                if (absValue * units.get(i).factor() >= 1.0) {
                    return units.get(i);
                }
            }
            return units.get(0);
        }

        /** Renders with up to two decimals, stripping trailing zeros: {@code 42}, {@code 1.5}, {@code 1.57}. */
        private static String formatNumber(double value) {
            String text = String.format(Locale.ROOT, "%.2f", value);
            int dot = text.indexOf('.');
            if (dot >= 0) {
                int end = text.length();
                while (end > dot + 1 && text.charAt(end - 1) == '0') {
                    end--;
                }
                if (end == dot + 1) {
                    end = dot;
                }
                text = text.substring(0, end);
            }
            return text;
        }

        @Override
        public String toString() {
            return category.name().toLowerCase(Locale.ROOT) + "->" + (targetLabel == null ? "auto" : targetLabel);
        }
    }
}
