/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.expression.unit;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * The unit of a column: a product of base units with integer exponents, so that {@code (ms/s) / (1/s)} cancels
 * to {@code ms}.
 * <p>
 * Base units are names normalized through a small vocabulary that covers the spellings mappings use, including
 * OpenTelemetry's UCUM units ({@code By}, {@code {packets}}). Only identical base units cancel: units of the same
 * family, such as {@code ms} and {@code s} or {@code bytes} and {@code MiB}, are only converted into each other
 * when a constant factor scales them.
 * <p>
 * For example {@code kB/s} is {@code {kB=1, s=-1}}, {@code bytes*s} is {@code {bytes=1, s=1}}, and {@link #ONE}
 * is the empty map.
 */
public record Unit(Map<String, Integer> exponents) {

    /** The unit of counts and of ratios between two values in the same unit. */
    public static final Unit ONE = new Unit(Map.of());

    public static final Unit SECOND = new Unit(Map.of("s", 1));

    private static final String UNITLESS = "1";

    private static final Map<String, String> VOCABULARY = Map.ofEntries(
        Map.entry("nanos", "nanos"),
        Map.entry("ns", "nanos"),
        Map.entry("nanoseconds", "nanos"),
        Map.entry("micros", "micros"),
        Map.entry("us", "micros"),
        Map.entry("microseconds", "micros"),
        Map.entry("ms", "ms"),
        Map.entry("millis", "ms"),
        Map.entry("milliseconds", "ms"),
        Map.entry("s", "s"),
        Map.entry("seconds", "s"),
        Map.entry("min", "min"),
        Map.entry("minutes", "min"),
        Map.entry("h", "h"),
        Map.entry("hours", "h"),
        Map.entry("d", "d"),
        Map.entry("days", "d"),
        Map.entry("By", "bytes"),
        Map.entry("byte", "bytes"),
        Map.entry("bytes", "bytes"),
        Map.entry("kB", "kB"),
        Map.entry("kBy", "kB"),
        Map.entry("MB", "MB"),
        Map.entry("MBy", "MB"),
        Map.entry("GB", "GB"),
        Map.entry("GBy", "GB"),
        Map.entry("TB", "TB"),
        Map.entry("TBy", "TB"),
        Map.entry("KiB", "KiB"),
        Map.entry("KiBy", "KiB"),
        Map.entry("MiB", "MiB"),
        Map.entry("MiBy", "MiB"),
        Map.entry("GiB", "GiB"),
        Map.entry("GiBy", "GiB"),
        Map.entry("TiB", "TiB"),
        Map.entry("TiBy", "TiB"),
        Map.entry("bit", "bits"),
        Map.entry("bits", "bits"),
        Map.entry("%", "percent"),
        Map.entry("percent", "percent")
    );

    /**
     * Canonical units that convert exactly into each other, with their size in a reference unit of their family:
     * time in nanoseconds, and data sizes in bytes, from bits to decimal ({@code kB}) and binary ({@code KiB}) prefixes.
     * The sizes of a family are distinct, so the factor of a scaling names at most one unit.
     */
    private static final List<List<Map.Entry<String, BigDecimal>>> FAMILIES = List.of(
        List.of(
            Map.entry("nanos", BigDecimal.ONE),
            Map.entry("micros", BigDecimal.valueOf(1_000L)),
            Map.entry("ms", BigDecimal.valueOf(1_000_000L)),
            Map.entry("s", BigDecimal.valueOf(1_000_000_000L)),
            Map.entry("min", BigDecimal.valueOf(60_000_000_000L)),
            Map.entry("h", BigDecimal.valueOf(3_600_000_000_000L)),
            Map.entry("d", BigDecimal.valueOf(86_400_000_000_000L))
        ),
        List.of(
            Map.entry("bits", new BigDecimal("0.125")),
            Map.entry("bytes", BigDecimal.ONE),
            Map.entry("kB", BigDecimal.TEN.pow(3)),
            Map.entry("MB", BigDecimal.TEN.pow(6)),
            Map.entry("GB", BigDecimal.TEN.pow(9)),
            Map.entry("TB", BigDecimal.TEN.pow(12)),
            Map.entry("KiB", BigDecimal.valueOf(1024L)),
            Map.entry("MiB", BigDecimal.valueOf(1024L).pow(2)),
            Map.entry("GiB", BigDecimal.valueOf(1024L).pow(3)),
            Map.entry("TiB", BigDecimal.valueOf(1024L).pow(4))
        )
    );

    public static Unit of(String base) {
        return new Unit(Map.of(normalize(base), 1));
    }

    /**
     * Parses a {@code meta.unit} value. {@code 1} is unitless; {@code a/b}, {@code a*b} and {@code a^2} compose.
     */
    public static Unit parse(String unit) {
        String[] parts = unit.split("/");
        Unit result = ONE;
        for (int i = 0; i < parts.length; i++) {
            for (String base : parts[i].split("\\*")) {
                String trimmed = base.trim();
                if (trimmed.isEmpty() || trimmed.equals(UNITLESS)) {
                    continue;
                }
                Unit term = power(trimmed);
                result = i == 0 ? result.times(term) : result.over(term);
            }
        }
        return result;
    }

    private static Unit power(String term) {
        int caret = term.lastIndexOf('^');
        if (caret > 0) {
            try {
                int exponent = Integer.parseInt(term.substring(caret + 1));
                if (exponent != 0) {
                    return new Unit(Map.of(normalize(term.substring(0, caret)), exponent));
                }
            } catch (NumberFormatException e) {
                // not an exponent, the whole term is the unit's name
            }
        }
        return of(term);
    }

    private static String normalize(String base) {
        // UCUM annotations such as {packets} name a counted thing.
        String name = base.length() > 2 && base.startsWith("{") && base.endsWith("}") ? base.substring(1, base.length() - 1) : base;
        return VOCABULARY.getOrDefault(name, name);
    }

    public Unit times(Unit other) {
        return combine(other, 1);
    }

    public Unit over(Unit other) {
        return combine(other, -1);
    }

    private Unit combine(Unit other, int sign) {
        Map<String, Integer> result = new LinkedHashMap<>(exponents);
        other.exponents.forEach((base, exponent) -> result.merge(base, sign * exponent, Integer::sum));
        result.values().removeIf(exponent -> exponent == 0);
        return new Unit(result);
    }

    public String format() {
        StringBuilder numerator = new StringBuilder();
        StringBuilder denominator = new StringBuilder();
        exponents.forEach((base, exponent) -> {
            int power = exponent > 0 ? exponent : -exponent;
            String term = power == 1 ? base : base + "^" + power;
            if (exponent > 0) {
                numerator.append(numerator.isEmpty() ? "" : "*").append(term);
            } else {
                denominator.append('/').append(term);
            }
        });
        return (numerator.isEmpty() ? UNITLESS : numerator.toString()) + denominator;
    }

    @Override
    public String toString() {
        return format();
    }

    // Algebra used by the arithmetic operators.

    /** {@code left + right} or {@code left - right}: both sides must agree; adding a constant keeps the other side's unit. */
    @Nullable
    public static Unit sameUnit(Expression left, Expression right, Function<Expression, Unit> unitOf) {
        Unit l = unitOf.apply(left);
        Unit r = unitOf.apply(right);
        if (l != null && r != null) {
            return l.equals(r) ? l : null;
        }
        if (l != null && right instanceof Literal) {
            return l;
        }
        if (r != null && left instanceof Literal) {
            return r;
        }
        return null;
    }

    @Nullable
    public static Unit multiply(Expression left, Expression right, Function<Expression, Unit> unitOf) {
        BigDecimal rightFactor = number(right);
        if (rightFactor != null) {
            return scale(unitOf.apply(left), rightFactor, true);
        }
        BigDecimal leftFactor = number(left);
        if (leftFactor != null) {
            return scale(unitOf.apply(right), leftFactor, true);
        }
        Unit l = unitOf.apply(left);
        Unit r = unitOf.apply(right);
        return l == null || r == null ? null : l.times(r);
    }

    @Nullable
    public static Unit divide(Expression left, Expression right, Function<Expression, Unit> unitOf) {
        BigDecimal divisor = number(right);
        if (divisor != null) {
            return scale(unitOf.apply(left), divisor, false);
        }
        Unit l = unitOf.apply(left);
        Unit r = unitOf.apply(right);
        return l == null || r == null ? null : l.over(r);
    }

    /**
     * The unit of {@code value * factor} or {@code value / factor}. The first unit of the numerator converts within its
     * family when the factor is an exact conversion ({@code bytes / 1024} is {@code KiB}, {@code bytes/s / 1000} is
     * {@code kB/s}); anything else is marked as scaled ({@code ms/7}), an opaque unit that no longer cancels.
     */
    @Nullable
    static Unit scale(@Nullable Unit unit, BigDecimal factor, boolean multiply) {
        if (unit == null) {
            return null;
        }
        if (factor.compareTo(BigDecimal.ONE) == 0) {
            return unit;
        }
        String base = unit.exponents.entrySet().stream().filter(e -> e.getValue() == 1).map(Map.Entry::getKey).findFirst().orElse(null);
        List<Map.Entry<String, BigDecimal>> family = base == null ? null : familyOf(base);
        if (family != null) {
            // value / k expresses the same quantity in a unit k times larger; value * k in one k times smaller.
            BigDecimal size = sizeIn(family, base);
            BigDecimal scaled = multiply ? size.divide(factor, MathContext.DECIMAL64) : size.multiply(factor);
            for (Map.Entry<String, BigDecimal> candidate : family) {
                if (candidate.getValue().compareTo(scaled) == 0) {
                    return unit.over(new Unit(Map.of(base, 1))).times(new Unit(Map.of(candidate.getKey(), 1)));
                }
            }
        }
        return new Unit(Map.of(unit.format() + (multiply ? "*" : "/") + factor.stripTrailingZeros().toPlainString(), 1));
    }

    @Nullable
    private static List<Map.Entry<String, BigDecimal>> familyOf(String base) {
        for (List<Map.Entry<String, BigDecimal>> family : FAMILIES) {
            if (sizeIn(family, base) != null) {
                return family;
            }
        }
        return null;
    }

    @Nullable
    private static BigDecimal sizeIn(List<Map.Entry<String, BigDecimal>> family, String base) {
        for (Map.Entry<String, BigDecimal> unit : family) {
            if (unit.getKey().equals(base)) {
                return unit.getValue();
            }
        }
        return null;
    }

    @Nullable
    private static BigDecimal number(Expression e) {
        if (e instanceof Literal literal && literal.value() instanceof Number n) {
            return new BigDecimal(n.toString());
        }
        return null;
    }
}
