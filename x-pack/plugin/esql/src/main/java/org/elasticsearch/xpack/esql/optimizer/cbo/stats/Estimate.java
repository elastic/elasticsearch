/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.cbo.stats;

import java.util.Objects;

/** A non-negative estimate that retains its uncertainty and evidence source. */
public record Estimate(double value, double upperBound, double spread, Provenance provenance) {
    public static final Estimate UNKNOWN = new Estimate(Double.NaN, Double.NaN, Double.NaN, Provenance.UNKNOWN);

    public Estimate {
        Objects.requireNonNull(provenance);
        if (provenance == Provenance.UNKNOWN) {
            value = Double.NaN;
            upperBound = Double.NaN;
            spread = Double.NaN;
        } else if (Double.isFinite(value) == false
            || value < 0
            || Double.isFinite(upperBound) == false
            || upperBound < value
            || Double.isFinite(spread) == false
            || spread < 0) {
                throw new IllegalArgumentException("known estimates require finite, non-negative values and bounds");
            }
    }

    /** Creates a point estimate for a value known without uncertainty. */
    public static Estimate exact(double value) {
        return new Estimate(value, value, 0, Provenance.EXACT);
    }

    /** Creates a point estimate reported by source metadata. */
    public static Estimate metadata(double value) {
        return new Estimate(value, value, 0, Provenance.METADATA);
    }

    /** Creates an estimate whose range reflects sampling variability. */
    public static Estimate sampled(double value, double spread) {
        return new Estimate(value, add(value, multiply(spread, 2)), spread, Provenance.SAMPLED);
    }

    /** Creates an estimate derived from other statistics. */
    public static Estimate derived(double value, double upperBound, double spread) {
        return new Estimate(value, Math.max(value, upperBound), spread, Provenance.DERIVED);
    }

    /** Creates a schema-based estimate with deliberately broad uncertainty. */
    public static Estimate defaultValue(double value) {
        double spread = value / 2;
        return new Estimate(value, add(value, value), spread, Provenance.DEFAULT);
    }

    /** Returns whether no usable estimate is available. */
    public boolean isUnknown() {
        return provenance == Provenance.UNKNOWN;
    }

    /** Returns the estimate's central value or a caller-provided fallback. */
    public double valueOr(double fallback) {
        return isUnknown() ? fallback : value;
    }

    /** Estimates the chance that the represented value is at most the supplied threshold. */
    public double probabilityAtMost(double threshold) {
        if (isUnknown() || Double.isNaN(threshold)) {
            return 0;
        }
        double lower = Math.max(0, value - spread);
        double upper = Math.max(upperBound, add(value, spread));
        if (threshold < lower) {
            return 0;
        }
        if (threshold >= upper || upper == lower) {
            return 1;
        }
        return Math.max(0, Math.min(1, (threshold - lower) / (upper - lower)));
    }

    /** Adds estimates while preserving their joint uncertainty. */
    public Estimate plus(Estimate other) {
        Objects.requireNonNull(other);
        if (isUnknown() || other.isUnknown()) {
            return UNKNOWN;
        }
        return new Estimate(
            add(value, other.value),
            add(upperBound, other.upperBound),
            add(spread, other.spread),
            combine(provenance, other.provenance)
        );
    }

    /** Multiplies non-negative estimates and propagates an upper bound. */
    public Estimate multiply(Estimate other) {
        Objects.requireNonNull(other);
        if (isUnknown() || other.isUnknown()) {
            if (isDefinitelyZero() || other.isDefinitelyZero()) {
                return exact(0);
            }
            return UNKNOWN;
        }
        double propagatedSpread = add(multiply(spread, other.upperBound), multiply(other.spread, upperBound));
        return new Estimate(
            multiply(value, other.value),
            multiply(upperBound, other.upperBound),
            propagatedSpread,
            combine(provenance, other.provenance)
        );
    }

    private boolean isDefinitelyZero() {
        return isUnknown() == false && value == 0 && upperBound == 0 && spread == 0;
    }

    /** Scales an estimate by a finite, non-negative factor. */
    public Estimate scale(double factor) {
        if (Double.isFinite(factor) == false || factor < 0) {
            throw new IllegalArgumentException("scale must be finite and non-negative");
        }
        if (isUnknown()) {
            return UNKNOWN;
        }
        return new Estimate(multiply(value, factor), multiply(upperBound, factor), multiply(spread, factor), provenance);
    }

    private static Provenance combine(Provenance left, Provenance right) {
        if (left == Provenance.UNKNOWN || right == Provenance.UNKNOWN) {
            return Provenance.UNKNOWN;
        }
        if (left == Provenance.DEFAULT || right == Provenance.DEFAULT) {
            return Provenance.DEFAULT;
        }
        if (left == Provenance.SAMPLED || right == Provenance.SAMPLED) {
            return Provenance.SAMPLED;
        }
        if (left == Provenance.EXACT && right == Provenance.EXACT) {
            return Provenance.EXACT;
        }
        if (left == Provenance.METADATA && right == Provenance.METADATA) {
            return Provenance.METADATA;
        }
        return Provenance.DERIVED;
    }

    private static double add(double left, double right) {
        double result = left + right;
        return Double.isFinite(result) ? result : Double.MAX_VALUE;
    }

    private static double multiply(double left, double right) {
        double result = left * right;
        return Double.isFinite(result) ? result : Double.MAX_VALUE;
    }
}
