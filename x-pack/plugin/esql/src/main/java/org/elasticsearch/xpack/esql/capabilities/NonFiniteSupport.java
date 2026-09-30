/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.capabilities;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Expression;

import java.io.IOException;

/**
 * Shared infrastructure for expressions that can optionally preserve non-finite ({@code NaN}/{@code ±Inf}) scalar
 * results instead of rejecting them to {@code null} (or throwing). The non-finite-preserving form is only used by the
 * PromQL translation, which requires IEEE-754 semantics; the strict (finite-only) form is the ES|QL default.
 * <p>
 *     The non-finite flag is gated on the wire behind {@link #nonFiniteTransportVersion()}, which defaults to
 *     {@link #ESQL_PROMQL_NON_FINITE_MATH}. Implementations that add the flag to an existing writeable override that
 *     method with a later version so mixed-version clusters that already know the original version do not see an
 *     unexpected extra byte. A non-finite-preserving instance is downgraded to its strict variant whenever the cluster
 *     contains a node that predates the implementation's version, so every node evaluates identical math.
 *     Implementations supply {@link #allowNonFinite()} and {@link #toStrictVariant()} and reuse
 *     {@link #readNonFinite(StreamInput)} / {@link #writeNonFinite(StreamOutput)} for serialization.
 * </p>
 * <p>
 *     Equality does not need to be implemented per expression: {@code Function} folds the flag into {@code equals} and
 *     {@code hashCode} for every implementation.
 * </p>
 */
public interface NonFiniteSupport extends TransportVersionAware {

    TransportVersion ESQL_PROMQL_NON_FINITE_MATH = TransportVersion.fromName("esql_promql_non_finite_math");
    TransportVersion ESQL_PROMQL_NON_FINITE_UNARY_MATH = TransportVersion.fromName("esql_promql_non_finite_unary_math");
    TransportVersion ESQL_PROMQL_NON_FINITE_TRIG = TransportVersion.fromName("esql_promql_non_finite_trig");

    /**
     * Transport version that first writes this expression's non-finite flag. Defaults to
     * {@link #ESQL_PROMQL_NON_FINITE_MATH}; implementations that add the flag to an existing writeable return a later
     * version so nodes that already support the original version do not observe an extra byte they cannot consume.
     */
    default TransportVersion nonFiniteTransportVersion() {
        return ESQL_PROMQL_NON_FINITE_MATH;
    }

    /**
     * Whether this expression preserves non-finite scalar results ({@code true}) or rejects them ({@code false}).
     */
    boolean allowNonFinite();

    /**
     * A copy of this expression with non-finite preservation disabled, i.e. the strict (finite-only) variant.
     */
    Expression toStrictVariant();

    @Override
    default Expression forTransportVersion(TransportVersion minTransportVersion) {
        // Older nodes cannot evaluate the non-finite-preserving variant; when any node in the cluster predates it,
        // downgrade to strict (finite-only) math so every node produces identical results.
        return allowNonFinite() && minTransportVersion.supports(nonFiniteTransportVersion()) == false ? toStrictVariant() : null;
    }

    /**
     * Writes the non-finite flag, but only on versions that understand it, so the byte stream stays compatible with
     * older nodes. Mirrors {@link #readNonFinite(StreamInput, TransportVersion)} using
     * {@link #nonFiniteTransportVersion()}.
     */
    default void writeNonFinite(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(nonFiniteTransportVersion())) {
            out.writeBoolean(allowNonFinite());
        }
    }

    /**
     * Reads the non-finite flag for implementations that use the default {@link #ESQL_PROMQL_NON_FINITE_MATH} version.
     * Predating versions never wrote the byte and are treated as strict ({@code false}). This is {@code static} so it
     * can be used from a delegating {@code this(...)} constructor call, where instance methods are not yet available.
     */
    static boolean readNonFinite(StreamInput in) throws IOException {
        return readNonFinite(in, ESQL_PROMQL_NON_FINITE_MATH);
    }

    /**
     * Reads the non-finite flag written by {@link #writeNonFinite(StreamOutput)}; versions that predate {@code version}
     * never wrote the byte and are treated as strict ({@code false}). Implementations that override
     * {@link #nonFiniteTransportVersion()} must pass that same version here.
     */
    static boolean readNonFinite(StreamInput in, TransportVersion version) throws IOException {
        return in.getTransportVersion().supports(version) && in.readBoolean();
    }
}
