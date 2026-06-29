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
 *     The non-finite flag is gated behind {@link #ESQL_PROMQL_NON_FINITE_MATH} on the wire, and a non-finite-preserving
 *     instance is downgraded to its strict variant whenever the cluster contains a node that predates that version, so
 *     every node evaluates identical math. Implementations supply {@link #allowNonFinite()} and {@link #toStrictVariant()}
 *     and reuse {@link #readNonFinite(StreamInput)} / {@link #writeNonFinite(StreamOutput)} for serialization.
 * </p>
 */
public interface NonFiniteSupport extends TransportVersionAware {

    TransportVersion ESQL_PROMQL_NON_FINITE_MATH = TransportVersion.fromName("esql_promql_non_finite_math");

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
        return allowNonFinite() && minTransportVersion.supports(ESQL_PROMQL_NON_FINITE_MATH) == false ? toStrictVariant() : null;
    }

    /**
     * Writes the non-finite flag, but only on versions that understand it, so the byte stream stays compatible with
     * older nodes. Mirrors {@link #readNonFinite(StreamInput)}.
     */
    default void writeNonFinite(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(ESQL_PROMQL_NON_FINITE_MATH)) {
            out.writeBoolean(allowNonFinite());
        }
    }

    /**
     * Reads the non-finite flag written by {@link #writeNonFinite(StreamOutput)}; versions that predate it never wrote
     * the byte and are treated as strict ({@code false}). This is {@code static} so it can be used from a delegating
     * {@code this(...)} constructor call, where instance methods are not yet available.
     */
    static boolean readNonFinite(StreamInput in) throws IOException {
        return in.getTransportVersion().supports(ESQL_PROMQL_NON_FINITE_MATH) && in.readBoolean();
    }
}
