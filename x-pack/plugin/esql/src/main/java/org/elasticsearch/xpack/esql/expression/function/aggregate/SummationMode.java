/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.aggregate;

import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * Specifies the summation algorithm to use for aggregating floating point values.
 */
public enum SummationMode {
    /**
     * The default mode in regular aggregations.
     * Uses Kahan summation for improved floating point precision.
     */
    COMPENSATED("compensated"),

    /**
     * The default mode in time-series aggregations.
     * Uses simple summation, allowing loss of precision for performance.
     */
    LOSSY("lossy")

    ;

    public static final Literal COMPENSATED_LITERAL = COMPENSATED.asLiteral();
    public static final Literal LOSSY_LITERAL = LOSSY.asLiteral();

    private final String mode;

    SummationMode(String mode) {
        this.mode = mode;
    }

    private Literal asLiteral() {
        return Literal.keyword(Source.EMPTY, mode);
    }

    public static SummationMode fromLiteral(Expression literal) {
        return fromString(BytesRefs.toString(literal.fold(FoldContext.small())));
    }

    private static SummationMode fromString(String mode) {
        return switch (mode) {
            case "compensated" -> COMPENSATED;
            case "lossy" -> LOSSY;
            default -> throw new IllegalArgumentException("unknown summation mode [" + mode + "]");
        };
    }
}
