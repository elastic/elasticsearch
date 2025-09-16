/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.selector;

import org.elasticsearch.core.TimeValue;

import java.time.Instant;
import java.util.Objects;

/**
 * Evaluation context for a PromQL selector, including the evaluation time and any offset to apply.
 * The evaluation time is passed through the promql API while the rest of the parameters through the query
 * directly.
 *
 * &lt;implicit&gt; offset &lt;optional_offset&gt; @ &lt;optional_at&gt;
 */
public class Evaluation {
    public static final Evaluation NONE = new Evaluation(TimeValue.ZERO, false, null);

    private final TimeValue offset;
    private final boolean offsetNegative;
    private final Instant at;

    public Evaluation(Instant at) {
        this(TimeValue.ZERO, false, at);
    }

    public Evaluation(TimeValue offset, boolean offsetNegative, Instant at) {
        this.offset = offset;
        this.offsetNegative = offsetNegative;
        this.at = at;
    }

    public TimeValue offset() {
        return offset;
    }

    public boolean offsetNegative() {
        return offsetNegative;
    }

    public Instant at() {
        return at;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Evaluation that = (Evaluation) o;
        return offsetNegative == that.offsetNegative && Objects.equals(offset, that.offset) && Objects.equals(at, that.at);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, offsetNegative, at);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (offset != null && offset.equals(TimeValue.ZERO) == false) {
            sb.append("offset ");
            if (offsetNegative) {
                sb.append("-");
            }
            sb.append(offset);
        }
        if (at != null) {
            if (sb.length() > 0) {
                sb.append(" ");
            }
            sb.append("@ ").append(at);
        }
        return sb.length() > 0 ? sb.toString() : "";
    }
}
