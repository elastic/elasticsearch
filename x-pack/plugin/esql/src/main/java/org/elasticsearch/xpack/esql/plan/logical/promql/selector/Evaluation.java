/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.promql.selector;

import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;

import java.time.Duration;
import java.util.Objects;

/**
 * Evaluation context for a PromQL selector, including the evaluation time and any offset to apply.
 * The evaluation time is passed through the promql API while the rest of the parameters through the query
 * directly.
 *
 * &lt;implicit&gt; offset &lt;optional_offset&gt; @ &lt;optional_at&gt;
 */
public class Evaluation {
    public static final Evaluation NONE = new Evaluation(new Literal(Source.EMPTY, Duration.ZERO, DataType.TIME_DURATION), Literal.NULL);

    private final Literal offset;
    private final Literal at;

    public Evaluation(Literal offset, Literal at) {
        this.offset = offset;
        this.at = at;
    }

    public Literal offset() {
        return offset;
    }

    /**
     * The offset modifier value.
     */
    public Duration offsetDuration() {
        return offset == null || offset.value() == null ? Duration.ZERO : (Duration) offset.value();
    }

    public Literal at() {
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
        return Objects.equals(offset, that.offset) && Objects.equals(at, that.at);
    }

    @Override
    public int hashCode() {
        return Objects.hash(offset, at);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        if (offsetDuration().isZero() == false) {
            sb.append("offset ").append(offset);
        }
        if (at != null) {
            if (sb.isEmpty() == false) {
                sb.append(" ");
            }
            sb.append("@ ").append(at);
        }
        return sb.toString();
    }
}
