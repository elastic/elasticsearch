/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.expression.function.aggregate.TimeSeriesAggregateFunction;

/**
 * Marker interface for {@link TimeSeriesAggregateFunction}s to identify classes of functions that operate
 * on the {code @timestamp} and the temporality field of an index.
 * Implementations of this interface need to expect the associated {@code Attribute}s to be passed after all regular arguments.
 * The {code @timestamp} will be passed first, followed by the temporality.
 */
public interface TemporalityAware extends TimestampAware {

    enum Temporality {
        CUMULATIVE("cumulative"),
        DELTA("delta");

        private final Literal literal;
        private final BytesRef bytesRef;

        Temporality(String name) {
            this.bytesRef = new BytesRef(name);
            this.literal = Literal.keyword(Source.EMPTY, name);
        }

        public Literal literal() {
            return literal;
        }

        public BytesRef bytesRef() {
            return bytesRef;
        }
    }

    Literal TEMPORALITY_UNSUPPORTED_MARKER = Literal.keyword(Source.EMPTY, "temporality_unsupported");

    Expression checkTemporalitySupport(TransportVersion version);

    Expression temporality();

    Temporality defaultTemporality();

    TimeSeriesAggregateFunction withTemporality(Expression injectedTemporality);
}
