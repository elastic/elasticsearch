/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;

import java.io.IOException;

/**
 * Base class for query builders that produce a single Lucene query.
 * <p>
 * Implementations provide the query via {@link #doToQuery(SearchExecutionContext)} and this class
 * ensures the resulting query is visited by the provided {@link QueryVisitor}.
 *
 * <p>Charges the per-clause "constant cost" against the request circuit breaker on every leaf,
 * via {@link #estimateRamBytes(Query)}: the produced clause's {@code ramBytesUsed()}
 * when it implements {@link Accountable}, otherwise a shallow size plus {@link #LEAF_BASE_BYTES}
 * so non-accountable clauses (e.g. {@code TermQuery}) still contribute a non-zero floor.
 * Field-type-level charges for {@code prefix}/{@code wildcard}/{@code regexp}/{@code range}/
 * {@code fuzzy} continue to record each clause's actual {@code ramBytesUsed} at the point of
 * construction — that is what protects parsers like {@code QueryStringQueryParser} which
 * bypass {@code LeafQueryBuilder}. The conservative over-charge that results when both layers
 * fire on the same clause is intentional and consistent with treating the breaker as a
 * deliberately-pessimistic upper bound.
 */
public abstract class LeafQueryBuilder<QB extends LeafQueryBuilder<QB>> extends AbstractQueryBuilder<QB> {

    /**
     * Per-clause floor charged to the request circuit breaker for leaf queries that don't
     * implement {@link Accountable}.
     */
    static final long LEAF_BASE_BYTES = 256L;

    protected LeafQueryBuilder() {}

    protected LeafQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected final Query doToQuery(SearchExecutionContext context, MaxClauseCountQueryVisitor queryVisitor) throws IOException {
        Query query = doToQuery(context);
        if (query != null) {
            context.addCircuitBreakerMemory(estimateRamBytes(query), "clause:" + getName());
            query.visit(queryVisitor);
        }
        return query;
    }

    static long estimateRamBytes(Query query) {
        if (query instanceof Accountable a) {
            return a.ramBytesUsed();
        }
        return RamUsageEstimator.shallowSizeOf(query) + LEAF_BASE_BYTES;
    }

    protected abstract Query doToQuery(SearchExecutionContext context) throws IOException;
}
