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
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;

import java.io.IOException;

/**
 * Base class for query builders that produce a single Lucene query.
 * <p>
 * Implementations provide the query via {@link #doToQuery(SearchExecutionContext)} and this class
 * ensures the resulting query is visited by the provided {@link QueryVisitor}.
 *
 * <p>Charges the per-clause "constant cost" against the request circuit breaker on every leaf,
 * via {@link Queries#estimateRamBytes(Query)}: the produced clause's {@code ramBytesUsed()}
 * when it implements {@link org.apache.lucene.util.Accountable}, otherwise a shallow size plus
 * {@link Queries#LEAF_BASE_BYTES} so non-accountable clauses (e.g. {@code TermQuery}) still
 * contribute a non-zero floor. Field-type-level charges for {@code prefix}/{@code wildcard}/
 * {@code regexp}/{@code range}/{@code fuzzy} continue to record each clause's actual
 * {@code ramBytesUsed} at the point of construction — that is what protects parsers like
 * {@code QueryStringQueryParser} which bypass {@code LeafQueryBuilder}. The conservative
 * over-charge that results when both layers fire on the same clause is intentional and
 * consistent with treating the breaker as a deliberately-pessimistic upper bound.
 */
public abstract class LeafQueryBuilder<QB extends LeafQueryBuilder<QB>> extends AbstractQueryBuilder<QB> {

    protected LeafQueryBuilder() {

    }

    protected LeafQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected final Query doToQuery(SearchExecutionContext context, MaxClauseCountQueryVisitor queryVisitor) throws IOException {
        Query query = doToQuery(context);
        if (query != null) {
            context.addCircuitBreakerMemory(Queries.estimateRamBytes(query), "clause:" + getName());
            query.visit(queryVisitor);
        }
        return query;
    }

    protected abstract Query doToQuery(SearchExecutionContext context) throws IOException;
}
