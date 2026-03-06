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
import org.elasticsearch.search.internal.MaxClauseCountQueryVisitor;

import java.io.IOException;

/**
 * Base class for query builders that produce a single Lucene query.
 * <p>
 * Implementations provide the query via {@link #doToQuery(SearchExecutionContext)} and this class
 * ensures the resulting query is visited by the provided {@link QueryVisitor}.
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
            query.visit(queryVisitor);
        }
        return query;
    }

    protected abstract Query doToQuery(SearchExecutionContext context) throws IOException;
}
