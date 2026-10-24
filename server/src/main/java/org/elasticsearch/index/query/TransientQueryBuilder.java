/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * A query builder that is never serialized nor parsed. It can only be built by rewrites that
 * {@link QueryRewriteContext#allowsTransientQueries() allow transient queries}.
 */
public abstract class TransientQueryBuilder<QB extends TransientQueryBuilder<QB>> extends AbstractQueryBuilder<QB> {

    @Override
    protected final QueryBuilder doRewrite(QueryRewriteContext context) throws IOException {
        assert context == null || context.allowsTransientQueries() : "[" + getName() + "] is rewritten outside of a transient rewrite";
        return doTransientRewrite(context);
    }

    protected abstract QueryBuilder doTransientRewrite(QueryRewriteContext context) throws IOException;

    @Override
    protected final void doWriteTo(StreamOutput out) {
        throw new IllegalStateException("[" + getName() + "] is never serialized");
    }

    @Override
    public final TransportVersion getMinimalSupportedVersion() {
        return TransportVersion.minimumCompatible();
    }
}
