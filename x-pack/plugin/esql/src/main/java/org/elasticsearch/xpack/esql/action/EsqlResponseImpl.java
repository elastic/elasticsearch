/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.xpack.core.esql.action.ColumnInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlClusterInfo;
import org.elasticsearch.xpack.core.esql.action.EsqlResponse;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

/** View over the response, that supports the xpack core transport API. */
public class EsqlResponseImpl implements EsqlResponse {

    private final EsqlQueryResponse queryResponse;
    private boolean closed;

    EsqlResponseImpl(EsqlQueryResponse queryResponse) {
        this.queryResponse = queryResponse;
    }

    @Override
    public List<? extends ColumnInfo> columns() {
        return queryResponse.columns();
    }

    @Override
    public Iterable<Iterable<Object>> rows() {
        ensureOpen();
        return () -> {
            ensureOpen();
            return new DelegatingIterator<>(queryResponse.rows().iterator());
        };
    }

    @Override
    public Iterable<Object> column(int columnIndex) {
        ensureOpen();
        return () -> {
            ensureOpen();
            return new DelegatingIterator<>(queryResponse.column(columnIndex));
        };
    }

    @Override
    public List<EsqlClusterInfo> clusters() {
        EsqlExecutionInfo info = queryResponse.getExecutionInfo();
        if (info == null || info.hasMetadataToReport() == false) {
            return List.of();
        }
        Stream<EsqlExecutionInfo.Cluster> clusters = info.getClusters().values().stream();
        if (info.includeExecutionMetadata() == EsqlExecutionInfo.IncludeExecutionMetadata.NEVER) {
            // As in the REST response: with no metadata requested, only partial-failure clusters are reported.
            clusters = clusters.filter(c -> c.getFailures().isEmpty() == false);
        }
        return clusters.map(
            c -> new EsqlClusterInfo(c.getClusterAlias(), c.getIndexExpression(), EsqlClusterInfo.Status.valueOf(c.getStatus().name()))
        ).sorted(Comparator.comparing(EsqlClusterInfo::clusterAlias)).toList();
    }

    @Override
    public void close() {
        setClosedState();
    }

    public void setClosedState() {
        closed = true;
    }

    private void ensureOpen() {
        if (closed || queryResponse.hasReferences() == false) {
            throw new IllegalStateException("closed");
        }
    }

    @Override
    public String toString() {
        return "EsqlResponse[response=" + queryResponse + "]";
    }

    /** A delegating iterator, that first checks the closed state before delegating. */
    final class DelegatingIterator<T> implements Iterator<T> {
        final Iterator<T> delegate;

        DelegatingIterator(Iterator<T> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            ensureOpen();
            return delegate.hasNext();
        }

        @Override
        public T next() {
            ensureOpen();
            return delegate.next();
        }
    }
}
