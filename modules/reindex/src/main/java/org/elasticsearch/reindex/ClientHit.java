/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.SliceIndexing;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.XContentType;

import java.util.function.Supplier;

/**
 * Implementation of {@link PaginatedHitSource.Hit} that wraps a {@link SearchHit} from a local
 * {@link org.elasticsearch.client.internal.Client} search. Shared by scroll-based and PIT-based
 * paginated hit sources. Callers must invoke {@link #release()} when the hit is no longer needed.
 */

class ClientHit implements PaginatedHitSource.Hit {
    /**
     * Charges a local page's source bytes to {@code breaker} until the action releases the page, since its hits pin the transport
     * buffers past the search-side charges. Like {@code RemoteParseContext} for remote. Pages below {@code thresholdBytes} aren't
     * charged, so a very high {@code cluster.reindex.memory_accounting_threshold} turns this off. If the coordinator already charged
     * the hits ({@code fetch[coordinator]}), that charge is taken over instead of charging them again.
     *
     * @throws CircuitBreakingException if the page does not fit, after releasing the hits
     */
    static void reserveHeldPage(
        CircuitBreaker breaker,
        String label,
        long thresholdBytes,
        Supplier<Releasable> coordinatorCharge,
        PaginatedHitSource.Response response
    ) {
        long bytes = 0;
        for (PaginatedHitSource.Hit hit : response.getHits()) {
            BytesReference source = hit.getSource();
            if (source != null) {
                bytes += source.length();
            }
        }
        if (bytes == 0 || bytes < thresholdBytes) {
            return;
        }
        Releasable adopted = coordinatorCharge.get();
        if (adopted != null) {
            response.setBodyReleasable(adopted);
            return;
        }
        try {
            breaker.addEstimateBytesAndMaybeBreak(bytes, label);
        } catch (CircuitBreakingException e) {
            response.getHits().forEach(PaginatedHitSource.Hit::release);
            throw e;
        }
        final long reserved = bytes;
        response.setBodyReleasable(() -> breaker.addWithoutBreaking(-reserved));
    }

    private final SearchHit delegate;
    private final BytesReference source;

    ClientHit(SearchHit delegate) {
        delegate.mustIncRef();
        this.delegate = delegate;
        source = delegate.hasSource() ? delegate.getSourceRef() : null;
    }

    @Override
    public String getIndex() {
        return delegate.getIndex();
    }

    @Override
    public String getId() {
        return delegate.getId();
    }

    @Override
    public BytesReference getSource() {
        return source;
    }

    @Override
    public XContentType getXContentType() {
        return source != null ? XContentHelper.xContentType(source) : null;
    }

    @Override
    public long getVersion() {
        return delegate.getVersion();
    }

    @Override
    public long getSeqNo() {
        return delegate.getSeqNo();
    }

    @Override
    public long getPrimaryTerm() {
        return delegate.getPrimaryTerm();
    }

    @Override
    public String getRouting() {
        // A slice-enabled index never exposes _routing; it surfaces the routing value as _slice instead.
        String routing = fieldValue(RoutingFieldMapper.NAME);
        return routing != null ? routing : fieldValue(SliceIndexing.FIELD_NAME);
    }

    @Override
    public Object[] getSortValues() {
        return delegate.getSortValues();
    }

    private <T> T fieldValue(String fieldName) {
        DocumentField field = delegate.field(fieldName);
        return field == null ? null : field.getValue();
    }

    @Override
    public void release() {
        delegate.decRef();
    }
}
