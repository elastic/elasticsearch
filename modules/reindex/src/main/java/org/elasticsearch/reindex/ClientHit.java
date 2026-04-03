/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.mapper.RoutingFieldMapper;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.xcontent.XContentType;

/**
 * Implementation of {@link PaginatedHitSource.Hit} that wraps a {@link SearchHit} from a local
 * {@link org.elasticsearch.client.internal.Client} search. Shared by scroll-based and PIT-based
 * paginated hit sources.
 */

class ClientHit implements PaginatedHitSource.Hit {
    private final SearchHit delegate;
    private final BytesReference source;

    ClientHit(SearchHit delegate) {
        this.delegate = delegate.asUnpooled(); // TODO: use pooled version here
        source = this.delegate.hasSource() ? this.delegate.getSourceRef() : null;
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
        return fieldValue(RoutingFieldMapper.NAME);
    }

    @Override
    public Object[] getSortValues() {
        return delegate.getSortValues();
    }

    private <T> T fieldValue(String fieldName) {
        DocumentField field = delegate.field(fieldName);
        return field == null ? null : field.getValue();
    }
}
