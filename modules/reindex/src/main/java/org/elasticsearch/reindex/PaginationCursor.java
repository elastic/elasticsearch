/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex;

import org.elasticsearch.core.Nullable;

/**
 * Cursor for paginating search results. Holds either a scroll ID (scroll-based pagination)
 * or search_after values (PIT-based pagination). Exactly one field is non-null.
 */
public record PaginationCursor(@Nullable String scrollId, @Nullable Object[] searchAfter) {

    public PaginationCursor {
        // Exactly one of scrollId or searchAfter must be non-null
        if ((scrollId == null && searchAfter == null) || (scrollId != null && searchAfter != null)) {
            throw new IllegalArgumentException("Exactly one of [scrollId] or [searchAfter] must be non-null");
        }
        if (scrollId != null && scrollId.isEmpty()) {
            throw new IllegalArgumentException("scrollId must not be empty");
        }
    }

    public static PaginationCursor forScroll(String scrollId) {
        if (scrollId == null) {
            throw new NullPointerException("scrollId must not be null");
        }
        if (scrollId.isEmpty()) {
            throw new IllegalArgumentException("scrollId must not be empty");
        }
        return new PaginationCursor(scrollId, null);
    }

    public static PaginationCursor forSearchAfter(Object[] searchAfter) {
        if (searchAfter == null) {
            throw new IllegalArgumentException("searchAfter must be non-null");
        }
        return new PaginationCursor(null, searchAfter);
    }

    public boolean isScroll() {
        return scrollId != null;
    }

    public boolean isSearchAfter() {
        return searchAfter != null;
    }
}
