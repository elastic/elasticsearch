/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.core.Nullable;

/**
 * Cursor for paginating search results. Holds either a scroll ID (scroll-based pagination)
 * or search_after values (PIT-based pagination). Exactly one field is non-null.
 */
public record PaginationCursor(@Nullable String scrollId, @Nullable Object[] searchAfter) {

    public static PaginationCursor forScroll(String scrollId) {
        return new PaginationCursor(scrollId, null);
    }

    public static PaginationCursor forSearchAfter(Object[] searchAfter) {
        return new PaginationCursor(null, searchAfter);
    }

    public boolean isScroll() {
        return scrollId != null;
    }

    public boolean isSearchAfter() {
        return searchAfter != null;
    }
}
