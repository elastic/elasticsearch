/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.lucene.search.uhighlight;

public class QueryMaxAnalyzedOffset {
    private final int queryMaxAnalyzedOffset;

    private QueryMaxAnalyzedOffset(final int queryMaxAnalyzedOffset) {
        // If we have a negative value, grab value for the actual maximum from the index.
        this.queryMaxAnalyzedOffset = queryMaxAnalyzedOffset;
    }

    public static QueryMaxAnalyzedOffset create(final Integer queryMaxAnalyzedOffset, final int indexMaxAnalyzedOffset) {
        if (queryMaxAnalyzedOffset == null) {
            return null;
        }
        return new QueryMaxAnalyzedOffset(queryMaxAnalyzedOffset < 0 ? indexMaxAnalyzedOffset : queryMaxAnalyzedOffset);
    }

    public int getNotNull() {
        return queryMaxAnalyzedOffset;
    }
}
