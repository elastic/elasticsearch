/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.core.Nullable;

/**
 * How a {@code POST /_query/stream} response is bounded: the body fields {@code max_rows} and {@code max_bytes}.
 * A value of {@code -1} means no limit. The {@code store} field is validated here but is not kept: only
 * {@code store: none} is supported, which is the behaviour of a response that has no storage behind it.
 *
 * @param maxRows  end the response after this many rows, or {@code -1}
 * @param maxBytes end the response after about this many bytes, checked between rows, or {@code -1}
 */
record StreamOptions(long maxRows, long maxBytes) {

    static final long NO_LIMIT = -1;
    static final String STORE_NONE = "none";

    /** No limits: the whole result is sent. */
    static final StreamOptions NONE = new StreamOptions(NO_LIMIT, NO_LIMIT);

    boolean hasRowLimit() {
        return maxRows != NO_LIMIT;
    }

    boolean hasByteLimit() {
        return maxBytes != NO_LIMIT;
    }

    /**
     * Whether a response that has written {@code rows} rows and {@code bytes} bytes so far must end now. The first row
     * of a response is always written, whatever {@code max_bytes} is, so a response with a byte limit always makes progress.
     */
    boolean reached(long rows, long bytes) {
        if (hasRowLimit() && rows >= maxRows) {
            return true;
        }
        return hasByteLimit() && rows > 0 && bytes >= maxBytes;
    }

    /** Collects the raw body values while parsing, then validates them together in {@link #build()}. */
    static final class Builder {
        // null means the field was not in the body; this keeps a user-supplied -1 distinguishable from "unset"
        private String store;
        private Long maxRows;
        private Long maxBytes;

        void store(@Nullable String store) {
            this.store = store;
        }

        void maxRows(long maxRows) {
            this.maxRows = maxRows;
        }

        void maxBytes(long maxBytes) {
            this.maxBytes = maxBytes;
        }

        StreamOptions build() {
            if (store != null && STORE_NONE.equals(store) == false) {
                throw new IllegalArgumentException("[store] only supports [" + STORE_NONE + "] for now, got [" + store + "]");
            }
            if (maxRows != null && maxRows < 0) {
                throw new IllegalArgumentException("[max_rows] must be greater than or equal to 0, got [" + maxRows + "]");
            }
            if (maxBytes != null && maxBytes <= 0) {
                throw new IllegalArgumentException("[max_bytes] must be greater than 0, got [" + maxBytes + "]");
            }
            if (maxRows == null && maxBytes == null) {
                return NONE;
            }
            return new StreamOptions(maxRows == null ? NO_LIMIT : maxRows, maxBytes == null ? NO_LIMIT : maxBytes);
        }
    }
}
