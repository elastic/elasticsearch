/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

/**
 * How a string column stores its values, recorded in {@link StringColumnMetadata}. Which layout a segment used
 * is codec-internal: every layout is served at the same binary surface, so the layer above never sees the
 * choice.
 *
 * <p>Ids are frozen once shipped, and a new layout arrives as a new id leaving already-written segments
 * decoding unchanged. That is the point of recording the id while only one layout exists: an ordinal layout,
 * decided at merge from statistics a flush emits, arrives without a format bump. See {@code docs/PLAN.md}.
 */
public enum StringColumnLayout {

    /** Values stored directly, {@code [VInt length][bytes]} per value. */
    PLAIN((byte) 0);

    private final byte id;

    StringColumnLayout(byte id) {
        this.id = id;
    }

    /** The stable on-disk id; frozen once shipped. */
    public byte id() {
        return id;
    }

    public static StringColumnLayout fromId(byte id) {
        for (StringColumnLayout layout : values()) {
            if (layout.id == id) {
                return layout;
            }
        }
        throw new IllegalArgumentException("unknown ColumNAR string column layout id [" + id + "]");
    }
}
