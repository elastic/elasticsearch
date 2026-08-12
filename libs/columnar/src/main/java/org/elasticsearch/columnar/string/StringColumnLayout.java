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
 * How a string column stores its values, decided per segment from that segment's cardinality and recorded
 * in {@link StringColumnMetadata}. This is a codec-internal decision: both layouts are served at the same
 * binary surface, so the layer above never sees which one a segment picked — in particular ordinals never
 * surface.
 *
 * <p>Ids are frozen once shipped. A further layout (prefix compression, for instance) arrives as a new id
 * and leaves already-written segments decoding unchanged.
 */
public enum StringColumnLayout {

    /** Values stored directly, {@code [VInt length][bytes]} per value. Chosen for high-cardinality segments. */
    PLAIN((byte) 0),

    /**
     * A per-segment terms dictionary plus one ordinal per value, the ordinals run through the numeric
     * encoder pipeline. Chosen when the segment's distinct-value count fits
     * {@link StringDictionary#MAX_SIZE}.
     */
    DICTIONARY((byte) 1);

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
