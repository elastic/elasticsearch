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
 * <p>Ids are frozen once shipped, so a layout added later arrives as a new id and already-written segments
 * go on decoding unchanged. That is why the id is recorded while only one layout exists.
 */
public enum StringColumnLayout {

    /** The values themselves, in written order, in one {@link ValueStream}. */
    PLAIN((byte) 0),

    /**
     * An ordinal per value into a sorted dictionary of the terms the column repeats. The terms are held in
     * their own {@link ValueStream}, read by ordinal and so left uncompressed; the ordinals are a numeric
     * column, packed to the width the dictionary actually needs.
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
