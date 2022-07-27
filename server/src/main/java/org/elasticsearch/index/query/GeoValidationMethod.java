/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.query;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.CollectionUtils;

import java.io.IOException;

/**
 * This enum is used to determine how to deal with invalid geo coordinates in geo related
 * queries:
 *
 *  On STRICT validation invalid coordinates cause an exception to be thrown.
 *  On IGNORE_MALFORMED invalid coordinates are being accepted.
 *  On COERCE invalid coordinates are being corrected to the most likely valid coordinate.
 * */
public enum GeoValidationMethod implements Writeable {
    COERCE,
    IGNORE_MALFORMED,
    STRICT;

    public static final GeoValidationMethod DEFAULT = STRICT;
    public static final boolean DEFAULT_LENIENT_PARSING = (DEFAULT != STRICT);

    public static GeoValidationMethod readFromStream(StreamInput in) throws IOException {
        return GeoValidationMethod.values()[in.readVInt()];
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(this.ordinal());
    }

    public static GeoValidationMethod fromString(String op) {
        for (GeoValidationMethod method : GeoValidationMethod.values()) {
            if (method.name().equalsIgnoreCase(op)) {
                return method;
            }
        }
        throw new IllegalArgumentException(
            "operator needs to be either " + CollectionUtils.arrayAsArrayList(GeoValidationMethod.values()) + ", but not [" + op + "]"
        );
    }

    /** Returns whether or not to skip bounding box validation. */
    public static boolean isIgnoreMalformed(GeoValidationMethod method) {
        return (method == GeoValidationMethod.IGNORE_MALFORMED || method == GeoValidationMethod.COERCE);
    }

    /** Returns whether or not to try and fix broken/wrapping bounding boxes. */
    public static boolean isCoerce(GeoValidationMethod method) {
        return method == GeoValidationMethod.COERCE;
    }

    /** Returns validation method corresponding to given coerce and ignoreMalformed values. */
    public static GeoValidationMethod infer(boolean coerce, boolean ignoreMalformed) {
        if (coerce) {
            return GeoValidationMethod.COERCE;
        } else if (ignoreMalformed) {
            return GeoValidationMethod.IGNORE_MALFORMED;
        } else {
            return GeoValidationMethod.STRICT;
        }
    }

}
