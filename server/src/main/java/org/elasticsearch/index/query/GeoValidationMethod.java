/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
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
    COERCE, IGNORE_MALFORMED, STRICT;

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
        throw new IllegalArgumentException("operator needs to be either " + CollectionUtils.arrayAsArrayList(GeoValidationMethod.values())
                + ", but not [" + op + "]");
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
