/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public enum MissingBucket implements Writeable {
    /**
     * Documents with missing values are ignored and no bucket is created.
     */
    IGNORE {
        @Override
        public int compareAnyValueToMissing(int reverseMul) {
            return reverseMul;
        }

        @Override
        public String toString() {
            return "_ignore";
        }
    },
    /**
     * Create a bucket for documents with missing values and place it first if the source uses ASC ordering or last otherwise.
     */
    INCLUDE {
        @Override
        public int compareAnyValueToMissing(int reverseMul) {
            return reverseMul;
        }

        @Override
        public String toString() {
            return "_include";
        }
    },
    /**
     * Create a bucket for documents with missing values and always place it first.
     */
    FIRST {
        @Override
        public int compareAnyValueToMissing(int reverseMul) {
            return 1;
        }

        @Override
        public String toString() {
            return "_first";
        }
    },
    /**
     * Create a bucket for documents with missing values and always place it last.
     */
    LAST {
        @Override
        public int compareAnyValueToMissing(int reverseMul) {
            return -1;
        }

        @Override
        public String toString() {
            return "_last";
        }
    };

    public boolean include() {
        return this != IGNORE;
    }

    public abstract int compareAnyValueToMissing(int reverseMul);

    public static MissingBucket readFromStream(StreamInput in) throws IOException {
        return in.readEnum(MissingBucket.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    public static MissingBucket fromString(String op) {
        switch (op) {
            case "_ignore":
                return IGNORE;
            case "_include":
                return INCLUDE;
            case "_first":
                return FIRST;
            case "_last":
                return LAST;
            default:
                throw new IllegalArgumentException(
                    "Unknown value [" + op + "] for missing_bucket, expected one of _first, _last, _include, _ignore"
                );
        }
    }
}
