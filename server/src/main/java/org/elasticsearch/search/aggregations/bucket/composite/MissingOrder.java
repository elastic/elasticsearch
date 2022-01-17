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
import java.util.Locale;

public enum MissingOrder implements Writeable {
    /**
     * Place buckets for missing values first if the source uses ASC ordering or last otherwise.
     */
    DEFAULT {
        @Override
        public int compareAnyValueToMissing(int reverseMul) {
            return reverseMul;
        }

        @Override
        public String toString() {
            return "default";
        }
    },
    /**
     * Place buckets for missing values first.
     */
    FIRST {
        @Override
        public int compareAnyValueToMissing(int reverseMul) {
            return 1;
        }

        @Override
        public String toString() {
            return "first";
        }
    },
    /**
     * Place buckets for missing values last.
     */
    LAST {
        @Override
        public int compareAnyValueToMissing(int reverseMul) {
            return -1;
        }

        @Override
        public String toString() {
            return "last";
        }
    };

    public abstract int compareAnyValueToMissing(int reverseMul);

    public static MissingOrder readFromStream(StreamInput in) throws IOException {
        return in.readEnum(MissingOrder.class);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    public static MissingOrder fromString(String op) {
        return valueOf(op.toUpperCase(Locale.ROOT));
    }
}
