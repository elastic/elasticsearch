/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.unit;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

/**
 * A {@code SizeUnit} represents size at a given unit of
 * granularity and provides utility methods to convert across units.
 * A {@code SizeUnit} does not maintain size information, but only
 * helps organize and use size representations that may be maintained
 * separately across various contexts.
 */
public enum ByteSizeUnit implements Writeable {
    BYTES {
        @Override
        public long toBytes(long size) {
            return size;
        }

        @Override
        public long toKB(long size) {
            return size / (C1 / C0);
        }

        @Override
        public long toMB(long size) {
            return size / (C2 / C0);
        }

        @Override
        public long toGB(long size) {
            return size / (C3 / C0);
        }

        @Override
        public long toTB(long size) {
            return size / (C4 / C0);
        }

        @Override
        public long toPB(long size) {
            return size / (C5 / C0);
        }

        @Override
        public String getSuffix() {
            return "b";
        }
    },
    KB {
        @Override
        public long toBytes(long size) {
            return x(size, C1 / C0, MAX / (C1 / C0));
        }

        @Override
        public long toKB(long size) {
            return size;
        }

        @Override
        public long toMB(long size) {
            return size / (C2 / C1);
        }

        @Override
        public long toGB(long size) {
            return size / (C3 / C1);
        }

        @Override
        public long toTB(long size) {
            return size / (C4 / C1);
        }

        @Override
        public long toPB(long size) {
            return size / (C5 / C1);
        }

        @Override
        public String getSuffix() {
            return "kb";
        }
    },
    MB {
        @Override
        public long toBytes(long size) {
            return x(size, C2 / C0, MAX / (C2 / C0));
        }

        @Override
        public long toKB(long size) {
            return x(size, C2 / C1, MAX / (C2 / C1));
        }

        @Override
        public long toMB(long size) {
            return size;
        }

        @Override
        public long toGB(long size) {
            return size / (C3 / C2);
        }

        @Override
        public long toTB(long size) {
            return size / (C4 / C2);
        }

        @Override
        public long toPB(long size) {
            return size / (C5 / C2);
        }

        @Override
        public String getSuffix() {
            return "mb";
        }
    },
    GB {
        @Override
        public long toBytes(long size) {
            return x(size, C3 / C0, MAX / (C3 / C0));
        }

        @Override
        public long toKB(long size) {
            return x(size, C3 / C1, MAX / (C3 / C1));
        }

        @Override
        public long toMB(long size) {
            return x(size, C3 / C2, MAX / (C3 / C2));
        }

        @Override
        public long toGB(long size) {
            return size;
        }

        @Override
        public long toTB(long size) {
            return size / (C4 / C3);
        }

        @Override
        public long toPB(long size) {
            return size / (C5 / C3);
        }

        @Override
        public String getSuffix() {
            return "gb";
        }
    },
    TB {
        @Override
        public long toBytes(long size) {
            return x(size, C4 / C0, MAX / (C4 / C0));
        }

        @Override
        public long toKB(long size) {
            return x(size, C4 / C1, MAX / (C4 / C1));
        }

        @Override
        public long toMB(long size) {
            return x(size, C4 / C2, MAX / (C4 / C2));
        }

        @Override
        public long toGB(long size) {
            return x(size, C4 / C3, MAX / (C4 / C3));
        }

        @Override
        public long toTB(long size) {
            return size;
        }

        @Override
        public long toPB(long size) {
            return size / (C5 / C4);
        }

        @Override
        public String getSuffix() {
            return "tb";
        }
    },
    PB {
        @Override
        public long toBytes(long size) {
            return x(size, C5 / C0, MAX / (C5 / C0));
        }

        @Override
        public long toKB(long size) {
            return x(size, C5 / C1, MAX / (C5 / C1));
        }

        @Override
        public long toMB(long size) {
            return x(size, C5 / C2, MAX / (C5 / C2));
        }

        @Override
        public long toGB(long size) {
            return x(size, C5 / C3, MAX / (C5 / C3));
        }

        @Override
        public long toTB(long size) {
            return x(size, C5 / C4, MAX / (C5 / C4));
        }

        @Override
        public long toPB(long size) {
            return size;
        }

        @Override
        public String getSuffix() {
            return "pb";
        }
    };

    static final long C0 = 1L;
    static final long C1 = C0 * 1024L;
    static final long C2 = C1 * 1024L;
    static final long C3 = C2 * 1024L;
    static final long C4 = C3 * 1024L;
    static final long C5 = C4 * 1024L;

    static final long MAX = Long.MAX_VALUE;

    public static ByteSizeUnit fromId(int id) {
        if (id < 0 || id >= values().length) {
            throw new IllegalArgumentException("No byte size unit found for id [" + id + "]");
        }
        return values()[id];
    }

    /**
     * Scale d by m, checking for overflow.
     * This has a short name to make above code more readable.
     */
    static long x(long d, long m, long over) {
        if (d > over) return Long.MAX_VALUE;
        if (d < -over) return Long.MIN_VALUE;
        return d * m;
    }

    /**
     * Convert to an {@code int} number of bytes. Callers are expected to be certain this will not overflow.
     * @throws IllegalArgumentException on overflow, unless assertions are enabled in which case it throws an {@link AssertionError}.
     * @return The number of bytes represented as an {@code int}.
     */
    public final int toIntBytes(long size) {
        final long l = toBytes(size);
        final int i = (int) l;
        if (i != l) {
            final String message = "could not convert [" + size + " " + this + "] to an int";
            assert false : message;
            throw new IllegalArgumentException(message);
        }
        return i;
    }

    public abstract long toBytes(long size);

    public abstract long toKB(long size);

    public abstract long toMB(long size);

    public abstract long toGB(long size);

    public abstract long toTB(long size);

    public abstract long toPB(long size);

    public abstract String getSuffix();

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(this.ordinal());
    }

    /**
     * Reads a {@link ByteSizeUnit} from a given {@link StreamInput}
     */
    public static ByteSizeUnit readFrom(StreamInput in) throws IOException {
        return ByteSizeUnit.fromId(in.readVInt());
    }
}
