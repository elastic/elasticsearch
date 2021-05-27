/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.unit;

public enum SizeUnit {
    SINGLE {
        @Override
        public long toSingles(long size) {
            return size;
        }

        @Override
        public long toKilo(long size) {
            return size / (C1 / C0);
        }

        @Override
        public long toMega(long size) {
            return size / (C2 / C0);
        }

        @Override
        public long toGiga(long size) {
            return size / (C3 / C0);
        }

        @Override
        public long toTera(long size) {
            return size / (C4 / C0);
        }

        @Override
        public long toPeta(long size) {
            return size / (C5 / C0);
        }
    },
    KILO {
        @Override
        public long toSingles(long size) {
            return x(size, C1 / C0, MAX / (C1 / C0));
        }

        @Override
        public long toKilo(long size) {
            return size;
        }

        @Override
        public long toMega(long size) {
            return size / (C2 / C1);
        }

        @Override
        public long toGiga(long size) {
            return size / (C3 / C1);
        }

        @Override
        public long toTera(long size) {
            return size / (C4 / C1);
        }

        @Override
        public long toPeta(long size) {
            return size / (C5 / C1);
        }
    },
    MEGA {
        @Override
        public long toSingles(long size) {
            return x(size, C2 / C0, MAX / (C2 / C0));
        }

        @Override
        public long toKilo(long size) {
            return x(size, C2 / C1, MAX / (C2 / C1));
        }

        @Override
        public long toMega(long size) {
            return size;
        }

        @Override
        public long toGiga(long size) {
            return size / (C3 / C2);
        }

        @Override
        public long toTera(long size) {
            return size / (C4 / C2);
        }

        @Override
        public long toPeta(long size) {
            return size / (C5 / C2);
        }
    },
    GIGA {
        @Override
        public long toSingles(long size) {
            return x(size, C3 / C0, MAX / (C3 / C0));
        }

        @Override
        public long toKilo(long size) {
            return x(size, C3 / C1, MAX / (C3 / C1));
        }

        @Override
        public long toMega(long size) {
            return x(size, C3 / C2, MAX / (C3 / C2));
        }

        @Override
        public long toGiga(long size) {
            return size;
        }

        @Override
        public long toTera(long size) {
            return size / (C4 / C3);
        }

        @Override
        public long toPeta(long size) {
            return size / (C5 / C3);
        }
    },
    TERA {
        @Override
        public long toSingles(long size) {
            return x(size, C4 / C0, MAX / (C4 / C0));
        }

        @Override
        public long toKilo(long size) {
            return x(size, C4 / C1, MAX / (C4 / C1));
        }

        @Override
        public long toMega(long size) {
            return x(size, C4 / C2, MAX / (C4 / C2));
        }

        @Override
        public long toGiga(long size) {
            return x(size, C4 / C3, MAX / (C4 / C3));
        }

        @Override
        public long toTera(long size) {
            return size;
        }

        @Override
        public long toPeta(long size) {
            return size / (C5 / C0);
        }
    },
    PETA {
        @Override
        public long toSingles(long size) {
            return x(size, C5 / C0, MAX / (C5 / C0));
        }

        @Override
        public long toKilo(long size) {
            return x(size, C5 / C1, MAX / (C5 / C1));
        }

        @Override
        public long toMega(long size) {
            return x(size, C5 / C2, MAX / (C5 / C2));
        }

        @Override
        public long toGiga(long size) {
            return x(size, C5 / C3, MAX / (C5 / C3));
        }

        @Override
        public long toTera(long size) {
            return x(size, C5 / C4, MAX / (C5 / C4));
        }

        @Override
        public long toPeta(long size) {
            return size;
        }
    };

    static final long C0 = 1L;
    static final long C1 = C0 * 1000L;
    static final long C2 = C1 * 1000L;
    static final long C3 = C2 * 1000L;
    static final long C4 = C3 * 1000L;
    static final long C5 = C4 * 1000L;

    static final long MAX = Long.MAX_VALUE;

    /**
     * Scale d by m, checking for overflow.
     * This has a short name to make above code more readable.
     */
    static long x(long d, long m, long over) {
        if (d > over) return Long.MAX_VALUE;
        if (d < -over) return Long.MIN_VALUE;
        return d * m;
    }


    public abstract long toSingles(long size);

    public abstract long toKilo(long size);

    public abstract long toMega(long size);

    public abstract long toGiga(long size);

    public abstract long toTera(long size);

    public abstract long toPeta(long size);
}
