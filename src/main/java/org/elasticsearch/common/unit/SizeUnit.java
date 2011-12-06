/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.common.unit;

/**
 *
 */
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
    };

    static final long C0 = 1L;
    static final long C1 = C0 * 1000L;
    static final long C2 = C1 * 1000L;
    static final long C3 = C2 * 1000L;

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


    public long toSingles(long size) {
        throw new AbstractMethodError();
    }

    public long toKilo(long size) {
        throw new AbstractMethodError();
    }

    public long toMega(long size) {
        throw new AbstractMethodError();
    }

    public long toGiga(long size) {
        throw new AbstractMethodError();
    }
}