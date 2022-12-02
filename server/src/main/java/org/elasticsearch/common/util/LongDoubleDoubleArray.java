/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

public interface LongDoubleDoubleArray extends BigArray { // TODO: implement Writeable

    /**
     * Get the position-0 long element given its index.
     */
    long getLong0(OpaqueIndex index);

    /**
     * Get the position-0 double element given its index.
     */
    double getDouble0(OpaqueIndex index);

    /**
     * Get the position-1 double element given its index.
     */
    double getDouble1(OpaqueIndex index);

    /**
     * Set a triple-value at the given index.
     * TODO: we could return a MH that allows retrieval of the previous value.
     */
    void set(OpaqueIndex index, long lValue0, double dValue0, double dValue1);

    void increment(OpaqueIndex index, long lValue0Inc, double dValue0Inc, double dValue1Inc);

    /**
     * Get the position-0 long element given its index.
     */
    void get(OpaqueIndex index, Holder holder);

    /** Opaque index into tuple element arrays. */
    final class OpaqueIndex {
        int pageIndex;
        int byteIndex;

        public OpaqueIndex() {}

        public void setForIndex(long index) {
            pageIndex = pageIndex(index);
            byteIndex = byteIndex(indexInPage(index));
        }

        private static int pageIndex(long index) {
            return (int) (index / BigLongDoubleDoubleArray.ELEMENTS_PER_PAGE);
        }

        private static int indexInPage(long index) {
            return (int) (index % BigLongDoubleDoubleArray.ELEMENTS_PER_PAGE);
        }

        private static int byteIndex(int indexInPage) {
            return (indexInPage << 4) + (indexInPage << 3);
        }

        @Override
        public String toString() {
            return "LongDoubleDoubleArray$OpaqueIndex[pageIndex=" + pageIndex + ", byteIndex=" + byteIndex + "]";
        }
    }

    /** Dumb POJO. */
    final class Holder {
        private long lValue0;
        private double dValue0;
        private double dValue1;

        public Holder() {}

        public Holder(long lValue0, double dValue0, double dValue1) {
            this.lValue0 = lValue0;
            this.dValue0 = dValue0;
            this.dValue1 = dValue1;
        }

        public long getLong0() {
            return lValue0;
        }

        public void setLong0(long value) {
            lValue0 = value;
        }

        public double getDouble0() {
            return dValue0;
        }

        public void setDouble0(double value) {
            dValue0 = value;
        }

        public double getDouble1() {
            return dValue1;
        }

        public void setDouble1(double value) {
            dValue1 = value;
        }

        @Override
        public String toString() {
            return "LongDoubleDoubleArray$Holder[lValue0=" + lValue0 + ", dValue0=" + dValue0 + ", dValue1=" + dValue1 + "]";
        }
    }

    // TODO: considering adding:
    // - fill
    // - bulk byte[] set

}
