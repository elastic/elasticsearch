/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

public interface LongDoubleArray extends BigArray { // TODO: implement Writeable

    /**
     * Get the position long element given its index.
     */
    long getLong(long index);

    /**
     * Get the position double element given its index.
     */
    double getDouble(long index);

    /**
     * Set a triple-value at the given index.
     * TODO: we could return a MH that allows retrieval of the previous value.
     */
    void set(long index, long lValue, double dValue);

    /**
     * Bulk set.
     */
    void set(long index, byte[] buf, int offset, int len);

    // /** Opaque index into tuple element arrays. */
    // final class OpaqueIndex {
    // int pageIndex;
    // int byteIndex;
    //
    // public OpaqueIndex() {}
    //
    // public void setForIndex(long index) {
    // pageIndex = pageIndex(index);
    // byteIndex = byteIndex(indexInPage(index));
    // }
    //
    // // 9 511
    // private static int pageIndex(long index) {
    // // return (int) (index / BigLongDoubleDoubleArray.ELEMENTS_PER_PAGE);
    // return (int) (index >> 9);
    // }
    //
    // private static int indexInPage(long index) {
    // // return (int) (index % BigLongDoubleDoubleArray.ELEMENTS_PER_PAGE);
    // return (int) (index & 0x1FF);
    // }
    //
    // private static int byteIndex(int indexInPage) {
    // return (indexInPage << 4) + (indexInPage << 3);
    // }
    //
    // @Override
    // public String toString() {
    // return "LongDoubleDoubleArray$OpaqueIndex[pageIndex=" + pageIndex + ", byteIndex=" + byteIndex + "]";
    // }
    // }
}
