/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.compute.operator.BreakingBytesRefBuilder;

/**
 * Encodes values for {@link TopNOperator}. Some encoders encode values so sorting
 * the bytes will sort the values. This is called "sortable" and you can always
 * go from any {@link TopNEncoder} to a "sortable" version of it with {@link #toSortable()}.
 * If you don't need the bytes to be sortable you can get an "unsortable" encoder
 * with {@link #toUnsortable()}.
 */
public interface TopNEncoder {
    /**
     * An encoder that encodes values such that sorting the bytes sorts the values.
     */
    DefaultSortableTopNEncoder DEFAULT_SORTABLE = new DefaultSortableTopNEncoder();
    /**
     * An encoder that encodes values as compactly as possible without making the
     * encoded bytes sortable.
     */
    DefaultUnsortableTopNEncoder DEFAULT_UNSORTABLE = new DefaultUnsortableTopNEncoder();
    /**
     * An encoder for IP addresses.
     */
    FixedLengthTopNEncoder IP = new FixedLengthTopNEncoder(InetAddressPoint.BYTES);
    /**
     * An encoder for UTF-8 text.
     */
    UTF8TopNEncoder UTF8 = new UTF8TopNEncoder();
    /**
     * An encoder for semver versions.
     */
    VersionTopNEncoder VERSION = new VersionTopNEncoder();

    /**
     * Placeholder encoder for unsupported data types.
     */
    UnsupportedTypesTopNEncoder UNSUPPORTED = new UnsupportedTypesTopNEncoder();

    void encodeLong(long value, BreakingBytesRefBuilder bytesRefBuilder);

    long decodeLong(BytesRef bytes);

    void encodeInt(int value, BreakingBytesRefBuilder bytesRefBuilder);

    int decodeInt(BytesRef bytes);

    void encodeFloat(float value, BreakingBytesRefBuilder bytesRefBuilder);

    float decodeFloat(BytesRef bytes);

    void encodeDouble(double value, BreakingBytesRefBuilder bytesRefBuilder);

    double decodeDouble(BytesRef bytes);

    void encodeBoolean(boolean value, BreakingBytesRefBuilder bytesRefBuilder);

    boolean decodeBoolean(BytesRef bytes);

    int encodeBytesRef(BytesRef value, BreakingBytesRefBuilder bytesRefBuilder);

    BytesRef decodeBytesRef(BytesRef bytes, BytesRef scratch);

    /**
     * Get a version of this encoder that encodes values such that sorting
     * the encoded bytes sorts by the values.
     */
    TopNEncoder toSortable();

    /**
     * Get a version of this encoder that encodes values as fast as possible
     * without making the encoded bytes sortable.
     */
    TopNEncoder toUnsortable();
}
