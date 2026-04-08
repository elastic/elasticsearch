/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator.topn;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.PagedBytesBuilder;
import org.elasticsearch.common.bytes.PagedBytesCursor;

/**
 * Encodes values for {@link TopNOperator}. Some encoders encode values so sorting
 * the bytes will sort the values. This is called "sortable" and you can always
 * go from any {@link TopNEncoder} to a "sortable" version of it with {@link #toSortable}.
 * If you don't need the bytes to be sortable you can get an "unsortable" encoder
 * with {@link #toUnsortable()}.
 */
public interface TopNEncoder {
    /**
     * An encoder that encodes values such that sorting the bytes sorts the values.
     */
    DefaultSortableAscTopNEncoder DEFAULT_SORTABLE = new DefaultSortableAscTopNEncoder();

    /**
     * An encoder that encodes values as compactly as possible without making the
     * encoded bytes sortable.
     */
    DefaultUnsortableTopNEncoder DEFAULT_UNSORTABLE = new DefaultUnsortableTopNEncoder();

    /**
     * An encoder for IP addresses.
     */
    FixedLengthAscTopNEncoder IP = new FixedLengthAscTopNEncoder(InetAddressPoint.BYTES);

    /**
     * An encoder for UTF-8 text.
     */
    Utf8AscTopNEncoder UTF8 = new Utf8AscTopNEncoder();

    /**
     * An encoder for semver versions.
     */
    VersionAscTopNEncoder VERSION = new VersionAscTopNEncoder();

    /**
     * Placeholder encoder for unsupported data types.
     */
    UnsupportedTypesTopNEncoder UNSUPPORTED = new UnsupportedTypesTopNEncoder();

    void encodeLong(long value, PagedBytesBuilder builder);

    long decodeLong(PagedBytesCursor bytes);

    void encodeInt(int value, PagedBytesBuilder builder);

    int decodeInt(PagedBytesCursor bytes);

    void encodeFloat(float value, PagedBytesBuilder builder);

    float decodeFloat(PagedBytesCursor bytes);

    void encodeDouble(double value, PagedBytesBuilder builder);

    double decodeDouble(PagedBytesCursor bytes);

    void encodeBoolean(boolean value, PagedBytesBuilder builder);

    boolean decodeBoolean(PagedBytesCursor bytes);

    void encodeBytesRef(BytesRef value, PagedBytesBuilder builder);

    PagedBytesCursor decodeBytesRef(PagedBytesCursor cursor, PagedBytesCursor scratch);

    /**
     * Get a version of this encoder that encodes values such that sorting
     * the encoded bytes sorts by the values.
     */
    TopNEncoder toSortable(boolean asc);

    /**
     * Get a version of this encoder that encodes values as fast as possible
     * without making the encoded bytes sortable.
     */
    TopNEncoder toUnsortable();
}
