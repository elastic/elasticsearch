/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.codec.bloomfilter.ES93BloomFilterStoredFieldsFormat;
import org.elasticsearch.index.codec.storedfields.TSDBStoredFieldsFormat;
import org.elasticsearch.index.codec.tsdb.TSDBSyntheticIdCodec;
import org.elasticsearch.index.mapper.IdFieldMapper;

/**
 * Abstract base class for ES codecs used with time-series ({@code TIME_SERIES}) indices
 * that employ synthetic document IDs for storage optimization.
 *
 * <p>This class configures the codec to use the following formats:
 * <ul>
 *   <li>
 *       Use {@link TSDBSyntheticIdCodec} as the underlying codec for synthesizing the `_id` field from
 *       the values of other fields of the document (ex: _tsid, @timestamp, etc.) so that no inverted index
 *       or stored field are required for the `_id`. As such, looking up documents by `_id` might be very
 *       slow and that's why it is used along with a Bloom filter.
 *   </li>
 *   <li>
 *       Apply {@link TSDBStoredFieldsFormat} with bloom filter optimization for efficient ID lookups
 *   </li>
 * </ul>
 *
 * <p>Synthetic IDs in TSDB indices are generated from the document's dimensions and timestamp,
 * replacing the standard {@code _id} field to reduce storage overhead.
 *
 * @see TSDBSyntheticIdCodec
 * @see TSDBStoredFieldsFormat
 */
abstract class TSDBCodecWithSyntheticId extends FilterCodec {
    private final TSDBStoredFieldsFormat storedFieldsFormat;

    TSDBCodecWithSyntheticId(String name, Codec delegate, BigArrays bigArrays) {
        super(name, new TSDBSyntheticIdCodec(delegate));
        this.storedFieldsFormat = new TSDBStoredFieldsFormat(
            delegate.storedFieldsFormat(),
            new ES93BloomFilterStoredFieldsFormat(
                bigArrays,
                ES93BloomFilterStoredFieldsFormat.DEFAULT_BLOOM_FILTER_SIZE,
                IdFieldMapper.NAME
            )
        );
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }
}
