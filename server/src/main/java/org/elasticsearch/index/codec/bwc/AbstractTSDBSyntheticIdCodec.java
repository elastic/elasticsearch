/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.bwc;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.StoredFieldsFormat;
import org.elasticsearch.index.codec.ElasticsearchFieldInfosFormat;
import org.elasticsearch.index.codec.perfield.XPerFieldDocValuesFormat;
import org.elasticsearch.index.codec.storedfields.TSDBStoredFieldsFormat;
import org.elasticsearch.index.codec.tsdb.ValidatingFieldInfosFormat;

/**
 * Abstract base class for ES codecs used with time-series ({@code TIME_SERIES}) indices
 * that employ synthetic document IDs for storage optimization.
 *
 * <p>This class configures the codec to use the following formats:
 * <ul>
 *   <li>Apply {@link TSDBStoredFieldsFormat} with bloom filter optimization for efficient ID lookups</li>
 * </ul>
 *
 * <p>
 *     Synthetic IDs in TSDB indices are generated from the document's dimensions and timestamp,
 *     replacing the standard {@code _id} field to reduce storage overhead.
 *
 * <p>
 *     Additionally, validates that all required fields are present and properly structured within the segment.
 * </p>
 *
 * @see TSDBStoredFieldsFormat
 */
abstract class AbstractTSDBSyntheticIdCodec extends FilterCodec {
    private final TSDBStoredFieldsFormat storedFieldsFormat;
    private final FieldInfosFormat fieldInfosFormat;
    private final DocValuesFormat docValuesFormat;

    AbstractTSDBSyntheticIdCodec(String name, Codec delegate, DocValuesFormatForField docValuesFormatForField) {
        super(name, delegate);
        // The delegate may already read synthetic ids; a second layer would keep one in place through merges.
        this.storedFieldsFormat = delegate.storedFieldsFormat() instanceof TSDBStoredFieldsFormat tsdbStoredFieldsFormat
            ? tsdbStoredFieldsFormat
            : new TSDBStoredFieldsFormat(delegate.storedFieldsFormat());
        // The delegate already shares field infos and validates synthetic ids; a second layer would only repeat both.
        this.fieldInfosFormat = delegate.fieldInfosFormat() instanceof ElasticsearchFieldInfosFormat elasticsearchFieldInfosFormat
            ? elasticsearchFieldInfosFormat
            : new ElasticsearchFieldInfosFormat(new ValidatingFieldInfosFormat(delegate.fieldInfosFormat(), true));
        this.docValuesFormat = new XPerFieldDocValuesFormat() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return docValuesFormatForField.get(field);
            }
        };
    }

    @Override
    public StoredFieldsFormat storedFieldsFormat() {
        return storedFieldsFormat;
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return docValuesFormat;
    }

    @Override
    public final FieldInfosFormat fieldInfosFormat() {
        return fieldInfosFormat;
    }

    @FunctionalInterface
    interface DocValuesFormatForField {
        DocValuesFormat get(String field);
    }
}
