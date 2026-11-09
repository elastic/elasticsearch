/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;

/**
 * The codec of the stub segments that attach writes. Every doc values field that reaches it is bound to a Parquet column, so every
 * field goes to {@link ParquetDocValuesFormat}. The segment stores this codec name, and Lucene resolves it through SPI when the
 * segment is opened, which is why the read-only engine needs no hook in the codec that Elasticsearch uses to write new segments.
 */
public class ParquetAttachCodec extends FilterCodec {

    public static final String NAME = "ESParquetAttach";

    private final DocValuesFormat docValuesFormat = new PerFieldDocValuesFormat() {
        private final DocValuesFormat parquet = new ParquetDocValuesFormat();

        @Override
        public DocValuesFormat getDocValuesFormatForField(String field) {
            return parquet;
        }
    };

    public ParquetAttachCodec() {
        super(NAME, new Lucene104Codec());
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return docValuesFormat;
    }
}
