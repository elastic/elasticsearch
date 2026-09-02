/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.tsdb;

import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.zstd.Zstd814StoredFieldsFormat;

public class ES94TSDBBestCompressionLucene104Codec extends AbstractTSDBSyntheticIdCodec {
    public static final String NAME = "ES94TSDBBestCompressionLucene104Codec";

    /** Public no-arg constructor, needed for SPI loading at read-time. */
    public ES94TSDBBestCompressionLucene104Codec() {
        this(new Elasticsearch93Lucene104Codec(Zstd814StoredFieldsFormat.Mode.BEST_COMPRESSION));
    }

    public ES94TSDBBestCompressionLucene104Codec(Elasticsearch93Lucene104Codec delegate) {
        super(NAME, delegate, delegate::getDocValuesFormatForField);
    }
}
