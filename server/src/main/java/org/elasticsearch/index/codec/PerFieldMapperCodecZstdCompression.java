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
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.codec.storedfields.ESStoredFieldsFormat;
import org.elasticsearch.index.codec.zstd.Zstd814StoredFieldsFormat;
import org.elasticsearch.index.mapper.MapperService;

public class PerFieldMapperCodecZstdCompression extends Elasticsearch93ZstdCompressionLucene103Codec {

    private final PerFieldFormatSupplier formatSupplier;

    public PerFieldMapperCodecZstdCompression(
        Zstd814StoredFieldsFormat.Mode compressionMode,
        MapperService mapperService,
        BigArrays bigArrays
    ) {
        super(compressionMode);
        this.formatSupplier = new PerFieldFormatSupplier(mapperService, bigArrays, defaultStoredFieldsFormat);
        // If the below assertion fails, it is a sign that Lucene released a new codec. You must create a copy of the current Elasticsearch
        // codec that delegates to this new Lucene codec, and make PerFieldMapperCodec extend this new Elasticsearch codec.
        assert Codec.forName(Lucene.LATEST_CODEC).getClass() == delegate.getClass()
            : "PerFieldMapperCodec must be on the latest lucene codec: " + Lucene.LATEST_CODEC;
    }

    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
        return formatSupplier.getPostingsFormatForField(field);
    }

    @Override
    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        return formatSupplier.getKnnVectorsFormatForField(field);
    }

    @Override
    public DocValuesFormat getDocValuesFormatForField(String field) {
        return formatSupplier.getDocValuesFormatForField(field);
    }

    @Override
    public ESStoredFieldsFormat getStoredFieldsFormatForField(String field) {
        return formatSupplier.getStoredFieldsFormatForField(field);
    }
}
