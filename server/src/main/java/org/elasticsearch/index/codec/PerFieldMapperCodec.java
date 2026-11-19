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
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * The codec Elasticsearch writes with. Postings, doc values and knn vectors are chosen per field by
 * {@link PerFieldFormatSupplier}; the stored fields implementation comes from the mode it is built with.
 *
 * <p>{@code index.codec=default} builds it with Lucene stored fields and {@code index.codec=best_compression} with Zstd.
 * {@code legacy_default} and {@code legacy_best_compression} select Lucene's own two compression levels.
 */
public final class PerFieldMapperCodec extends Elasticsearch96Codec {

    private final PerFieldFormatSupplier formatSupplier;

    public PerFieldMapperCodec(
        Lucene104Codec.Mode compressionMode,
        ElasticsearchStoredFieldsFormat.Mode storedFieldsMode,
        ElasticsearchStoredFieldsFormat.Mode legacyMode,
        MapperService mapperService,
        BigArrays bigArrays,
        ThreadPool threadPool
    ) {
        super(
            compressionMode,
            storedFieldsMode,
            legacyMode,
            mapperService != null && mapperService.getIndexSettings().useTimeSeriesSyntheticId()
        );
        this.formatSupplier = new PerFieldFormatSupplier(mapperService, bigArrays, threadPool);
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

}
