/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.codec.perfield.XPerFieldDocValuesFormat.MergeCallback;
import org.elasticsearch.index.codec.startree.StarTreeMergeBuilder;
import org.elasticsearch.index.codec.zstd.Zstd814StoredFieldsFormat;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.startree.StarTreeConfig;
import org.elasticsearch.threadpool.ThreadPool;

/**
 * {@link PerFieldMapperCodec This Lucene codec} provides the default
 * {@link PostingsFormat} and {@link KnnVectorsFormat} for Elasticsearch. It utilizes the
 * {@link MapperService} to lookup a {@link PostingsFormat} and {@link KnnVectorsFormat} per field. This
 * allows users to change the low level postings format and vectors format for individual fields
 * per index in real time via the mapping API. If no specific postings format or vector format is
 * configured for a specific field the default postings or vector format is used.
 */
public final class PerFieldMapperCodec extends Elasticsearch92Lucene103Codec {

    private final PerFieldFormatSupplier formatSupplier;

    public PerFieldMapperCodec(
        Zstd814StoredFieldsFormat.Mode compressionMode,
        MapperService mapperService,
        BigArrays bigArrays,
        ThreadPool threadPool
    ) {
        super(compressionMode, createMergeCallback(mapperService));
        this.formatSupplier = new PerFieldFormatSupplier(mapperService, bigArrays, threadPool);
        // If the below assertion fails, it is a sign that Lucene released a new codec. You must create a copy of the current Elasticsearch
        // codec that delegates to this new Lucene codec, and make PerFieldMapperCodec extend this new Elasticsearch codec.
        assert Codec.forName(Lucene.LATEST_CODEC).getClass() == delegate.getClass()
            : "PerFieldMapperCodec must be on the latest lucene codec: " + Lucene.LATEST_CODEC;
    }

    private static final Logger logger = LogManager.getLogger(PerFieldMapperCodec.class);

    private static MergeCallback createMergeCallback(MapperService mapperService) {
        // Get star-tree config at merge time, not codec creation time
        // This ensures we get the latest config even if mapping was updated after codec creation
        return (state, mergeState) -> {
            StarTreeConfig starTreeConfig = mapperService.mappingLookup().getStarTreeConfig();
            logger.info("Merge callback: starTreeConfig={}", starTreeConfig);
            if (starTreeConfig != null && starTreeConfig.getStarTrees().isEmpty() == false) {
                // Create a field type lookup that checks if a field is a double/float type
                java.util.function.Function<String, Boolean> isDoubleFieldLookup = fieldName -> {
                    var fieldType = mapperService.mappingLookup().getFieldType(fieldName);
                    if (fieldType != null) {
                        String typeName = fieldType.typeName();
                        return "double".equals(typeName) || "float".equals(typeName) || "half_float".equals(typeName)
                            || "scaled_float".equals(typeName);
                    }
                    return false;
                };
                StarTreeMergeBuilder.buildStarTrees(state, starTreeConfig, mergeState, isDoubleFieldLookup);
            }
        };
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
