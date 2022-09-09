/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene94.Lucene94Codec;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.bloomfilter.ES85BloomFilterPostingsFormat;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

/**
 * {@link PerFieldMapperCodec This Lucene codec} provides the default
 * {@link PostingsFormat} and {@link KnnVectorsFormat} for Elasticsearch. It utilizes the
 * {@link MapperService} to lookup a {@link PostingsFormat} and {@link KnnVectorsFormat} per field. This
 * allows users to change the low level postings format and vectors format for individual fields
 * per index in real time via the mapping API. If no specific postings format or vector format is
 * configured for a specific field the default postings or vector format is used.
 */
public class PerFieldMapperCodec extends Lucene94Codec {
    private final MapperService mapperService;

    private final DocValuesFormat docValuesFormat = new Lucene90DocValuesFormat();
    private final ES85BloomFilterPostingsFormat bloomFilterPostingsFormat;

    static {
        assert Codec.forName(Lucene.LATEST_CODEC).getClass().isAssignableFrom(PerFieldMapperCodec.class)
            : "PerFieldMapperCodec must subclass the latest " + "lucene codec: " + Lucene.LATEST_CODEC;
    }

    public PerFieldMapperCodec(Mode compressionMode, MapperService mapperService, BigArrays bigArrays) {
        super(compressionMode);
        this.mapperService = mapperService;
        this.bloomFilterPostingsFormat = new ES85BloomFilterPostingsFormat(bigArrays, this::internalGetPostingsFormatForField);
    }

    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
        if (useBloomFilter(field)) {
            return bloomFilterPostingsFormat;
        }
        return internalGetPostingsFormatForField(field);
    }

    private PostingsFormat internalGetPostingsFormatForField(String field) {
        final PostingsFormat format = mapperService.mappingLookup().getPostingsFormat(field);
        if (format != null) {
            return format;
        }
        return super.getPostingsFormatForField(field);
    }

    private boolean useBloomFilter(String field) {
        return IdFieldMapper.NAME.equals(field)
            && mapperService.mappingLookup().isDataStreamTimestampFieldEnabled() == false
            && IndexSettings.BLOOM_FILTER_ID_FIELD_ENABLED_SETTING.get(mapperService.getIndexSettings().getSettings());
    }

    @Override
    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        Mapper mapper = mapperService.mappingLookup().getMapper(field);
        if (mapper instanceof DenseVectorFieldMapper vectorMapper) {
            KnnVectorsFormat format = vectorMapper.getKnnVectorsFormatForField();
            if (format != null) {
                return format;
            }
        }
        return super.getKnnVectorsFormatForField(field);
    }

    @Override
    public DocValuesFormat getDocValuesFormatForField(String field) {
        return docValuesFormat;
    }
}
