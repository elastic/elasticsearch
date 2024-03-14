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
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.bloomfilter.ES87BloomFilterPostingsFormat;
import org.elasticsearch.index.codec.postings.ES812PostingsFormat;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormat;
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
public final class PerFieldMapperCodec extends Lucene99Codec {

    private final MapperService mapperService;
    private final DocValuesFormat docValuesFormat = new Lucene90DocValuesFormat();
    private final ES87BloomFilterPostingsFormat bloomFilterPostingsFormat;
    private final ES87TSDBDocValuesFormat tsdbDocValuesFormat;

    private final ES812PostingsFormat es812PostingsFormat;

    static {
        assert Codec.forName(Lucene.LATEST_CODEC).getClass().isAssignableFrom(PerFieldMapperCodec.class)
            : "PerFieldMapperCodec must subclass the latest lucene codec: " + Lucene.LATEST_CODEC;
    }

    public PerFieldMapperCodec(Mode compressionMode, MapperService mapperService, BigArrays bigArrays) {
        super(compressionMode);
        this.mapperService = mapperService;
        this.bloomFilterPostingsFormat = new ES87BloomFilterPostingsFormat(bigArrays, this::internalGetPostingsFormatForField);
        this.tsdbDocValuesFormat = new ES87TSDBDocValuesFormat();
        this.es812PostingsFormat = new ES812PostingsFormat();
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
        // return our own posting format using PFOR
        return es812PostingsFormat;
    }

    boolean useBloomFilter(String field) {
        IndexSettings indexSettings = mapperService.getIndexSettings();
        if (mapperService.mappingLookup().isDataStreamTimestampFieldEnabled()) {
            // In case for time series indices, they _id isn't randomly generated,
            // but based on dimension fields and timestamp field, so during indexing
            // version/seq_no/term needs to be looked up and having a bloom filter
            // can speed this up significantly.
            return indexSettings.getMode() == IndexMode.TIME_SERIES
                && IdFieldMapper.NAME.equals(field)
                && IndexSettings.BLOOM_FILTER_ID_FIELD_ENABLED_SETTING.get(indexSettings.getSettings());
        } else {
            return IdFieldMapper.NAME.equals(field) && IndexSettings.BLOOM_FILTER_ID_FIELD_ENABLED_SETTING.get(indexSettings.getSettings());
        }
    }

    @Override
    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        Mapper mapper = mapperService.mappingLookup().getMapper(field);
        if (mapper instanceof DenseVectorFieldMapper vectorMapper) {
            return vectorMapper.getKnnVectorsFormatForField(super.getKnnVectorsFormatForField(field));
        }
        return super.getKnnVectorsFormatForField(field);
    }

    @Override
    public DocValuesFormat getDocValuesFormatForField(String field) {
        if (useTSDBDocValuesFormat(field)) {
            return tsdbDocValuesFormat;
        }
        return docValuesFormat;
    }

    boolean useTSDBDocValuesFormat(final String field) {
        if (excludeFields(field)) {
            return false;
        }

        return mapperService != null && isTimeSeriesModeIndex() && mapperService.getIndexSettings().isES87TSDBCodecEnabled();
    }

    private boolean excludeFields(String fieldName) {
        // Avoid using tsdb codec for fields like _seq_no, _primary_term.
        // But _tsid and _ts_routing_hash should always use the tsdb codec.
        return fieldName.startsWith("_") && fieldName.equals("_tsid") == false && fieldName.equals("_ts_routing_hash") == false;
    }

    private boolean isTimeSeriesModeIndex() {
        return IndexMode.TIME_SERIES == mapperService.getIndexSettings().getMode();
    }

}
