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
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.bloomfilter.ES87BloomFilterPostingsFormat;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.MappingLookup;
import org.elasticsearch.index.mapper.NumberFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesParams;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

/**
 * {@link PerFieldMapperCodec This Lucene codec} provides the default
 * {@link PostingsFormat} and {@link KnnVectorsFormat} for Elasticsearch. It utilizes the
 * {@link MapperService} to lookup a {@link PostingsFormat} and {@link KnnVectorsFormat} per field. This
 * allows users to change the low level postings format and vectors format for individual fields
 * per index in real time via the mapping API. If no specific postings format or vector format is
 * configured for a specific field the default postings or vector format is used.
 */
public class PerFieldMapperCodec extends Lucene95Codec {

    private final MapperService mapperService;
    private final DocValuesFormat docValuesFormat = new Lucene90DocValuesFormat();
    private final ES87BloomFilterPostingsFormat bloomFilterPostingsFormat;
    private final ES87TSDBDocValuesFormat tsdbDocValuesFormat;

    static {
        assert Codec.forName(Lucene.LATEST_CODEC).getClass().isAssignableFrom(PerFieldMapperCodec.class)
            : "PerFieldMapperCodec must subclass the latest lucene codec: " + Lucene.LATEST_CODEC;
    }

    public PerFieldMapperCodec(Mode compressionMode, MapperService mapperService, BigArrays bigArrays) {
        super(compressionMode);
        this.mapperService = mapperService;
        this.bloomFilterPostingsFormat = new ES87BloomFilterPostingsFormat(bigArrays, this::internalGetPostingsFormatForField);
        this.tsdbDocValuesFormat = new ES87TSDBDocValuesFormat();
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
            KnnVectorsFormat format = vectorMapper.getKnnVectorsFormatForField();
            if (format != null) {
                return format;
            }
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
        return mapperService.getIndexSettings().isES87TSDBCodecEnabled()
            && isTimeSeriesModeIndex()
            && isNotSpecialField(field)
            && (isCounterOrGaugeMetricType(field) || isTimestampField(field));
    }

    private boolean isTimeSeriesModeIndex() {
        return IndexMode.TIME_SERIES.equals(mapperService.getIndexSettings().getMode());
    }

    private boolean isCounterOrGaugeMetricType(String field) {
        if (mapperService != null) {
            final MappingLookup mappingLookup = mapperService.mappingLookup();
            if (mappingLookup.getMapper(field) instanceof NumberFieldMapper) {
                final MappedFieldType fieldType = mappingLookup.getFieldType(field);
                return TimeSeriesParams.MetricType.COUNTER.equals(fieldType.getMetricType())
                    || TimeSeriesParams.MetricType.GAUGE.equals(fieldType.getMetricType());
            }
        }
        return false;
    }

    private boolean isTimestampField(String field) {
        return "@timestamp".equals(field);
    }

    private boolean isNotSpecialField(String field) {
        return field.startsWith("_") == false;
    }
}
