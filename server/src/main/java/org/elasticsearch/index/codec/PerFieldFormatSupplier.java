/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.codecs.lucene99.Lucene99HnswVectorsFormat;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.codec.bloomfilter.ES87BloomFilterPostingsFormat;
import org.elasticsearch.index.codec.postings.ES812PostingsFormat;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.CompletionFieldMapper;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.SeqNoFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesIdFieldMapper;
import org.elasticsearch.index.mapper.TimeSeriesRoutingHashFieldMapper;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Class that encapsulates the logic of figuring out the most appropriate file format for a given field, across postings, doc values and
 * vectors.
 */
public class PerFieldFormatSupplier {

    private static final Set<String> INCLUDE_META_FIELDS;
    private static final Set<String> EXCLUDE_MAPPER_TYPES;

    static {
        // TODO: should we just allow all fields to use tsdb doc values codec?
        // Avoid using tsdb codec for fields like _seq_no, _primary_term.
        // But _tsid and _ts_routing_hash should always use the tsdb codec.
        Set<String> includeMetaField = new HashSet<>(3);
        includeMetaField.add(TimeSeriesIdFieldMapper.NAME);
        includeMetaField.add(TimeSeriesRoutingHashFieldMapper.NAME);
        includeMetaField.add(SeqNoFieldMapper.NAME);
        // Don't the include _recovery_source_size and _recovery_source fields, since their values can be trimmed away in
        // RecoverySourcePruneMergePolicy, which leads to inconsistencies between merge stats and actual values.
        INCLUDE_META_FIELDS = Collections.unmodifiableSet(includeMetaField);
        EXCLUDE_MAPPER_TYPES = Set.of("geo_shape");
    }

    private static final DocValuesFormat docValuesFormat = new Lucene90DocValuesFormat();
    private static final KnnVectorsFormat knnVectorsFormat = new Lucene99HnswVectorsFormat();
    private static final ES819TSDBDocValuesFormat tsdbDocValuesFormat = new ES819TSDBDocValuesFormat();
    private static final ES812PostingsFormat es812PostingsFormat = new ES812PostingsFormat();
    private static final PostingsFormat completionPostingsFormat = PostingsFormat.forName("Completion101");

    private final ES87BloomFilterPostingsFormat bloomFilterPostingsFormat;
    private final MapperService mapperService;

    private final PostingsFormat defaultPostingsFormat;

    public PerFieldFormatSupplier(MapperService mapperService, BigArrays bigArrays) {
        this.mapperService = mapperService;
        this.bloomFilterPostingsFormat = new ES87BloomFilterPostingsFormat(bigArrays, this::internalGetPostingsFormatForField);
        this.defaultPostingsFormat = getDefaultPostingsFormat(mapperService);
    }

    private static PostingsFormat getDefaultPostingsFormat(final MapperService mapperService) {
        // we migrated to using a new postings format for the standard indices with Lucene 10.3
        if (mapperService != null
            && mapperService.getIndexSettings().getIndexVersionCreated().onOrAfter(IndexVersions.UPGRADE_TO_LUCENE_10_3_0)) {
            if (IndexSettings.USE_ES_812_POSTINGS_FORMAT.get(mapperService.getIndexSettings().getSettings())) {
                return es812PostingsFormat;
            } else {
                return Elasticsearch92Lucene103Codec.DEFAULT_POSTINGS_FORMAT;
            }
        } else {
            // our own posting format using PFOR, used for logsdb and tsdb indices by default
            return es812PostingsFormat;
        }
    }

    public PostingsFormat getPostingsFormatForField(String field) {
        if (useBloomFilter(field)) {
            return bloomFilterPostingsFormat;
        }
        return internalGetPostingsFormatForField(field);
    }

    private PostingsFormat internalGetPostingsFormatForField(String field) {
        if (mapperService != null) {
            Mapper mapper = mapperService.mappingLookup().getMapper(field);
            if (mapper instanceof CompletionFieldMapper) {
                return completionPostingsFormat;
            }
        }

        return defaultPostingsFormat;
    }

    boolean useBloomFilter(String field) {
        if (mapperService == null) {
            return false;
        }
        IndexSettings indexSettings = mapperService.getIndexSettings();
        if (mapperService.mappingLookup().isDataStreamTimestampFieldEnabled()) {
            // In case for time series indices, the _id isn't randomly generated,
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

    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        if (mapperService != null) {
            Mapper mapper = mapperService.mappingLookup().getMapper(field);
            if (mapper instanceof DenseVectorFieldMapper vectorMapper) {
                return vectorMapper.getKnnVectorsFormatForField(knnVectorsFormat, mapperService.getIndexSettings());
            }
        }
        return knnVectorsFormat;
    }

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

        if (excludeMapperTypes(field)) {
            return false;
        }

        return mapperService != null
            && mapperService.getIndexSettings().useTimeSeriesDocValuesFormat()
            && mapperService.getIndexSettings().isES87TSDBCodecEnabled();
    }

    private boolean excludeFields(String fieldName) {
        return fieldName.startsWith("_") && INCLUDE_META_FIELDS.contains(fieldName) == false;
    }

    private boolean excludeMapperTypes(String fieldName) {
        var typeName = getMapperType(fieldName);
        if (typeName == null) {
            return false;
        }
        return EXCLUDE_MAPPER_TYPES.contains(getMapperType(fieldName));
    }

    private boolean isTimeSeriesModeIndex() {
        return mapperService != null && IndexMode.TIME_SERIES == mapperService.getIndexSettings().getMode();
    }

    private boolean isLogsModeIndex() {
        return mapperService != null && IndexMode.LOGSDB == mapperService.getIndexSettings().getMode();
    }

    String getMapperType(final String field) {
        if (mapperService != null) {
            Mapper mapper = mapperService.mappingLookup().getMapper(field);
            if (mapper != null) {
                return mapper.typeName();
            }
        }
        return null;
    }
}
