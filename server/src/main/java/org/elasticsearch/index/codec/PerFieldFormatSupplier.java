/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
import org.elasticsearch.index.codec.bloomfilter.ES87BloomFilterPostingsFormat;
import org.elasticsearch.index.codec.postings.ES812PostingsFormat;
import org.elasticsearch.index.codec.tsdb.ES87TSDBDocValuesFormat;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;

/**
 * Class that encapsulates the logic of figuring out the most appropriate file format for a given field, across postings, doc values and
 * vectors.
 */
public class PerFieldFormatSupplier {

    private final MapperService mapperService;
    private final DocValuesFormat docValuesFormat = new Lucene90DocValuesFormat();
    private final KnnVectorsFormat knnVectorsFormat = new Lucene99HnswVectorsFormat();
    private final ES87BloomFilterPostingsFormat bloomFilterPostingsFormat;
    private final ES87TSDBDocValuesFormat tsdbDocValuesFormat;

    private final ES812PostingsFormat es812PostingsFormat;

    public PerFieldFormatSupplier(MapperService mapperService, BigArrays bigArrays) {
        this.mapperService = mapperService;
        this.bloomFilterPostingsFormat = new ES87BloomFilterPostingsFormat(bigArrays, this::internalGetPostingsFormatForField);
        this.tsdbDocValuesFormat = new ES87TSDBDocValuesFormat();
        this.es812PostingsFormat = new ES812PostingsFormat();
    }

    public PostingsFormat getPostingsFormatForField(String field) {
        if (useBloomFilter(field)) {
            return bloomFilterPostingsFormat;
        }
        return internalGetPostingsFormatForField(field);
    }

    private PostingsFormat internalGetPostingsFormatForField(String field) {
        if (mapperService != null) {
            final PostingsFormat format = mapperService.mappingLookup().getPostingsFormat(field);
            if (format != null) {
                return format;
            }
        }
        // return our own posting format using PFOR
        return es812PostingsFormat;
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
                return vectorMapper.getKnnVectorsFormatForField(knnVectorsFormat);
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

        return mapperService != null
            && (isTimeSeriesModeIndex() || isLogsModeIndex())
            && mapperService.getIndexSettings().isES87TSDBCodecEnabled();
    }

    private boolean excludeFields(String fieldName) {
        // Avoid using tsdb codec for fields like _seq_no, _primary_term.
        // But _tsid and _ts_routing_hash should always use the tsdb codec.
        return fieldName.startsWith("_") && fieldName.equals("_tsid") == false && fieldName.equals("_ts_routing_hash") == false;
    }

    private boolean isTimeSeriesModeIndex() {
        return mapperService != null && IndexMode.TIME_SERIES == mapperService.getIndexSettings().getMode();
    }

    private boolean isLogsModeIndex() {
        return mapperService != null && IndexMode.LOGSDB == mapperService.getIndexSettings().getMode();
    }

}
