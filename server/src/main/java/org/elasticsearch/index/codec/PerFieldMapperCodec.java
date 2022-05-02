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
import org.apache.lucene.codecs.lucene91.Lucene91Codec;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.mapper.MapperService;

/**
 * {@link PerFieldMapperCodec This Lucene codec} provides the default
 * {@link PostingsFormat} and {@link KnnVectorsFormat} for Elasticsearch. It utilizes the
 * {@link MapperService} to lookup a {@link PostingsFormat} and {@link KnnVectorsFormat} per field. This
 * allows users to change the low level postings format and vectors format for individual fields
 * per index in real time via the mapping API. If no specific postings format or vector format is
 * configured for a specific field the default postings or vector format is used.
 */
public class PerFieldMapperCodec extends Lucene91Codec {
    private final MapperService mapperService;

    private final DocValuesFormat docValuesFormat = new Lucene90DocValuesFormat();

    static {
        assert Codec.forName(Lucene.LATEST_CODEC).getClass().isAssignableFrom(PerFieldMapperCodec.class)
            : "PerFieldMapperCodec must subclass the latest " + "lucene codec: " + Lucene.LATEST_CODEC;
    }

    public PerFieldMapperCodec(Mode compressionMode, MapperService mapperService) {
        super(compressionMode);
        this.mapperService = mapperService;
    }

    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
        PostingsFormat format = mapperService.mappingLookup().getPostingsFormat(field);
        if (format == null) {
            return super.getPostingsFormatForField(field);
        }
        return format;
    }

    @Override
    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
        KnnVectorsFormat format = mapperService.mappingLookup().getKnnVectorsFormatForField(field);
        if (format == null) {
            return super.getKnnVectorsFormatForField(field);
        }
        return format;
    }

    @Override
    public DocValuesFormat getDocValuesFormatForField(String field) {
        return docValuesFormat;
    }
}
