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
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene90.Lucene90Codec;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.mapper.MapperService;

/**
 * {@link PerFieldMappingPostingFormatCodec This postings format} is the default
 * {@link PostingsFormat} for Elasticsearch. It utilizes the
 * {@link MapperService} to lookup a {@link PostingsFormat} per field. This
 * allows users to change the low level postings format for individual fields
 * per index in real time via the mapping API. If no specific postings format is
 * configured for a specific field the default postings format is used.
 */
public class PerFieldMappingPostingFormatCodec extends Lucene90Codec {
    private final MapperService mapperService;

    private final DocValuesFormat docValuesFormat = new Lucene90DocValuesFormat();

    static {
        assert Codec.forName(Lucene.LATEST_CODEC).getClass().isAssignableFrom(PerFieldMappingPostingFormatCodec.class) :
            "PerFieldMappingPostingFormatCodec must subclass the latest " + "lucene codec: " + Lucene.LATEST_CODEC;
    }

    public PerFieldMappingPostingFormatCodec(Mode compressionMode, MapperService mapperService) {
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
    public DocValuesFormat getDocValuesFormatForField(String field) {
        return docValuesFormat;
    }
}
