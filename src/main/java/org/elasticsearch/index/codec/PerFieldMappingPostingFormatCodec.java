/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene46.Lucene46Codec;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.mapper.FieldMappers;
import org.elasticsearch.index.mapper.MapperService;

/**
 * {@link PerFieldMappingPostingFormatCodec This postings format} is the default
 * {@link PostingsFormat} for Elasticsearch. It utilizes the
 * {@link MapperService} to lookup a {@link PostingsFormat} per field. This
 * allows users to change the low level postings format for individual fields
 * per index in real time via the mapping API. If no specific postings format is
 * configured for a specific field the default postings format is used.
 */
// LUCENE UPGRADE: make sure to move to a new codec depending on the lucene version
public class PerFieldMappingPostingFormatCodec extends Lucene46Codec {
    private final ESLogger logger;
    private final MapperService mapperService;
    private final PostingsFormat defaultPostingFormat;

    public PerFieldMappingPostingFormatCodec(MapperService mapperService, PostingsFormat defaultPostingFormat, ESLogger logger) {
        this.mapperService = mapperService;
        this.logger = logger;
        this.defaultPostingFormat = defaultPostingFormat;
    }

    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
        final FieldMappers indexName = mapperService.indexName(field);
        if (indexName == null) {
            logger.warn("no index mapper found for field: [{}] returning default postings format", field);
            return defaultPostingFormat;
        }
        PostingsFormatProvider postingsFormat = indexName.mapper().postingsFormatProvider();
        return postingsFormat != null ? postingsFormat.get() : defaultPostingFormat;
    }
}
