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
import org.apache.lucene.codecs.lucene40.Lucene40Codec;
import org.elasticsearch.index.codec.postingsformat.PostingsFormatProvider;
import org.elasticsearch.index.mapper.MapperService;

/**
 * This one is the "default" codec we use.
 */
// LUCENE UPGRADE: make sure to move to a new codec depending on the lucene version
public class PerFieldMappingPostingFormatCodec extends Lucene40Codec {

    private final MapperService mapperService;
    private final PostingsFormat defaultPostingFormat;

    public PerFieldMappingPostingFormatCodec(MapperService mapperService, PostingsFormat defaultPostingFormat) {
        this.mapperService = mapperService;
        this.defaultPostingFormat = defaultPostingFormat;
    }

    @Override
    public PostingsFormat getPostingsFormatForField(String field) {
        PostingsFormatProvider postingsFormat = mapperService.indexName(field).mapper().postingFormatProvider();
        return postingsFormat != null ? postingsFormat.get() : defaultPostingFormat;
    }
}
