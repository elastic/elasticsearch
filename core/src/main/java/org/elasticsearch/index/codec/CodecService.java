/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat.Mode;
import org.apache.lucene.codecs.lucene54.Lucene54Codec;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.index.mapper.MapperService;

/**
 * Since Lucene 4.0 low level index segments are read and written through a
 * codec layer that allows to use use-case specific file formats &amp;
 * data-structures per field. Elasticsearch exposes the full
 * {@link Codec} capabilities through this {@link CodecService}.
 */
public class CodecService {
    public final static String DEFAULT_CODEC = "default";
    public final static String BEST_COMPRESSION_CODEC = "best_compression";
    /** the raw unfiltered lucene default. useful for testing */
    public final static String LUCENE_DEFAULT_CODEC = "lucene_default";

    private final MapperService mapperService;
    private final ESLogger logger;

    public CodecService(@Nullable MapperService mapperService, ESLogger logger) {
        this.mapperService = mapperService;
        this.logger = logger;
    }

    public Codec codec(String name) {
        switch (name) {
        case DEFAULT_CODEC:
            if (mapperService == null) {
                return new Lucene54Codec();
            }
            return new PerFieldMappingPostingFormatCodec(Mode.BEST_SPEED, mapperService, logger);
        case BEST_COMPRESSION_CODEC:
            if (mapperService == null) {
                return new Lucene54Codec(Mode.BEST_COMPRESSION);
            }
            return new PerFieldMappingPostingFormatCodec(Mode.BEST_COMPRESSION, mapperService, logger);
        case LUCENE_DEFAULT_CODEC:
            return Codec.getDefault();
        default:
            return Codec.forName(name);
        }
    }
}
