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

import com.google.common.collect.ImmutableMap;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene50.Lucene50StoredFieldsFormat.Mode;
import org.apache.lucene.codecs.lucene53.Lucene53Codec;
import org.elasticsearch.common.collect.MapBuilder;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.AbstractIndexComponent;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.index.settings.IndexSettings;

/**
 * Since Lucene 4.0 low level index segments are read and written through a
 * codec layer that allows to use use-case specific file formats &
 * data-structures per field. Elasticsearch exposes the full
 * {@link Codec} capabilities through this {@link CodecService}.
 *
 * @see PostingsFormatService
 * @see DocValuesFormatService
 */
public class CodecService extends AbstractIndexComponent {

    private final MapperService mapperService;
    private final ImmutableMap<String, Codec> codecs;

    public final static String DEFAULT_CODEC = "default";
    public final static String BEST_COMPRESSION_CODEC = "best_compression";
    /** the raw unfiltered lucene default. useful for testing */
    public final static String LUCENE_DEFAULT_CODEC = "lucene_default";

    public CodecService(Index index) {
        this(index, Settings.Builder.EMPTY_SETTINGS);
    }

    public CodecService(Index index, @IndexSettings Settings indexSettings) {
        this(index, indexSettings, null);
    }

    @Inject
    public CodecService(Index index, @IndexSettings Settings indexSettings, MapperService mapperService) {
        super(index, indexSettings);
        this.mapperService = mapperService;
        MapBuilder<String, Codec> codecs = MapBuilder.<String, Codec>newMapBuilder();
        if (mapperService == null) {
            codecs.put(DEFAULT_CODEC, new Lucene53Codec());
            codecs.put(BEST_COMPRESSION_CODEC, new Lucene53Codec(Mode.BEST_COMPRESSION));
        } else {
            codecs.put(DEFAULT_CODEC, 
                    new PerFieldMappingPostingFormatCodec(Mode.BEST_SPEED, mapperService, logger));
            codecs.put(BEST_COMPRESSION_CODEC, 
                    new PerFieldMappingPostingFormatCodec(Mode.BEST_COMPRESSION, mapperService, logger));
        }
        codecs.put(LUCENE_DEFAULT_CODEC, Codec.getDefault());
        for (String codec : Codec.availableCodecs()) {
            codecs.put(codec, Codec.forName(codec));
        }
        this.codecs = codecs.immutableMap();
    }

    public MapperService mapperService() {
        return mapperService;
    }

    public Codec codec(String name) {
        Codec codec = codecs.get(name);
        if (codec == null) {
            throw new IllegalArgumentException("failed to find codec [" + name + "]");
        }
        return codec;
    }

    /**
     * Returns all registered available codec names
     */
    public String[] availableCodecs() {
        return codecs.keySet().toArray(new String[0]);
    }
}
