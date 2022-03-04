/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene91.Lucene91Codec;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MapperService;

import java.util.HashMap;
import java.util.Map;

/**
 * Since Lucene 4.0 low level index segments are read and written through a
 * codec layer that allows to use use-case specific file formats &amp;
 * data-structures per field. Elasticsearch exposes the full
 * {@link Codec} capabilities through this {@link CodecService}.
 */
public class CodecService {

    private final Map<String, Codec> codecs;

    public static final String DEFAULT_CODEC = "default";
    public static final String BEST_COMPRESSION_CODEC = "best_compression";
    /** the raw unfiltered lucene default. useful for testing */
    public static final String LUCENE_DEFAULT_CODEC = "lucene_default";

    public CodecService(@Nullable MapperService mapperService) {
        final var codecs = new HashMap<String, Codec>();
        if (mapperService == null) {
            codecs.put(DEFAULT_CODEC, new Lucene91Codec());
            codecs.put(BEST_COMPRESSION_CODEC, new Lucene91Codec(Lucene91Codec.Mode.BEST_COMPRESSION));
        } else {
            codecs.put(DEFAULT_CODEC, new PerFieldMapperCodec(Lucene91Codec.Mode.BEST_SPEED, mapperService));
            codecs.put(BEST_COMPRESSION_CODEC, new PerFieldMapperCodec(Lucene91Codec.Mode.BEST_COMPRESSION, mapperService));
        }
        codecs.put(LUCENE_DEFAULT_CODEC, Codec.getDefault());
        for (String codec : Codec.availableCodecs()) {
            codecs.put(codec, Codec.forName(codec));
        }
        this.codecs = Map.copyOf(codecs);
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
