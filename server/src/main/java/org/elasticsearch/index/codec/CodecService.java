/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.FieldInfosFormat;
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.lucene100.Lucene100Codec;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.FeatureFlag;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.zstd.Zstd814StoredFieldsFormat;
import org.elasticsearch.index.mapper.MapperService;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Since Lucene 4.0 low level index segments are read and written through a
 * codec layer that allows to use use-case specific file formats &amp;
 * data-structures per field. Elasticsearch exposes the full
 * {@link Codec} capabilities through this {@link CodecService}.
 */
public class CodecService implements CodecProvider {

    public static final FeatureFlag ZSTD_STORED_FIELDS_FEATURE_FLAG = new FeatureFlag("zstd_stored_fields");

    private final Map<String, Codec> codecs;

    public static final String DEFAULT_CODEC = "default";
    public static final String LEGACY_DEFAULT_CODEC = "legacy_default"; // escape hatch
    public static final String BEST_COMPRESSION_CODEC = "best_compression";
    public static final String LEGACY_BEST_COMPRESSION_CODEC = "legacy_best_compression"; // escape hatch

    /** the raw unfiltered lucene default. useful for testing */
    public static final String LUCENE_DEFAULT_CODEC = "lucene_default";

    public CodecService(@Nullable MapperService mapperService, BigArrays bigArrays) {
        final var codecs = new HashMap<String, Codec>();

        Codec legacyBestSpeedCodec = new LegacyPerFieldMapperCodec(Lucene100Codec.Mode.BEST_SPEED, mapperService, bigArrays);
        if (ZSTD_STORED_FIELDS_FEATURE_FLAG.isEnabled()) {
            codecs.put(DEFAULT_CODEC, new PerFieldMapperCodec(Zstd814StoredFieldsFormat.Mode.BEST_SPEED, mapperService, bigArrays));
        } else {
            codecs.put(DEFAULT_CODEC, legacyBestSpeedCodec);
        }
        codecs.put(LEGACY_DEFAULT_CODEC, legacyBestSpeedCodec);

        codecs.put(
            BEST_COMPRESSION_CODEC,
            new PerFieldMapperCodec(Zstd814StoredFieldsFormat.Mode.BEST_COMPRESSION, mapperService, bigArrays)
        );
        Codec legacyBestCompressionCodec = new LegacyPerFieldMapperCodec(Lucene100Codec.Mode.BEST_COMPRESSION, mapperService, bigArrays);
        codecs.put(LEGACY_BEST_COMPRESSION_CODEC, legacyBestCompressionCodec);

        codecs.put(LUCENE_DEFAULT_CODEC, Codec.getDefault());
        for (String codec : Codec.availableCodecs()) {
            codecs.put(codec, Codec.forName(codec));
        }
        this.codecs = codecs.entrySet().stream().collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> {
            var codec = e.getValue();
            if (codec instanceof DeduplicateFieldInfosCodec) {
                return codec;
            }
            return new DeduplicateFieldInfosCodec(codec.getName(), codec);
        }));
    }

    public Codec codec(String name) {
        Codec codec = codecs.get(name);
        if (codec == null) {
            throw new IllegalArgumentException("failed to find codec [" + name + "]");
        }
        return codec;
    }

    /**
     * Returns all registered available codec names.
     */
    @Override
    public String[] availableCodecs() {
        return codecs.keySet().toArray(new String[0]);
    }

    public static class DeduplicateFieldInfosCodec extends FilterCodec {

        private final DeduplicatingFieldInfosFormat deduplicatingFieldInfosFormat;

        @SuppressWarnings("this-escape")
        protected DeduplicateFieldInfosCodec(String name, Codec delegate) {
            super(name, delegate);
            this.deduplicatingFieldInfosFormat = new DeduplicatingFieldInfosFormat(super.fieldInfosFormat());
        }

        @Override
        public final FieldInfosFormat fieldInfosFormat() {
            return deduplicatingFieldInfosFormat;
        }

        public final Codec delegate() {
            return delegate;
        }

    }
}
