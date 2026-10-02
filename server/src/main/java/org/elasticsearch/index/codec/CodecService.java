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
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.threadpool.ThreadPool;

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

    private final Map<String, Codec> codecs;

    public static final String DEFAULT_CODEC = "default";
    public static final String LEGACY_DEFAULT_CODEC = "legacy_default"; // escape hatch
    public static final String BEST_COMPRESSION_CODEC = "best_compression";
    public static final String LEGACY_BEST_COMPRESSION_CODEC = "legacy_best_compression"; // escape hatch

    /** the raw unfiltered lucene default. useful for testing */
    public static final String LUCENE_DEFAULT_CODEC = "lucene_default";

    public CodecService(@Nullable MapperService mapperService, BigArrays bigArrays, @Nullable ThreadPool threadPool) {
        final var codecs = new HashMap<String, Codec>();

        var bestSpeedCodec = new PerFieldMapperCodec(
            Lucene104Codec.Mode.BEST_SPEED,
            ElasticsearchStoredFieldsFormat.Mode.LUCENE,
            ElasticsearchStoredFieldsFormat.Mode.LUCENE,
            mapperService,
            bigArrays,
            threadPool
        );
        codecs.put(DEFAULT_CODEC, bestSpeedCodec);
        // We can't remove this now
        codecs.put(LEGACY_DEFAULT_CODEC, bestSpeedCodec);

        // best_compression compresses stored fields with Zstd and leaves the rest of the Lucene codec on its default settings.
        // legacy_best_compression is what the name meant before Zstd: Lucene's own high compression stored fields.
        var bestCompressionCodec = new PerFieldMapperCodec(
            Lucene104Codec.Mode.BEST_SPEED,
            ElasticsearchStoredFieldsFormat.Mode.ZSTD_BEST_COMPRESSION,
            // Segments named Elasticsearch96 that record no mode were written with Lucene stored fields.
            ElasticsearchStoredFieldsFormat.Mode.LUCENE,
            mapperService,
            bigArrays,
            threadPool
        );
        codecs.put(BEST_COMPRESSION_CODEC, bestCompressionCodec);
        Codec legacyBestCompressionCodec = new PerFieldMapperCodec(
            Lucene104Codec.Mode.BEST_COMPRESSION,
            ElasticsearchStoredFieldsFormat.Mode.LUCENE,
            ElasticsearchStoredFieldsFormat.Mode.LUCENE,
            mapperService,
            bigArrays,
            threadPool
        );
        codecs.put(LEGACY_BEST_COMPRESSION_CODEC, legacyBestCompressionCodec);

        codecs.put(LUCENE_DEFAULT_CODEC, Codec.getDefault());
        for (String codec : Codec.availableCodecs()) {
            codecs.put(codec, Codec.forName(codec));
        }

        // A codec that does not share field infos gets a wrapper that does, under the codec's own name. Freshly written
        // segments are read back through the instance that wrote them, so this reaches those reads.
        this.codecs = codecs.entrySet()
            .stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    Map.Entry::getKey,
                    e -> e.getValue().fieldInfosFormat() instanceof ElasticsearchFieldInfosFormat
                        ? e.getValue()
                        : new SharedFieldInfosCodec(e.getValue())
                )
            );
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

    /** Adds field infos sharing to a codec that does not provide it, keeping that codec's name. */
    private static final class SharedFieldInfosCodec extends FilterCodec {

        private final FieldInfosFormat fieldInfosFormat;

        @SuppressWarnings("this-escape")
        SharedFieldInfosCodec(Codec delegate) {
            super(delegate.getName(), delegate);
            this.fieldInfosFormat = new ElasticsearchFieldInfosFormat(delegate.fieldInfosFormat());
        }

        @Override
        public FieldInfosFormat fieldInfosFormat() {
            return fieldInfosFormat;
        }
    }

}
