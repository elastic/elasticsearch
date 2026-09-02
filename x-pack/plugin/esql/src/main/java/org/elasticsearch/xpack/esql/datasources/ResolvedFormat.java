/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.spi.DecompressionCodec;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;

import java.util.Map;

/**
 * Composed lookup result: the format factory plus an optional whole-file compression codec.
 * Dispatch uses {@link #codec} identity. Per-file compressed reads wrap at {@link #create};
 * stream-only workers that already see a {@link DecompressingStorageObject} call
 * {@link #createBare} so the reader is not wrapped again.
 */
public record ResolvedFormat(FormatReaderFactory factory, @Nullable DecompressionCodec codec) {

    public String formatName() {
        return factory.formatName();
    }

    /**
     * Creates a distinct reader, wrapping it for whole-file compression when a codec is present.
     */
    public FormatReader create(
        Settings settings,
        BlockFactory blockFactory,
        @Nullable Map<String, Object> config,
        @Nullable FormatReadContext.Binding binding
    ) {
        FormatReader reader = factory.create(settings, blockFactory, config, binding);
        return codec == null ? reader : new CompressionDelegatingFormatReader(reader, codec);
    }

    /**
     * Creates a distinct reader without wrapping for compression. Use when the storage object
     * already presents decompressed bytes.
     */
    public FormatReader createBare(
        Settings settings,
        BlockFactory blockFactory,
        @Nullable Map<String, Object> config,
        @Nullable FormatReadContext.Binding binding
    ) {
        return factory.create(settings, blockFactory, config, binding);
    }
}
