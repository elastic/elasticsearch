/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.Nullable;

import java.util.Map;

/**
 * Resource-free factory for one data format. The registry stores this object, never a
 * {@link FormatReader}. {@link #inspect} validates without I/O. {@link #create} returns a
 * distinct owned reader.
 */
public interface FormatReaderFactory {

    /**
     * Creates a distinct owned reader with default format options.
     */
    FormatReader create(Settings settings, BlockFactory blockFactory);

    /**
     * Creates a distinct owned reader with query configuration applied.
     * Per-unit schema, filter, and declarations come from {@code binding}.
     */
    default FormatReader create(
        Settings settings,
        BlockFactory blockFactory,
        @Nullable Map<String, Object> config,
        @Nullable FormatReadContext.Binding binding
    ) {
        return create(settings, blockFactory);
    }

    /**
     * Parses {@code config} and returns the format-owned keys consumed. Does not create a reader.
     */
    default Configured<Void> inspect(Map<String, Object> config) {
        return Configured.empty(null);
    }

    String formatName();

    default ErrorPolicy defaultErrorPolicy() {
        return ErrorPolicy.STRICT;
    }

    default AggregatePushdownSupport aggregatePushdownSupport() {
        return AggregatePushdownSupport.UNSUPPORTED;
    }

    default FilterPushdownSupport filterPushdownSupport() {
        return null;
    }

    default boolean dropsRowsUnderPushedFilter() {
        return false;
    }

    default boolean supportsNativeAsync() {
        return false;
    }

    default boolean supportsWholeFileCompression() {
        return true;
    }

    default boolean rangeAware() {
        return false;
    }

    default boolean supportsBatchRead() {
        return false;
    }

    default boolean segmentable() {
        return false;
    }

    default boolean columnExtractor() {
        return false;
    }

    default boolean acceptsDynamicThreshold() {
        return false;
    }

    /**
     * Whether this format's parsed options include a header row. Combined with declared
     * provenance by the framework, not here.
     */
    default boolean headerRow(@Nullable Map<String, Object> config) {
        return false;
    }

    @Nullable
    default RecordSplitter recordSplitter(@Nullable Map<String, Object> config, int maxRecordBytes) {
        return null;
    }

    default long minimumSegmentSize(@Nullable Map<String, Object> config) {
        return 0L;
    }
}
