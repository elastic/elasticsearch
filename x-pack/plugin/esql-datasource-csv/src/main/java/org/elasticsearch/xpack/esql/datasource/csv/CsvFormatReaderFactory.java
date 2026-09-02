/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.TextAggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.cache.SchemaCacheKey;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;

import java.util.List;
import java.util.Map;

/**
 * Resource-free CSV/TSV factory. {@link #inspect} and {@link #create} share
 * {@link CsvFormatReader#parseOptionsFromConfig}.
 */
public final class CsvFormatReaderFactory implements FormatReaderFactory {

    private static final long MIN_SEGMENT_BYTES = 1024L * 1024L;

    private final String format;
    private final List<String> extensions;
    private final CsvFormatOptions defaults;
    private final boolean directBlockEnabled;

    public CsvFormatReaderFactory(String format, List<String> extensions, CsvFormatOptions defaults, boolean directBlockEnabled) {
        this.format = format;
        this.extensions = List.copyOf(extensions);
        this.defaults = defaults;
        this.directBlockEnabled = directBlockEnabled;
    }

    @Override
    public FormatReader create(Settings settings, BlockFactory blockFactory) {
        return create(settings, blockFactory, null, FormatReadContext.Binding.empty());
    }

    @Override
    public FormatReader create(
        Settings settings,
        BlockFactory blockFactory,
        @Nullable Map<String, Object> config,
        @Nullable FormatReadContext.Binding binding
    ) {
        Parsed parsed = parse(config, defaults);
        FormatReadContext.Binding bound = binding == null ? FormatReadContext.Binding.empty() : binding;
        return new CsvFormatReader(
            blockFactory,
            parsed.options,
            format,
            extensions,
            bound.boundSchema(),
            parsed.schemaSampleSize,
            parsed.effectivePolicy,
            parsed.canonicalConfig,
            bound.readConfig(),
            directBlockEnabled,
            bound.declaredDateFormats(),
            bound.declaredProvenanceBinding(),
            new CsvReaderCounters(format)
        );
    }

    @Override
    public Configured<Void> inspect(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return Configured.empty(null);
        }
        parse(config, defaults);
        return Configured.fromKnownSubset(null, config, CsvFormatReader.RECOGNIZED_KEYS);
    }

    @Override
    public String formatName() {
        return format;
    }

    @Override
    public AggregatePushdownSupport aggregatePushdownSupport() {
        return new TextAggregatePushdownSupport();
    }

    @Override
    public boolean segmentable() {
        return true;
    }

    @Override
    public boolean headerRow(@Nullable Map<String, Object> config) {
        return parse(config, defaults).options.headerRow();
    }

    @Override
    public RecordSplitter recordSplitter(@Nullable Map<String, Object> config, int maxRecordBytes) {
        return CsvFormatReader.recordSplitter(parse(config, defaults).options, maxRecordBytes);
    }

    @Override
    public long minimumSegmentSize(@Nullable Map<String, Object> config) {
        return MIN_SEGMENT_BYTES;
    }

    private static Parsed parse(@Nullable Map<String, Object> config, CsvFormatOptions defaults) {
        if (config == null || config.isEmpty()) {
            return new Parsed(defaults, CsvFormatReader.DEFAULT_SCHEMA_SAMPLE_SIZE, ErrorPolicy.STRICT, "");
        }
        CsvFormatOptions parsed = CsvFormatReader.parseOptionsFromConfig(config, defaults);
        int sampleSize = CsvFormatReader.parseInt(
            config.get(CsvFormatReader.CONFIG_SCHEMA_SAMPLE_SIZE),
            CsvFormatReader.DEFAULT_SCHEMA_SAMPLE_SIZE
        );
        Check.clientError(sampleSize > 0, CsvFormatReader.CONFIG_SCHEMA_SAMPLE_SIZE + " must be positive, got: {}", sampleSize);
        return new Parsed(
            parsed == null ? defaults : parsed,
            sampleSize,
            ErrorPolicy.fromConfig(config, ErrorPolicy.STRICT),
            SchemaCacheKey.buildFormatConfig(config)
        );
    }

    private record Parsed(CsvFormatOptions options, int schemaSampleSize, ErrorPolicy effectivePolicy, String canonicalConfig) {}
}
