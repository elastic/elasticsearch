/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.TextAggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.cache.SchemaCacheKey;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.Configured;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;
import org.elasticsearch.xpack.esql.datasources.spi.RecordSplitter;

import java.util.Map;
import java.util.Set;

/**
 * Resource-free NDJSON factory. {@link #inspect} and {@link #create} share one config parse.
 */
public final class NdJsonFormatReaderFactory implements FormatReaderFactory {

    static final String CONFIG_SCHEMA_SAMPLE_SIZE = "schema_sample_size";
    static final String CONFIG_SEGMENT_SIZE = "segment_size";
    static final String CONFIG_DATETIME_FORMAT = "datetime_format";
    public static final Set<String> RECOGNIZED_KEYS = Set.of(CONFIG_SCHEMA_SAMPLE_SIZE, CONFIG_SEGMENT_SIZE, CONFIG_DATETIME_FORMAT);

    private final Settings nodeSettings;

    public NdJsonFormatReaderFactory(Settings nodeSettings) {
        this.nodeSettings = nodeSettings == null ? Settings.EMPTY : nodeSettings;
    }

    @Override
    public FormatReader create(Settings settings, BlockFactory blockFactory) {
        return new NdJsonFormatReader(settings == null ? nodeSettings : settings, blockFactory);
    }

    @Override
    public FormatReader create(
        Settings settings,
        BlockFactory blockFactory,
        @Nullable Map<String, Object> config,
        @Nullable FormatReadContext.Binding binding
    ) {
        Settings resolved = settings == null ? nodeSettings : settings;
        Parsed parsed = parse(config, resolved);
        FormatReadContext.Binding bound = binding == null ? FormatReadContext.Binding.empty() : binding;
        return new NdJsonFormatReader(
            blockFactory,
            bound.boundSchema(),
            parsed.schemaSampleSize,
            parsed.segmentSizeBytes,
            parsed.datetimeFormatter,
            parsed.canonicalConfig,
            bound.declaredDateFormats(),
            bound.readConfig(),
            new NdJsonReaderCounters()
        );
    }

    @Override
    public Configured<Void> inspect(Map<String, Object> config) {
        if (config == null || config.isEmpty()) {
            return Configured.empty(null);
        }
        parse(config, nodeSettings);
        return Configured.fromKnownSubset(null, config, RECOGNIZED_KEYS);
    }

    @Override
    public String formatName() {
        return "ndjson";
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
    public RecordSplitter recordSplitter(@Nullable Map<String, Object> config, int maxRecordBytes) {
        return new NdJsonRecordSplitter(maxRecordBytes);
    }

    @Override
    public long minimumSegmentSize(@Nullable Map<String, Object> config) {
        return parse(config, nodeSettings).segmentSizeBytes;
    }

    private static Parsed parse(@Nullable Map<String, Object> config, Settings settings) {
        int sampleSize = schemaSampleSize(settings);
        long segmentSize = segmentSize(settings);
        DateFormatter datetimeFormatter = null;
        String canonical = "";
        if (config != null && config.isEmpty() == false) {
            sampleSize = parseInt(config.get(CONFIG_SCHEMA_SAMPLE_SIZE), sampleSize);
            Check.clientError(sampleSize > 0, CONFIG_SCHEMA_SAMPLE_SIZE + " must be positive, got: {}", sampleSize);
            segmentSize = parseSegmentSize(config.get(CONFIG_SEGMENT_SIZE), segmentSize);
            datetimeFormatter = parseDatetimeFormat(config.get(CONFIG_DATETIME_FORMAT), datetimeFormatter);
            canonical = SchemaCacheKey.buildFormatConfig(config);
        }
        return new Parsed(sampleSize, segmentSize, datetimeFormatter, canonical);
    }

    static int schemaSampleSize(Settings settings) {
        Settings resolved = settings == null ? Settings.EMPTY : settings;
        return resolved.getAsInt(NdJsonFormatReader.SCHEMA_SAMPLE_SIZE_SETTING, NdJsonFormatReader.DEFAULT_SCHEMA_SAMPLE_SIZE);
    }

    static long segmentSize(Settings settings) {
        Settings resolved = settings == null ? Settings.EMPTY : settings;
        ByteSizeValue value = resolved.getAsBytesSize(NdJsonFormatReader.SEGMENT_SIZE_SETTING, NdJsonFormatReader.DEFAULT_SEGMENT_SIZE);
        long bytes = value.getBytes();
        Check.clientError(
            bytes >= NdJsonFormatReader.MIN_SEGMENT_SIZE.getBytes(),
            "{} must be >= {}, got: {}",
            NdJsonFormatReader.SEGMENT_SIZE_SETTING,
            NdJsonFormatReader.MIN_SEGMENT_SIZE,
            value
        );
        return bytes;
    }

    static int parseInt(Object value, int defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value [" + value + "]", e);
        }
    }

    static long parseSegmentSize(Object value, long defaultValueBytes) {
        if (value == null) {
            return defaultValueBytes;
        }
        ByteSizeValue parsed = ByteSizeValue.parseBytesSizeValue(value.toString(), CONFIG_SEGMENT_SIZE);
        long bytes = parsed.getBytes();
        Check.clientError(
            bytes >= NdJsonFormatReader.MIN_SEGMENT_SIZE.getBytes(),
            CONFIG_SEGMENT_SIZE + " must be >= {}, got: {}",
            NdJsonFormatReader.MIN_SEGMENT_SIZE,
            parsed
        );
        return bytes;
    }

    static DateFormatter parseDatetimeFormat(Object value, DateFormatter baseline) {
        if (value == null || value.toString().isEmpty()) {
            return baseline;
        }
        try {
            return DateFormatter.forPattern(value.toString());
        } catch (Exception e) {
            throw new IllegalArgumentException("Invalid datetime_format [" + value + "]", e);
        }
    }

    private record Parsed(int schemaSampleSize, long segmentSizeBytes, DateFormatter datetimeFormatter, String canonicalConfig) {}
}
