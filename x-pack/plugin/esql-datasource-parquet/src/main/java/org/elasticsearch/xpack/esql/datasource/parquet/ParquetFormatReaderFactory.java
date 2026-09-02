/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.parquet;

import org.apache.parquet.filter2.compat.FilterCompat;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.datasources.FormatNameResolver;
import org.elasticsearch.xpack.esql.datasources.spi.AggregatePushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FilterPushdownSupport;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReadContext;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReader;
import org.elasticsearch.xpack.esql.datasources.spi.FormatReaderFactory;

import java.util.Map;

/**
 * Resource-free Parquet factory. Retains a node-scoped codec factory; each {@link #create}
 * returns a distinct reader.
 */
public final class ParquetFormatReaderFactory implements FormatReaderFactory {

    private final PlainCompressionCodecFactory codecFactory = new PlainCompressionCodecFactory();

    @Override
    public FormatReader create(Settings settings, BlockFactory blockFactory) {
        return new ParquetFormatReader(blockFactory);
    }

    @Override
    public FormatReader create(
        Settings settings,
        BlockFactory blockFactory,
        @Nullable Map<String, Object> config,
        @Nullable FormatReadContext.Binding binding
    ) {
        FormatReadContext.Binding bound = binding == null ? FormatReadContext.Binding.empty() : binding;
        FilterCompat.Filter filter = FilterCompat.NOOP;
        ParquetPushedExpressions expressions = null;
        if (bound.pushedFilter() instanceof FilterCompat.Filter parquetFilter) {
            filter = parquetFilter;
        } else if (bound.pushedFilter() instanceof ParquetPushedExpressions pushed) {
            expressions = pushed;
        }
        return new ParquetFormatReader(
            blockFactory,
            filter,
            expressions,
            false,
            true,
            bound.dynamicThreshold(),
            bound.declaredDateFormats(),
            bound.declaredTypeColumns(),
            codecFactory,
            new ParquetReaderCounters()
        );
    }

    @Override
    public String formatName() {
        return FormatNameResolver.FORMAT_PARQUET;
    }

    @Override
    public AggregatePushdownSupport aggregatePushdownSupport() {
        return new ParquetAggregatePushdownSupport();
    }

    @Override
    public FilterPushdownSupport filterPushdownSupport() {
        return new ParquetFilterPushdownSupport();
    }

    @Override
    public boolean rangeAware() {
        return true;
    }

    @Override
    public boolean columnExtractor() {
        return true;
    }

    @Override
    public boolean acceptsDynamicThreshold() {
        return true;
    }

    @Override
    public boolean supportsWholeFileCompression() {
        return false;
    }
}
